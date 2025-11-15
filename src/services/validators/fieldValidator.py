from typing import Dict, Optional, Any, Set
from datetime import datetime, timezone
from decimal import Decimal
import logging


class ForeignKeyHandler:
    """Handles foreign key validation and entity creation."""

    def __init__(self, db, logger: logging.Logger):
        self.db = db
        self.logger = logger
        # Cache to avoid repeated lookups in the same batch
        self._existence_cache: Dict[str, Set[str]] = {}

    def ensure_entity_exists(self, table_name: str, entity_id: str) -> bool:
        """
        Ensure an entity exists in the specified table, creating it if necessary.

        Args:
            table_name: Name of the table (e.g., 'operators', 'strategies')
            entity_id: ID/address of the entity

        Returns:
            True if entity exists or was created successfully
        """
        # Check cache first
        if table_name not in self._existence_cache:
            self._existence_cache[table_name] = set()

        if entity_id in self._existence_cache[table_name]:
            return True

        # Check if entity exists in DB
        check_query = f"SELECT id FROM {table_name} WHERE id = :id LIMIT 1"
        result = self.db.execute_query(check_query, {"id": entity_id}, db="analytics")

        if result:
            self._existence_cache[table_name].add(entity_id)
            return True

        # Entity doesn't exist, create it
        try:
            insert_query = f"""
                INSERT INTO {table_name} (id, address, created_at, updated_at)
                VALUES (:id, :address, NOW(), NOW())
                ON CONFLICT (id) DO NOTHING
            """
            self.db.execute_update(
                insert_query, {"id": entity_id, "address": entity_id}, db="analytics"
            )
            self._existence_cache[table_name].add(entity_id)
            self.logger.debug(f"Created {table_name[:-1]} {entity_id}")
            return True

        except Exception as exc:
            self.logger.error(f"Failed to create {table_name[:-1]} {entity_id}: {exc}")
            return False

    def clear_cache(self):
        """Clear the existence cache. Call between different operators."""
        self._existence_cache.clear()


class FieldValidator:
    """Handles field validation and transformation rules."""

    def __init__(self, foreign_key_handler: Optional[ForeignKeyHandler] = None):
        self.timestamp_fields = set()
        self.decimal_fields = set()
        self.string_fields = set()
        self.nullable_fields = set()
        self.foreign_key_fields: Dict[str, str] = {}  # field_name -> table_name
        self.foreign_key_handler = foreign_key_handler

    def add_timestamp_field(self, field_name: str, nullable: bool = False):
        """Register a field that should be converted from Unix timestamp to datetime."""
        self.timestamp_fields.add(field_name)
        if nullable:
            self.nullable_fields.add(field_name)
        return self

    def add_decimal_field(self, field_name: str, nullable: bool = False):
        """Register a field that should be ensured as Decimal."""
        self.decimal_fields.add(field_name)
        if nullable:
            self.nullable_fields.add(field_name)
        return self

    def add_string_field(self, field_name: str, nullable: bool = False):
        """Register a field that should be ensured as string."""
        self.string_fields.add(field_name)
        if nullable:
            self.nullable_fields.add(field_name)
        return self

    def add_foreign_key_field(
        self, field_name: str, table_name: str, nullable: bool = False
    ):
        """
        Register a field that references another table.
        The entity will be auto-created if it doesn't exist.

        Args:
            field_name: Name of the foreign key field (e.g., 'operator_id')
            table_name: Name of the referenced table (e.g., 'operators')
            nullable: Whether the field can be None
        """
        self.foreign_key_fields[field_name] = table_name
        if nullable:
            self.nullable_fields.add(field_name)
        return self

    def validate_and_transform(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and transform a row according to registered rules.

        Raises:
            ValueError: If validation fails
        """
        transformed = row.copy()

        # Validate and ensure foreign key entities exist
        if self.foreign_key_handler:
            for field_name, table_name in self.foreign_key_fields.items():
                if field_name not in transformed:
                    continue

                value = transformed[field_name]

                if value is None:
                    if field_name not in self.nullable_fields:
                        raise ValueError(
                            f"Foreign key field '{field_name}' cannot be None"
                        )
                    continue

                # Ensure the referenced entity exists
                if not self.foreign_key_handler.ensure_entity_exists(table_name, value):
                    raise ValueError(
                        f"Failed to ensure {table_name[:-1]} '{value}' exists for field '{field_name}'"
                    )

        # Transform timestamp fields
        for field in self.timestamp_fields:
            if field not in transformed:
                continue

            value = transformed[field]

            if value is None:
                if field not in self.nullable_fields:
                    raise ValueError(f"Field '{field}' cannot be None")
                continue

            # Convert Unix timestamp (int) to datetime
            if isinstance(value, int):
                transformed[field] = datetime.fromtimestamp(value, tz=timezone.utc)
            elif isinstance(value, datetime):
                # Already a datetime, ensure it has timezone
                if value.tzinfo is None:
                    transformed[field] = value.replace(tzinfo=timezone.utc)
            else:
                raise ValueError(
                    f"Field '{field}' must be int (Unix timestamp) or datetime, got {type(value)}"
                )

        # Validate decimal fields
        for field in self.decimal_fields:
            if field not in transformed:
                continue

            value = transformed[field]

            if value is None:
                if field not in self.nullable_fields:
                    raise ValueError(f"Field '{field}' cannot be None")
                continue

            if not isinstance(value, (Decimal, int, float)):
                raise ValueError(f"Field '{field}' must be numeric, got {type(value)}")

            # Convert to Decimal if not already
            if not isinstance(value, Decimal):
                transformed[field] = Decimal(str(value))

        # Validate string fields
        for field in self.string_fields:
            if field not in transformed:
                continue

            value = transformed[field]

            if value is None:
                if field not in self.nullable_fields:
                    raise ValueError(f"Field '{field}' cannot be None")
                continue

            if not isinstance(value, str):
                transformed[field] = str(value)

        return transformed
