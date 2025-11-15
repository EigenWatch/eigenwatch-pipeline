from typing import Dict, Optional, Any, Set, Callable
from datetime import datetime, timezone
from decimal import Decimal
import logging


class ForeignKeyHandler:
    """Handles foreign key validation and entity creation."""

    # Simple entities: just id and address
    SIMPLE_ENTITY_TABLES = {"operators", "avs", "stakers", "strategies"}

    def __init__(self, db, logger: logging.Logger):
        self.db = db
        self.logger = logger
        # Cache to avoid repeated lookups in the same batch
        self._existence_cache: Dict[str, Set[str]] = {}
        # Register handlers for complex entities
        self._complex_entity_handlers: Dict[str, Callable] = {
            "operator_sets": self._create_operator_set,
        }

    def ensure_entity_exists(
        self, table_name: str, entity_id: str, context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Ensure an entity exists in the specified table, creating it if necessary.

        Args:
            table_name: Name of the table (e.g., 'operators', 'strategies')
            entity_id: ID/address of the entity
            context: Additional context data needed for complex entities

        Returns:
            True if entity exists or was created successfully
        """
        # Check cache first
        if table_name not in self._existence_cache:
            self._existence_cache[table_name] = set()

        if entity_id in self._existence_cache[table_name]:
            return True

        # Check if entity exists in DB
        if self._check_entity_exists(table_name, entity_id):
            self._existence_cache[table_name].add(entity_id)
            return True

        # Entity doesn't exist, create it
        if table_name in self.SIMPLE_ENTITY_TABLES:
            return self._create_simple_entity(table_name, entity_id)
        elif table_name in self._complex_entity_handlers:
            handler = self._complex_entity_handlers[table_name]
            return handler(entity_id, context or {})
        else:
            self.logger.warning(
                f"Entity {entity_id} not found in {table_name}. "
                f"No auto-creation handler registered for this table."
            )
            return False

    def _check_entity_exists(self, table_name: str, entity_id: str) -> bool:
        """Check if an entity exists in the database."""
        try:
            check_query = f"SELECT id FROM {table_name} WHERE id = :id LIMIT 1"
            result = self.db.execute_query(
                check_query, {"id": entity_id}, db="analytics"
            )
            return bool(result)
        except Exception as exc:
            self.logger.error(
                f"Failed to check existence of {table_name} {entity_id}: {exc}"
            )
            return False

    def _create_simple_entity(self, table_name: str, entity_id: str) -> bool:
        """Create a simple entity with just id and address."""
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

    def _create_operator_set(
        self, operator_set_id: str, context: Dict[str, Any]
    ) -> bool:
        """
        Create an operator_set entity.
        Requires avs_id and operator_set_id in context.
        """
        operator_set_number = context.get("operator_set_number")
        avs_id = operator_set_id.split("-")[0]  # Extract from "0xaddress-0"

        # Validate required fields
        if not avs_id:
            self.logger.warning(
                f"Cannot create operator_set {operator_set_id}: missing avs_id in context"
            )
            return False

        if operator_set_number is None:
            self.logger.warning(
                f"Cannot create operator_set {operator_set_id}: missing operator_set_number in context"
            )
            return False

        # Extract integer from operator_set_id (format: "0xaddress-0")
        try:
            set_number = int(operator_set_id.split("-")[-1])
        except (ValueError, IndexError):
            self.logger.error(
                f"Cannot parse operator_set_number from {operator_set_id}"
            )
            return False

        # Ensure AVS exists first
        if not self.ensure_entity_exists("avs", avs_id):
            self.logger.warning(
                f"Cannot create operator_set {operator_set_id}: "
                f"failed to ensure AVS {avs_id} exists"
            )
            return False

        # Create the operator_set
        try:
            insert_query = """
                INSERT INTO operator_sets (id, avs_id, operator_set_id, created_at, updated_at)
                VALUES (:id, :avs_id, :operator_set_id, NOW(), NOW())
                ON CONFLICT (id) DO NOTHING
            """
            self.db.execute_update(
                insert_query,
                {
                    "id": operator_set_id,
                    "avs_id": avs_id,
                    "operator_set_id": set_number,  # Use extracted integer
                },
                db="analytics",
            )
            self._existence_cache["operator_sets"].add(operator_set_id)
            self.logger.debug(
                f"Created operator_set {operator_set_id} (avs: {avs_id}, set_id: {set_number})"
            )
            return True

        except Exception as exc:
            self.logger.error(f"Failed to create operator_set {operator_set_id}: {exc}")
            return False

        except Exception as exc:
            self.logger.error(f"Failed to create operator_set {operator_set_id}: {exc}")
            return False

    def register_complex_entity_handler(
        self, table_name: str, handler: Callable[[str, Dict[str, Any]], bool]
    ):
        """
        Register a custom handler for creating complex entities.

        Args:
            table_name: Name of the table
            handler: Function that takes (entity_id, context) and returns success bool
        """
        self._complex_entity_handlers[table_name] = handler

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
        # field_name -> (table_name, context_fields)
        self.foreign_key_fields: Dict[str, tuple[str, Optional[list]]] = {}
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
        self,
        field_name: str,
        table_name: str,
        nullable: bool = False,
        context_fields: Optional[list] = None,
    ):
        """
        Register a field that references another table.
        The entity will be auto-created if it doesn't exist.

        Args:
            field_name: Name of the foreign key field (e.g., 'operator_id')
            table_name: Name of the referenced table (e.g., 'operators')
            nullable: Whether the field can be None
            context_fields: Additional fields needed to create the entity (for complex entities)
                          e.g., ['avs_id', 'operator_set_number'] for operator_sets
        """
        self.foreign_key_fields[field_name] = (table_name, context_fields)
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

        # Step 1: Validate and ensure foreign key entities exist
        self._validate_foreign_keys(transformed)

        # Step 2: Transform timestamp fields
        self._transform_timestamps(transformed)

        # Step 3: Validate decimal fields
        self._validate_decimals(transformed)

        # Step 4: Validate string fields
        self._validate_strings(transformed)

        return transformed

    def _validate_foreign_keys(self, row: Dict[str, Any]) -> None:
        """Validate foreign key fields and ensure referenced entities exist."""
        if not self.foreign_key_handler:
            return

        for field_name, (table_name, context_fields) in self.foreign_key_fields.items():
            if field_name not in row:
                continue

            value = row[field_name]

            # Handle nullable fields
            if value is None:
                if field_name not in self.nullable_fields:
                    raise ValueError(f"Foreign key field '{field_name}' cannot be None")
                continue

            # Build context for complex entities
            context = {}
            if context_fields:
                for ctx_field in context_fields:
                    if ctx_field in row:
                        # Handle special naming (e.g., operator_set_id field -> operator_set_number context)
                        if (
                            ctx_field == "operator_set_id"
                            and table_name == "operator_sets"
                        ):
                            context["operator_set_number"] = row[ctx_field]
                        else:
                            context[ctx_field] = row[ctx_field]
                    else:
                        self.logger.warning(
                            f"Missing context field '{ctx_field}' for creating {table_name}"
                        )

            # Ensure the referenced entity exists
            if not self.foreign_key_handler.ensure_entity_exists(
                table_name, value, context
            ):
                raise ValueError(
                    f"Failed to ensure {table_name[:-1]} '{value}' exists for field '{field_name}'"
                )

    def _transform_timestamps(self, row: Dict[str, Any]) -> None:
        """Transform timestamp fields from Unix timestamps to datetime objects."""
        for field in self.timestamp_fields:
            if field not in row:
                continue

            value = row[field]

            if value is None:
                if field not in self.nullable_fields:
                    raise ValueError(f"Field '{field}' cannot be None")
                continue

            # Convert Unix timestamp (int) to datetime
            if isinstance(value, int):
                row[field] = datetime.fromtimestamp(value, tz=timezone.utc)
            elif isinstance(value, datetime):
                # Already a datetime, ensure it has timezone
                if value.tzinfo is None:
                    row[field] = value.replace(tzinfo=timezone.utc)
            else:
                raise ValueError(
                    f"Field '{field}' must be int (Unix timestamp) or datetime, got {type(value)}"
                )

    def _validate_decimals(self, row: Dict[str, Any]) -> None:
        """Validate and convert decimal fields."""
        for field in self.decimal_fields:
            if field not in row:
                continue

            value = row[field]

            if value is None:
                if field not in self.nullable_fields:
                    raise ValueError(f"Field '{field}' cannot be None")
                continue

            if not isinstance(value, (Decimal, int, float)):
                raise ValueError(f"Field '{field}' must be numeric, got {type(value)}")

            # Convert to Decimal if not already
            if not isinstance(value, Decimal):
                row[field] = Decimal(str(value))

    def _validate_strings(self, row: Dict[str, Any]) -> None:
        """Validate and convert string fields."""
        for field in self.string_fields:
            if field not in row:
                continue

            value = row[field]

            if value is None:
                if field not in self.nullable_fields:
                    raise ValueError(f"Field '{field}' cannot be None")
                continue

            if not isinstance(value, str):
                row[field] = str(value)

    @property
    def logger(self):
        """Get logger from foreign_key_handler if available."""
        if self.foreign_key_handler:
            return self.foreign_key_handler.logger
        return logging.getLogger(__name__)
