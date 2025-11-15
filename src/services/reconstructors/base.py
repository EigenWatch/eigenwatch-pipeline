from typing import Callable, List, Dict, Optional
import logging

from services.validators.fieldValidator import FieldValidator, ForeignKeyHandler


class BaseReconstructor:
    """
    Generic reconstructor for fetching from events DB and inserting/updating
    in analytics DB. Works with a query_builder providing fetch/insert queries.
    """

    def __init__(
        self,
        db,
        logger: logging.Logger,
        query_builder,
        column_names: Optional[List[str]] = None,
        field_validator: Optional[FieldValidator] = None,
    ):
        self.db = db
        self.logger = logger
        self.query_builder = query_builder
        self.column_names = column_names

        # Setup foreign key handler if validator has foreign key fields
        if field_validator and field_validator.foreign_key_fields:
            if not field_validator.foreign_key_handler:
                field_validator.foreign_key_handler = ForeignKeyHandler(db, logger)

        self.field_validator = field_validator or FieldValidator()

    def rebuild_for_operator(self, operator_id: str) -> int:
        """
        Full rebuild for a single operator: fetch rows from events, insert/update analytics.
        Returns total inserted/updated rows.
        """
        rows = self.fetch_state_for_operator(operator_id)
        return self.insert_state_rows(operator_id, rows)

    def fetch_state_for_operator(self, operator_id: str) -> List[Dict]:
        """
        Fetch raw rows from the events DB and transform to dictionaries.
        """
        fetch_query, params = self.query_builder.build_fetch_query(operator_id)
        rows = self.db.execute_query(fetch_query, params, db="events")
        return self.tuple_to_dict_transformer(self.column_names)(rows)

    def insert_state_rows(self, operator_id: str, rows: List[Dict]) -> int:
        """
        Validate, transform, and insert/update rows into the analytics DB.
        """
        if not rows:
            return 0

        insert_query = self.query_builder.build_insert_query()
        total = 0
        skipped = 0

        for idx, row in enumerate(rows):
            try:
                # Validate and transform fields (includes foreign key handling)
                validated_row = self.field_validator.validate_and_transform(row)

                # Generate composite ID
                validated_row["id"] = self.query_builder.generate_id(validated_row)

                # Execute insert/update
                self.db.execute_update(insert_query, validated_row, db="analytics")
                total += 1

            except Exception as exc:
                error_msg = str(exc)

                # Check if it's a foreign key violation (shouldn't happen with auto-creation)
                if (
                    "ForeignKeyViolation" in error_msg
                    or "foreign key constraint" in error_msg
                ):
                    self.logger.warning(
                        f"Skipping row {idx} for operator {operator_id}: "
                        f"foreign key violation (auto-creation may have failed)"
                    )
                    self.logger.debug(f"Error: {error_msg}")
                    skipped += 1
                else:
                    # Other errors - log as error
                    self.logger.error(
                        f"Failed to insert row {idx} for operator {operator_id}: {exc}"
                    )
                    self.logger.debug(f"Problematic row: {row}")

                # Continue processing other rows
                continue

        if skipped > 0:
            self.logger.info(
                f"Skipped {skipped} rows for operator {operator_id} due to constraint violations"
            )

        # Clear foreign key cache for next operator
        if self.field_validator.foreign_key_handler and hasattr(
            self.field_validator.foreign_key_handler, "clear_cache"
        ):
            self.field_validator.foreign_key_handler.clear_cache()

        return total

    def tuple_to_dict_transformer(
        self,
        column_names: List[str],
    ) -> Callable[[List[tuple]], List[dict]]:
        """Create a transformer that converts tuples to dicts with length check."""

        def transform(rows: List[tuple]) -> List[dict]:
            transformed = []
            for idx, row in enumerate(rows):
                if len(row) != len(column_names):
                    raise ValueError(
                        f"Row {idx} length ({len(row)}) != column_names length ({len(column_names)}): {row}"
                    )
                transformed.append(dict(zip(column_names, row)))
            return transformed

        return transform
