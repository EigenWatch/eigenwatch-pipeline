from pipeline.services.query_builders.metadata_history_builder import (
    OperatorMetadataHistoryQueryBuilder,
)
from .base import BaseReconstructor, FieldValidator


class OperatorMetadataHistoryReconstructor(BaseReconstructor):
    """Reconstructs operator metadata history"""

    def __init__(self, db, logger):
        query_builder = OperatorMetadataHistoryQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )

        # Timestamp fields
        field_validator.add_timestamp_field("updated_at", nullable=False)
        field_validator.add_timestamp_field("metadata_fetched_at", nullable=True)

        # String fields
        field_validator.add_string_field("metadata_uri", nullable=False)
        field_validator.add_string_field("transaction_hash", nullable=False)

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
