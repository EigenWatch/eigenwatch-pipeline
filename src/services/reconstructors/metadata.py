from services.query_builders.metadata_builder import OperatorMetadataQueryBuilder
from .base import BaseReconstructor, FieldValidator


class OperatorMetadataReconstructor(BaseReconstructor):
    """Reconstructs current operator metadata state"""

    def __init__(self, db, logger):
        query_builder = OperatorMetadataQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )

        # Timestamp fields
        field_validator.add_timestamp_field("last_updated_at", nullable=False)
        field_validator.add_timestamp_field("metadata_fetched_at", nullable=True)

        # String fields
        field_validator.add_string_field("metadata_uri", nullable=False)

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
