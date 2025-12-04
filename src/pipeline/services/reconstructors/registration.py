# services/reconstructors/registration.py

from .base import BaseReconstructor, FieldValidator
from ..query_builders.registration_builder import (
    OperatorRegistrationQueryBuilder,
)


class OperatorRegistrationReconstructor(BaseReconstructor):
    """Reconstructs operator registration information"""

    def __init__(self, db, logger):
        query_builder = OperatorRegistrationQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )

        # Timestamp fields
        field_validator.add_timestamp_field("registered_at", nullable=False)

        # String fields
        field_validator.add_string_field("delegation_approver", nullable=False)
        field_validator.add_string_field("transaction_hash", nullable=False)

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
