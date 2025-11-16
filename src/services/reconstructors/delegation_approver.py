from services.query_builders.delegation_approver_history_builder import (
    DelegationApproverHistoryQueryBuilder,
)
from .base import BaseReconstructor, FieldValidator


class DelegationApproverHistoryReconstructor(BaseReconstructor):
    """Reconstructs delegation approver change history"""

    def __init__(self, db, logger):
        query_builder = DelegationApproverHistoryQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )

        # Timestamp fields
        field_validator.add_timestamp_field("changed_at", nullable=False)

        # String fields
        field_validator.add_string_field("old_delegation_approver", nullable=True)
        field_validator.add_string_field("new_delegation_approver", nullable=False)
        field_validator.add_string_field("transaction_hash", nullable=False)

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
