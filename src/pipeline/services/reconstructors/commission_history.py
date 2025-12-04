# services/reconstructors/commission_history.py
"""
Commission History Reconstructor - tracks all commission changes
"""

from .base import BaseReconstructor, FieldValidator
from ..query_builders.commission_history_builder import CommissionHistoryQueryBuilder


class CommissionHistoryReconstructor(BaseReconstructor):
    """
    Reconstructs commission change history for all commission types.
    This is needed for the last_commission_change_at field in operator_state.
    """

    def __init__(self, db, logger):
        query_builder = CommissionHistoryQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field("avs_id", "avs", nullable=True)
        field_validator.add_foreign_key_field(
            "operator_set_id",
            "operator_sets",
            nullable=True,
            context_fields=["operator_set_id"],
        )

        # Timestamp fields
        field_validator.add_timestamp_field("changed_at", nullable=False)
        field_validator.add_timestamp_field("activated_at", nullable=False)

        # String fields
        field_validator.add_string_field("commission_type", nullable=False)
        field_validator.add_string_field("caller", nullable=True)

        # Decimal/Integer fields for bips
        # Note: These come as Numeric from events DB

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
