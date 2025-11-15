from .base import BaseReconstructor, FieldValidator
from ..query_builders.allocation_state_builder import AllocationStateQueryBuilder


class AllocationReconstructor(BaseReconstructor):
    def __init__(self, db, logger):
        query_builder = AllocationStateQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field(
            "strategy_id", "strategies", nullable=False
        )
        field_validator.add_foreign_key_field(
            field_name="operator_set_id",
            table_name="operator_sets",
            nullable=False,
            context_fields=[
                "operator_set_id"
            ],  # Only needs itself to extract avs_id from composite key
        )

        # Timestamp fields (Unix timestamps from events DB need conversion)
        field_validator.add_timestamp_field("allocated_at", nullable=False)
        field_validator.add_timestamp_field("activated_at", nullable=True)
        field_validator.add_timestamp_field("updated_at", nullable=False)

        # Decimal/numeric fields
        field_validator.add_decimal_field("magnitude", nullable=False)

        # Integer fields (block numbers and effect_block)
        # No special handling needed - they come through correctly

        # Boolean field (is_active)
        # No special handling needed

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
