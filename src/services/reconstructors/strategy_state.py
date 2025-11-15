from .base import BaseReconstructor, FieldValidator
from ..query_builders.strategy_state_builder import StrategyStateQueryBuilder


class StrategyStateReconstructor(BaseReconstructor):
    def __init__(self, db, logger):
        query_builder = StrategyStateQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields (will auto-create entities if they don't exist)
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field(
            "strategy_id", "strategies", nullable=False
        )

        # Timestamp fields (Unix timestamps from events DB need conversion)
        field_validator.add_timestamp_field("max_magnitude_updated_at", nullable=True)
        field_validator.add_timestamp_field(
            "encumbered_magnitude_updated_at", nullable=True
        )
        field_validator.add_timestamp_field("updated_at", nullable=False)

        # Decimal/numeric fields
        field_validator.add_decimal_field("max_magnitude", nullable=False)
        field_validator.add_decimal_field("encumbered_magnitude", nullable=False)
        field_validator.add_decimal_field("utilization_rate", nullable=False)

        # Integer fields (block numbers) don't need special handling
        # They're already correct from the DB

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
