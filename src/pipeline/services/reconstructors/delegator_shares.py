# services/reconstructors/delegator_shares.py
from .base import BaseReconstructor, FieldValidator
from ..query_builders.delegator_shares_builder import DelegatorSharesQueryBuilder


class DelegatorSharesReconstructor(BaseReconstructor):
    def __init__(self, db, logger):
        query_builder = DelegatorSharesQueryBuilder()
        column_names = query_builder.get_column_names()

        field_validator = FieldValidator()
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field("staker_id", "stakers", nullable=False)
        field_validator.add_foreign_key_field(
            "strategy_id", "strategies", nullable=False
        )
        field_validator.add_timestamp_field("shares_updated_at", nullable=False)
        field_validator.add_timestamp_field("updated_at", nullable=False)
        field_validator.add_decimal_field("shares", nullable=False)

        super().__init__(db, logger, query_builder, column_names, field_validator)
