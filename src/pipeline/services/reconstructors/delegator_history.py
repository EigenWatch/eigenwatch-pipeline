# services/reconstructors/delegator_history.py
from .base import BaseReconstructor, FieldValidator
from ..query_builders.delegator_history_builder import DelegatorHistoryQueryBuilder


class DelegatorHistoryReconstructor(BaseReconstructor):
    def __init__(self, db, logger):
        query_builder = DelegatorHistoryQueryBuilder()
        column_names = query_builder.get_column_names()

        field_validator = FieldValidator()
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field("staker_id", "stakers", nullable=False)
        field_validator.add_timestamp_field("event_timestamp", nullable=False)
        field_validator.add_timestamp_field("created_at", nullable=False)
        field_validator.add_timestamp_field("updated_at", nullable=False)
        field_validator.add_string_field("delegation_type", nullable=False)
        field_validator.add_string_field("transaction_hash", nullable=False)

        super().__init__(db, logger, query_builder, column_names, field_validator)
