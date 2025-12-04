# services/reconstructors/slashing_events_cache.py
from .base import BaseReconstructor, FieldValidator
from ..query_builders.slashing_events_cache_builder import (
    SlashingEventsCacheQueryBuilder,
)


class SlashingEventsCacheReconstructor(BaseReconstructor):
    def __init__(self, db, logger):
        query_builder = SlashingEventsCacheQueryBuilder()
        column_names = query_builder.get_column_names()

        field_validator = FieldValidator()
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field(
            field_name="operator_set_id",
            table_name="operator_sets",
            nullable=False,
            context_fields=["operator_set_id"],
        )
        field_validator.add_timestamp_field("slashed_at", nullable=False)
        field_validator.add_timestamp_field("created_at", nullable=False)
        field_validator.add_timestamp_field("updated_at", nullable=False)
        field_validator.add_string_field("description", nullable=True)
        field_validator.add_string_field("transaction_hash", nullable=False)

        super().__init__(db, logger, query_builder, column_names, field_validator)
