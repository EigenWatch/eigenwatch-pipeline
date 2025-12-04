# services/reconstructors/avs_relationship_history.py
from .base import BaseReconstructor, FieldValidator
from ..query_builders.avs_relationship_history_builder import (
    AVSRelationshipHistoryQueryBuilder,
)


class AVSRelationshipHistoryReconstructor(BaseReconstructor):
    def __init__(self, db, logger):
        query_builder = AVSRelationshipHistoryQueryBuilder()
        column_names = query_builder.get_column_names()

        field_validator = FieldValidator()
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field("avs_id", "avs", nullable=False)
        field_validator.add_timestamp_field("status_changed_at", nullable=False)
        field_validator.add_timestamp_field("period_start", nullable=False)
        field_validator.add_timestamp_field("created_at", nullable=False)
        field_validator.add_timestamp_field("updated_at", nullable=False)
        field_validator.add_string_field("status", nullable=False)
        field_validator.add_string_field("transaction_hash", nullable=False)

        super().__init__(db, logger, query_builder, column_names, field_validator)
