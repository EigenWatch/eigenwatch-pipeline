# services/reconstructors/avs_relationship_current.py
from .base import BaseReconstructor, FieldValidator
from ..query_builders.avs_relationship_current_builder import (
    AVSRelationshipCurrentQueryBuilder,
)


class AVSRelationshipCurrentReconstructor(BaseReconstructor):
    def __init__(self, db, logger):
        query_builder = AVSRelationshipCurrentQueryBuilder()
        column_names = query_builder.get_column_names()

        field_validator = FieldValidator()
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field("avs_id", "avs", nullable=False)
        field_validator.add_timestamp_field("current_status_since", nullable=False)
        field_validator.add_timestamp_field("first_registered_at", nullable=True)
        field_validator.add_timestamp_field("last_registered_at", nullable=True)
        field_validator.add_timestamp_field("last_unregistered_at", nullable=True)
        field_validator.add_timestamp_field("last_activity_at", nullable=True)
        field_validator.add_timestamp_field("updated_at", nullable=False)
        field_validator.add_string_field("current_status", nullable=False)

        super().__init__(db, logger, query_builder, column_names, field_validator)

    def fetch_state_for_operator(self, operator_id: str):
        """Override to query analytics DB instead of events DB"""
        fetch_query, params = self.query_builder.build_fetch_query(operator_id)
        rows = self.db.execute_query(fetch_query, params, db="analytics")
        return self.tuple_to_dict_transformer(self.column_names)(rows)
