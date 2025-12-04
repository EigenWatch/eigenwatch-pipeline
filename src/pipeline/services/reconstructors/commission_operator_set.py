# services/reconstructors/commission_operator_set.py
from .base import BaseReconstructor, FieldValidator
from ..query_builders.commission_operator_set_builder import (
    CommissionOperatorSetQueryBuilder,
)


class CommissionOperatorSetReconstructor(BaseReconstructor):
    def __init__(self, db, logger):
        query_builder = CommissionOperatorSetQueryBuilder()
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
        field_validator.add_timestamp_field("current_activated_at", nullable=True)
        field_validator.add_timestamp_field("first_set_at", nullable=True)
        field_validator.add_timestamp_field("updated_at", nullable=False)
        field_validator.add_string_field("commission_type", nullable=False)

        super().__init__(db, logger, query_builder, column_names, field_validator)
