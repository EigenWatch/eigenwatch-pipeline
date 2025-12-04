# services/reconstructors/avs_allocation_summary.py

from typing import Dict, List, Optional
from .base import BaseReconstructor, FieldValidator
from ..query_builders.avs_allocation_summary_builder import (
    AVSAllocationSummaryQueryBuilder,
)


class AVSAllocationSummaryReconstructor(BaseReconstructor):
    """
    Reconstructs AVS allocation summaries.
    Aggregates allocations across all operator sets for each operator-AVS-strategy combo.
    """

    def __init__(self, db, logger):
        query_builder = AVSAllocationSummaryQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field("avs_id", "avs", nullable=False)
        field_validator.add_foreign_key_field(
            "strategy_id", "strategies", nullable=False
        )

        # Timestamp fields
        field_validator.add_timestamp_field("last_allocated_at", nullable=True)

        # Decimal fields
        field_validator.add_decimal_field("total_allocated_magnitude", nullable=False)

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )

    def fetch_state_for_operator(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> List[Dict]:
        fetch_query, params = self.query_builder.build_fetch_query(
            operator_id, up_to_block
        )
        rows = self.db.execute_query(fetch_query, params, db="analytics")
        return self.tuple_to_dict_transformer(self.column_names)(rows)
