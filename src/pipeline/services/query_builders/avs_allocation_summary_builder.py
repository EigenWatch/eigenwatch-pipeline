# query_builders/avs_allocation_summary_builder.py

from typing import Optional
from .base_builder import BaseQueryBuilder


# Fetch query - aggregates allocations by AVS
avs_allocation_summary_fetch_query = """
WITH allocations_with_avs AS (
    SELECT 
        oa.operator_id,
        -- Extract AVS ID from operator_set_id (format: "0xabc...-0")
        SPLIT_PART(oa.operator_set_id, '-', 1) as avs_id,
        oa.strategy_id,
        oa.magnitude,
        oa.allocated_at,
        oa.operator_set_id
    FROM operator_allocations oa
    WHERE oa.operator_id = :operator_id
)
SELECT 
    operator_id,
    avs_id,
    strategy_id,
    SUM(magnitude) as total_allocated_magnitude,
    COUNT(DISTINCT operator_set_id) as active_allocation_count,
    MAX(allocated_at) as last_allocated_at,
    NOW() as updated_at
FROM allocations_with_avs
GROUP BY operator_id, avs_id, strategy_id;
"""

# Insert query
avs_allocation_summary_insert_query = """
INSERT INTO operator_avs_allocation_summary (
    id, operator_id, avs_id, strategy_id,
    total_allocated_magnitude, active_allocation_count,
    last_allocated_at, created_at, updated_at
)
VALUES (
    :id, :operator_id, :avs_id, :strategy_id,
    :total_allocated_magnitude, :active_allocation_count,
    :last_allocated_at, :updated_at, :updated_at
)
ON CONFLICT (id) DO UPDATE SET
    total_allocated_magnitude = EXCLUDED.total_allocated_magnitude,
    active_allocation_count = EXCLUDED.active_allocation_count,
    last_allocated_at = EXCLUDED.last_allocated_at,
    updated_at = EXCLUDED.updated_at;
"""


class AVSAllocationSummaryQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        query = avs_allocation_summary_fetch_query
        params = {"operator_id": operator_id}

        # For historical snapshots, we'd need to add block filtering
        # This would require storing allocated_at_block in operator_allocations
        if up_to_block is not None:
            # Add filter if we have block numbers
            query = query.replace(
                "WHERE oa.operator_id = :operator_id",
                "WHERE oa.operator_id = :operator_id AND oa.allocated_at_block <= :up_to_block",
            )
            params["up_to_block"] = up_to_block

        return query, params

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return avs_allocation_summary_insert_query

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        return f"{row['operator_id']}-{row['avs_id']}-{row['strategy_id']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "avs_id",
            "strategy_id",
            "total_allocated_magnitude",
            "active_allocation_count",
            "last_allocated_at",
            "updated_at",
        ]
