from typing import Optional
from .base_builder import BaseQueryBuilder

# Fetch query - remove avs_id from SELECT
allocation_state_fetch_query = """
WITH latest_allocations AS (
    SELECT DISTINCT ON (operator_set_id, strategy_id)
        operator_id,
        operator_set_id,
        strategy_id,
        magnitude,
        effect_block,
        block_timestamp AS allocated_at,
        block_number AS allocated_at_block,
        NOW() AS updated_at
    FROM allocation_events
    WHERE operator_id = :operator_id
    ORDER BY operator_set_id, strategy_id, block_number DESC, log_index DESC
)
SELECT * FROM latest_allocations;
"""

# Insert query - remove avs_id
allocation_state_insert_query = """
INSERT INTO operator_allocations (
    id, operator_id, operator_set_id, strategy_id,
    magnitude, effect_block,
    allocated_at, allocated_at_block,
    updated_at
)
VALUES (
    :id, :operator_id, :operator_set_id, :strategy_id,
    :magnitude, :effect_block,
    :allocated_at, :allocated_at_block,
    :updated_at
)
ON CONFLICT (id) DO UPDATE SET
    magnitude = EXCLUDED.magnitude,
    effect_block = EXCLUDED.effect_block,
    allocated_at = EXCLUDED.allocated_at,
    allocated_at_block = EXCLUDED.allocated_at_block,
    updated_at = EXCLUDED.updated_at;
"""


class AllocationStateQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        return allocation_state_fetch_query, {"operator_id": operator_id}

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return allocation_state_insert_query

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        return f"{row['operator_id']}-{row['operator_set_id']}-{row['strategy_id']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "operator_set_id",
            "strategy_id",
            "magnitude",
            "effect_block",
            "allocated_at",
            "allocated_at_block",
            "updated_at",
        ]
