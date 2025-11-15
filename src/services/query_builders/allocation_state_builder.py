from .base_builder import BaseQueryBuilder

# Fetch query template (events DB)
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
        CASE WHEN effect_block <= :current_block THEN TRUE ELSE FALSE END AS is_active,
        CASE WHEN effect_block <= :current_block THEN TO_TIMESTAMP(block_timestamp) ELSE NULL END AS activated_at
    FROM allocation_events
    WHERE operator_id = :operator_id
    ORDER BY operator_set_id, strategy_id, block_number DESC, log_index DESC
),
allocation_with_metadata AS (
    SELECT
        la.*,
        os.avs_id,
        NOW() AS updated_at
    FROM latest_allocations la
    LEFT JOIN operator_sets os ON la.operator_set_id = os.id
    LEFT JOIN avs ON os.avs_id = avs.id
    LEFT JOIN strategies s ON la.strategy_id = s.id
)
SELECT * FROM allocation_with_metadata;
"""

# Insert query template (analytics DB)
allocation_state_insert_query = """
INSERT INTO operator_allocations (
    id, operator_id, operator_set_id, strategy_id,
    magnitude, effect_block, is_active,
    allocated_at, allocated_at_block, activated_at,
    avs_id, updated_at
)
VALUES (
    :id, :operator_id, :operator_set_id, :strategy_id,
    :magnitude, :effect_block, :is_active,
    :allocated_at, :allocated_at_block, :activated_at,
    :avs_id, :updated_at
)
ON CONFLICT (id) DO UPDATE SET
    magnitude = EXCLUDED.magnitude,
    effect_block = EXCLUDED.effect_block,
    is_active = EXCLUDED.is_active,
    allocated_at = EXCLUDED.allocated_at,
    allocated_at_block = EXCLUDED.allocated_at_block,
    activated_at = EXCLUDED.activated_at,
    avs_id = EXCLUDED.avs_id,
    updated_at = EXCLUDED.updated_at;
"""


class AllocationStateQueryBuilder(BaseQueryBuilder):
    def __init__(self, current_block: int):
        self.current_block = current_block

    def build_fetch_query(self, operator_id: str):
        return allocation_state_fetch_query, {
            "operator_id": operator_id,
            "current_block": self.current_block,
        }

    def build_insert_query(self) -> str:
        return allocation_state_insert_query

    def generate_id(self, row: dict) -> str:
        return f"{row['operator_id']}-{row['operator_set_id']}-{row['strategy_id']}"
