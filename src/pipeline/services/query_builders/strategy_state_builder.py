from typing import Optional
from .base_builder import BaseQueryBuilder

# Fetch query (events DB)
strategy_state_fetch_query = """
WITH latest_max_magnitude AS (
    SELECT DISTINCT ON (strategy_id)
        strategy_id,
        max_magnitude,
        block_timestamp AS max_magnitude_updated_at,
        block_number AS max_magnitude_updated_block
    FROM max_magnitude_updated_events
    WHERE operator_id = :operator_id
    ORDER BY strategy_id, block_number DESC, log_index DESC
),
latest_encumbered_magnitude AS (
    SELECT DISTINCT ON (strategy_id)
        strategy_id,
        encumbered_magnitude,
        block_timestamp AS encumbered_magnitude_updated_at,
        block_number AS encumbered_magnitude_updated_block
    FROM encumbered_magnitude_updated_events
    WHERE operator_id = :operator_id
    ORDER BY strategy_id, block_number DESC, log_index DESC
)
SELECT
    :operator_id AS operator_id,
    COALESCE(mm.strategy_id, em.strategy_id) AS strategy_id,
    COALESCE(mm.max_magnitude, 0) AS max_magnitude,
    mm.max_magnitude_updated_at,
    mm.max_magnitude_updated_block,
    COALESCE(em.encumbered_magnitude, 0) AS encumbered_magnitude,
    em.encumbered_magnitude_updated_at,
    em.encumbered_magnitude_updated_block,
    CASE 
        WHEN COALESCE(mm.max_magnitude, 0) > 0 
        THEN (COALESCE(em.encumbered_magnitude, 0)::NUMERIC / mm.max_magnitude::NUMERIC * 100)
        ELSE 0 
    END AS utilization_rate,
    NOW() AS updated_at
FROM latest_max_magnitude mm
FULL OUTER JOIN latest_encumbered_magnitude em 
    ON mm.strategy_id = em.strategy_id;
"""

# Insert query (analytics DB)
strategy_state_insert_query = """
INSERT INTO operator_strategy_state (
    id, operator_id, strategy_id,
    max_magnitude, max_magnitude_updated_at, max_magnitude_updated_block,
    encumbered_magnitude, encumbered_magnitude_updated_at, encumbered_magnitude_updated_block,
    utilization_rate, updated_at
)
VALUES (
    :id, :operator_id, :strategy_id,
    :max_magnitude, :max_magnitude_updated_at, :max_magnitude_updated_block,
    :encumbered_magnitude, :encumbered_magnitude_updated_at, :encumbered_magnitude_updated_block,
    :utilization_rate, :updated_at
)
ON CONFLICT (id) DO UPDATE SET
    max_magnitude = EXCLUDED.max_magnitude,
    max_magnitude_updated_at = EXCLUDED.max_magnitude_updated_at,
    max_magnitude_updated_block = EXCLUDED.max_magnitude_updated_block,
    encumbered_magnitude = EXCLUDED.encumbered_magnitude,
    encumbered_magnitude_updated_at = EXCLUDED.encumbered_magnitude_updated_at,
    encumbered_magnitude_updated_block = EXCLUDED.encumbered_magnitude_updated_block,
    utilization_rate = EXCLUDED.utilization_rate,
    updated_at = EXCLUDED.updated_at;
"""


class StrategyStateQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        return strategy_state_fetch_query, {"operator_id": operator_id}

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return strategy_state_insert_query

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        return f"{row['operator_id']}-{row['strategy_id']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "strategy_id",
            "max_magnitude",
            "max_magnitude_updated_at",
            "max_magnitude_updated_block",
            "encumbered_magnitude",
            "encumbered_magnitude_updated_at",
            "encumbered_magnitude_updated_block",
            "utilization_rate",
            "updated_at",
        ]
