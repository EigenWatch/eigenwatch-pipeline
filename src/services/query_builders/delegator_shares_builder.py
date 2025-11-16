# services/query_builders/delegator_shares_builder.py
from typing import Optional
from .base_builder import BaseQueryBuilder

delegator_shares_query = """
WITH share_events AS (
    SELECT 
        operator_id,
        staker_id,
        strategy_id,
        shares,
        event_type,
        block_number,
        log_index,
        block_timestamp
    FROM operator_share_events
    WHERE operator_id = :operator_id
    ORDER BY strategy_id, staker_id, block_number, log_index
),
cumulative_shares AS (
    SELECT 
        operator_id,
        staker_id,
        strategy_id,
        SUM(
            CASE 
                WHEN event_type = 'INCREASED' THEN shares
                WHEN event_type = 'DECREASED' THEN -shares
                ELSE 0
            END
        ) as total_shares,
        MAX(block_timestamp) as shares_updated_at,
        MAX(block_number) as shares_updated_block
    FROM share_events
    GROUP BY operator_id, staker_id, strategy_id
    HAVING SUM(
        CASE 
            WHEN event_type = 'INCREASED' THEN shares
            WHEN event_type = 'DECREASED' THEN -shares
            ELSE 0
        END
    ) > 0
)
SELECT
    operator_id,
    staker_id,
    strategy_id,
    total_shares as shares,
    shares_updated_at,
    shares_updated_block,
    NOW() as updated_at
FROM cumulative_shares
"""


class DelegatorSharesQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        return delegator_shares_query, {"operator_id": operator_id}

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return """
INSERT INTO operator_delegator_shares (
    id, operator_id, staker_id, strategy_id, shares, shares_updated_at,
    shares_updated_block, updated_at
)
VALUES (
    :id, :operator_id, :staker_id, :strategy_id, :shares, :shares_updated_at,
    :shares_updated_block, :updated_at
)
ON CONFLICT (id) DO UPDATE SET
    shares = EXCLUDED.shares,
    shares_updated_at = EXCLUDED.shares_updated_at,
    shares_updated_block = EXCLUDED.shares_updated_block,
    updated_at = EXCLUDED.updated_at
"""

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        return f"{row['operator_id']}-{row['staker_id']}-{row['strategy_id']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "staker_id",
            "strategy_id",
            "shares",
            "shares_updated_at",
            "shares_updated_block",
            "updated_at",
        ]
