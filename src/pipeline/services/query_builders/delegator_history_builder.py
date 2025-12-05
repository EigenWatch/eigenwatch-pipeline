# services/query_builders/delegator_history_builder.py
from typing import Optional
from .base_builder import BaseQueryBuilder

delegator_history_query = """
SELECT 
    operator_id,
    staker_id,
    delegation_type::TEXT,
    block_timestamp as event_timestamp,
    block_number as event_block,
    transaction_hash,
    NOW() as created_at,
    NOW() as updated_at
FROM staker_delegation_events
WHERE operator_id = :operator_id

UNION ALL

SELECT 
    operator_id,
    staker_id,
    'FORCE_UNDELEGATED' as delegation_type,
    block_timestamp as event_timestamp,
    block_number as event_block,
    transaction_hash,
    NOW() as created_at,
    NOW() as updated_at
FROM staker_force_undelegated_events
WHERE operator_id = :operator_id
"""


class DelegatorHistoryQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        return delegator_history_query, {"operator_id": operator_id}

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return """
INSERT INTO operator_delegator_history (
    operator_id, staker_id, delegation_type, event_timestamp, event_block,
    transaction_hash, created_at, updated_at
)
VALUES (
    :operator_id, :staker_id, :delegation_type, :event_timestamp, :event_block,
    :transaction_hash, :created_at, :updated_at
)
ON CONFLICT DO NOTHING
"""

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        return f"{row['operator_id']}-{row['staker_id']}-{row['event_block']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "staker_id",
            "delegation_type",
            "event_timestamp",
            "event_block",
            "transaction_hash",
            "created_at",
            "updated_at",
        ]
