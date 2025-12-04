# services/query_builders/slashing_events_cache_builder.py
from typing import Optional
from .base_builder import BaseQueryBuilder

slashing_events_cache_query = """
SELECT 
    operator_id,
    operator_set_id,
    block_number,
    transaction_hash,
    block_timestamp as slashed_at,
    description,
    strategies,
    wad_slashed,
    NOW() as created_at,
    NOW() as updated_at
FROM operator_slashed_events
WHERE operator_id = :operator_id
"""


class SlashingEventsCacheQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        return slashing_events_cache_query, {"operator_id": operator_id}

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return """
INSERT INTO slashing_events_cache (
    operator_id, operator_set_id, block_number, transaction_hash,
    slashed_at, description, strategies, wad_slashed, created_at, updated_at
)
VALUES (
    :operator_id, :operator_set_id, :block_number, :transaction_hash,
    :slashed_at, :description, :strategies, :wad_slashed, :created_at, :updated_at
)
ON CONFLICT (operator_id, block_number, transaction_hash) DO UPDATE SET
    updated_at = EXCLUDED.updated_at
"""

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        return f"{row['operator_id']}-{row['block_number']}-{row['transaction_hash']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "operator_set_id",
            "block_number",
            "transaction_hash",
            "slashed_at",
            "description",
            "strategies",
            "wad_slashed",
            "created_at",
            "updated_at",
        ]
