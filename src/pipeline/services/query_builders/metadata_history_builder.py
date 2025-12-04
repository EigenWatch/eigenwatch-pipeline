from typing import Optional
from .base_builder import BaseQueryBuilder

# Metadata History
metadata_history_fetch_query = """
SELECT 
    operator_id,
    metadata_uri,
    NULL::jsonb AS metadata_json,
    NULL::timestamp AS metadata_fetched_at,
    block_timestamp AS updated_at,
    block_number AS updated_at_block,
    transaction_hash,
    NOW() AS created_at
FROM operator_metadata_update_events
WHERE operator_id = :operator_id
ORDER BY block_number ASC, log_index ASC;
"""

metadata_history_insert_query = """
INSERT INTO operator_metadata_history (
    operator_id, metadata_uri, metadata_json, metadata_fetched_at,
    updated_at, updated_at_block, transaction_hash, created_at
)
VALUES (
    :operator_id, :metadata_uri, :metadata_json, :metadata_fetched_at,
    :updated_at, :updated_at_block, :transaction_hash, :created_at
)
ON CONFLICT DO NOTHING;
"""


class OperatorMetadataHistoryQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        query = metadata_history_fetch_query
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            query = query.replace(
                "WHERE operator_id = :operator_id",
                "WHERE operator_id = :operator_id AND block_number <= :up_to_block",
            )
            params["up_to_block"] = up_to_block

        return query, params

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return metadata_history_insert_query

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        # Auto-increment ID
        return None

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "metadata_uri",
            "metadata_json",
            "metadata_fetched_at",
            "updated_at",
            "updated_at_block",
            "transaction_hash",
            "created_at",
        ]
