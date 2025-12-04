# query_builders/metadata_builder.py

from typing import Optional
from .base_builder import BaseQueryBuilder


# Current Metadata State
metadata_current_fetch_query = """
WITH latest_metadata AS (
    SELECT DISTINCT ON (operator_id)
        operator_id,
        metadata_uri,
        block_timestamp AS last_updated_at,
        block_number AS last_updated_block,
        transaction_hash,
        NOW() AS updated_at
    FROM operator_metadata_update_events
    WHERE operator_id = :operator_id
    ORDER BY operator_id, block_number DESC, log_index DESC
),
update_count AS (
    SELECT 
        operator_id,
        COUNT(*) as total_updates
    FROM operator_metadata_update_events
    WHERE operator_id = :operator_id
    GROUP BY operator_id
)
SELECT 
    lm.operator_id,
    lm.metadata_uri,
    NULL::jsonb AS metadata_json,
    NULL::timestamp AS metadata_fetched_at,
    lm.last_updated_at,
    lm.last_updated_block,
    COALESCE(uc.total_updates, 0) as total_updates,
    lm.updated_at
FROM latest_metadata lm
LEFT JOIN update_count uc ON lm.operator_id = uc.operator_id;
"""

metadata_current_insert_query = """
INSERT INTO operator_metadata (
    operator_id, metadata_uri, metadata_json, metadata_fetched_at,
    last_updated_at, last_updated_block, total_updates,
    created_at, updated_at
)
VALUES (
    :operator_id, :metadata_uri, :metadata_json, :metadata_fetched_at,
    :last_updated_at, :last_updated_block, :total_updates,
    :updated_at, :updated_at
)
ON CONFLICT (operator_id) DO UPDATE SET
    metadata_uri = EXCLUDED.metadata_uri,
    metadata_json = COALESCE(EXCLUDED.metadata_json, operator_metadata.metadata_json),
    metadata_fetched_at = COALESCE(EXCLUDED.metadata_fetched_at, operator_metadata.metadata_fetched_at),
    last_updated_at = EXCLUDED.last_updated_at,
    last_updated_block = EXCLUDED.last_updated_block,
    total_updates = EXCLUDED.total_updates,
    updated_at = EXCLUDED.updated_at;
"""


class OperatorMetadataQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        query = metadata_current_fetch_query
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            # Add block filter to both CTEs
            query = query.replace(
                "FROM operator_metadata_update_events\n    WHERE operator_id = :operator_id",
                "FROM operator_metadata_update_events\n    WHERE operator_id = :operator_id AND block_number <= :up_to_block",
                2,  # Replace both occurrences
            )
            params["up_to_block"] = up_to_block

        return query, params

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return metadata_current_insert_query

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        # Primary key is operator_id itself
        return row["operator_id"]

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "metadata_uri",
            "metadata_json",
            "metadata_fetched_at",
            "last_updated_at",
            "last_updated_block",
            "total_updates",
            "updated_at",
        ]
