# services/query_builders/avs_relationship_history_builder.py
from .base_builder import BaseQueryBuilder

avs_relationship_history_query = """
SELECT 
    operator_id,
    avs_id,
    status::TEXT,
    block_timestamp as status_changed_at,
    block_number as status_changed_block,
    block_timestamp as period_start,
    transaction_hash,
    NOW() as created_at,
    NOW() as updated_at
FROM operator_avs_registration_status_updated_events
WHERE operator_id = :operator_id
"""


class AVSRelationshipHistoryQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str):
        return avs_relationship_history_query, {"operator_id": operator_id}

    def build_insert_query(self) -> str:
        return """
INSERT INTO operator_avs_registration_history (
    operator_id, avs_id, status, status_changed_at, status_changed_block,
    period_start, transaction_hash, created_at, updated_at
)
VALUES (
    :operator_id, :avs_id, :status, :status_changed_at, :status_changed_block,
    :period_start, :transaction_hash, :created_at, :updated_at
)
ON CONFLICT DO NOTHING
"""

    def generate_id(self, row: dict) -> str:
        return f"{row['operator_id']}-{row['avs_id']}-{row['status_changed_block']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "avs_id",
            "status",
            "status_changed_at",
            "status_changed_block",
            "period_start",
            "transaction_hash",
            "created_at",
            "updated_at",
        ]
