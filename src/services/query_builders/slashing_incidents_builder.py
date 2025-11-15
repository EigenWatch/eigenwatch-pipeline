# services/query_builders/slashing_incidents_builder.py
from .base_builder import BaseQueryBuilder

slashing_incidents_query = """
SELECT 
    operator_id,
    operator_set_id,
    slashed_at,
    block_number as slashed_at_block,
    description,
    transaction_hash,
    NOW() as created_at,
    NOW() as updated_at
FROM slashing_events_cache
WHERE operator_id = :operator_id
"""


class SlashingIncidentsQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str):
        return slashing_incidents_query, {"operator_id": operator_id}

    def build_insert_query(self) -> str:
        return """
INSERT INTO operator_slashing_incidents (
    operator_id, operator_set_id, slashed_at, slashed_at_block,
    description, transaction_hash, created_at, updated_at
)
VALUES (
    :operator_id, :operator_set_id, :slashed_at, :slashed_at_block,
    :description, :transaction_hash, :created_at, :updated_at
)
ON CONFLICT DO NOTHING
"""

    def generate_id(self, row: dict) -> str:
        return (
            f"{row['operator_id']}-{row['slashed_at_block']}-{row['transaction_hash']}"
        )

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "operator_set_id",
            "slashed_at",
            "slashed_at_block",
            "description",
            "transaction_hash",
            "created_at",
            "updated_at",
        ]
