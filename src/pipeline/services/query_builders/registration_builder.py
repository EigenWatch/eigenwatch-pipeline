# query_builders/registration_builder.py

from typing import Optional
from .base_builder import BaseQueryBuilder


# Operator Registration
registration_fetch_query = """
SELECT 
    operator_id,
    delegation_approver,
    block_timestamp AS registered_at,
    block_number AS registration_block,
    transaction_hash,
    NOW() AS updated_at
FROM operator_registered_events
WHERE operator_id = :operator_id
ORDER BY block_number ASC, log_index ASC
LIMIT 1;
"""

registration_insert_query = """
INSERT INTO operator_registration (
    operator_id, delegation_approver, registered_at, registration_block,
    transaction_hash, created_at, updated_at
)
VALUES (
    :operator_id, :delegation_approver, :registered_at, :registration_block,
    :transaction_hash, :updated_at, :updated_at
)
ON CONFLICT (operator_id) DO UPDATE SET
    delegation_approver = EXCLUDED.delegation_approver,
    registered_at = EXCLUDED.registered_at,
    registration_block = EXCLUDED.registration_block,
    transaction_hash = EXCLUDED.transaction_hash,
    updated_at = EXCLUDED.updated_at;
"""


class OperatorRegistrationQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        query = registration_fetch_query
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            # For historical snapshots
            query = query.replace(
                "WHERE operator_id = :operator_id",
                "WHERE operator_id = :operator_id AND block_number <= :up_to_block",
            )
            params["up_to_block"] = up_to_block

        return query, params

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return registration_insert_query

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        # Primary key is operator_id itself
        return row["operator_id"]

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "delegation_approver",
            "registered_at",
            "registration_block",
            "transaction_hash",
            "updated_at",
        ]
