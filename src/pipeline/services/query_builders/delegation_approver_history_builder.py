from typing import Optional
from .base_builder import BaseQueryBuilder

# Delegation Approver History
delegation_approver_history_fetch_query = """
WITH registration_event AS (
    SELECT 
        operator_id,
        delegation_approver AS new_delegation_approver,
        NULL::text AS old_delegation_approver,
        block_timestamp AS changed_at,
        block_number AS changed_at_block,
        transaction_hash,
        log_index,
        'REGISTRATION' AS event_type
    FROM operator_registered_events
    WHERE operator_id = :operator_id
),
approver_updates AS (
    SELECT 
        operator_id,
        new_delegation_approver,
        LAG(new_delegation_approver) OVER (
            PARTITION BY operator_id 
            ORDER BY block_number, log_index
        ) AS old_delegation_approver,
        block_timestamp AS changed_at,
        block_number AS changed_at_block,
        transaction_hash,
        log_index,
        'UPDATE' AS event_type
    FROM delegation_approver_updated_events
    WHERE operator_id = :operator_id
),
combined AS (
    SELECT * FROM registration_event
    UNION ALL
    SELECT * FROM approver_updates
)
SELECT 
    operator_id,
    old_delegation_approver,
    new_delegation_approver,
    changed_at,
    changed_at_block,
    transaction_hash,
    NOW() AS updated_at
FROM combined
ORDER BY changed_at_block, log_index;
"""

delegation_approver_history_insert_query = """
INSERT INTO operator_delegation_approver_history (
    operator_id, old_delegation_approver, new_delegation_approver,
    changed_at, changed_at_block, transaction_hash, created_at, updated_at
)
VALUES (
    :operator_id, :old_delegation_approver, :new_delegation_approver,
    :changed_at, :changed_at_block, :transaction_hash, :updated_at, :updated_at
)
ON CONFLICT DO NOTHING;
"""


class DelegationApproverHistoryQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        query = delegation_approver_history_fetch_query
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            # Add block filter to both CTEs
            query = query.replace(
                "FROM operator_registered_events\n    WHERE operator_id = :operator_id",
                "FROM operator_registered_events\n    WHERE operator_id = :operator_id AND block_number <= :up_to_block",
            ).replace(
                "FROM delegation_approver_updated_events\n    WHERE operator_id = :operator_id",
                "FROM delegation_approver_updated_events\n    WHERE operator_id = :operator_id AND block_number <= :up_to_block",
            )
            params["up_to_block"] = up_to_block

        return query, params

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return delegation_approver_history_insert_query

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        # Auto-increment ID
        return None

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "old_delegation_approver",
            "new_delegation_approver",
            "changed_at",
            "changed_at_block",
            "transaction_hash",
            "updated_at",
        ]
