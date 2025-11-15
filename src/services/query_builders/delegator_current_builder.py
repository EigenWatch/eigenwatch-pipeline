# services/query_builders/delegator_current_builder.py
from .base_builder import BaseQueryBuilder

delegator_current_query = """
WITH latest_delegation AS (
    SELECT DISTINCT ON (staker_id)
        staker_id,
        delegation_type,
        event_timestamp
    FROM operator_delegator_history
    WHERE operator_id = :operator_id
    ORDER BY staker_id, event_timestamp DESC
),
first_delegation AS (
    SELECT 
        staker_id,
        MIN(event_timestamp) as delegated_at
    FROM operator_delegator_history
    WHERE operator_id = :operator_id
        AND delegation_type = 'DELEGATED'
    GROUP BY staker_id
)
SELECT 
    :operator_id as operator_id,
    ld.staker_id,
    CASE WHEN ld.delegation_type = 'DELEGATED' THEN TRUE ELSE FALSE END as is_delegated,
    fd.delegated_at,
    CASE WHEN ld.delegation_type != 'DELEGATED' THEN ld.event_timestamp END as undelegated_at,
    NOW() as updated_at
FROM latest_delegation ld
LEFT JOIN first_delegation fd ON ld.staker_id = fd.staker_id
"""


class DelegatorCurrentQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str):
        return delegator_current_query, {"operator_id": operator_id}

    def build_insert_query(self) -> str:
        return """
INSERT INTO operator_delegators (
    id, operator_id, staker_id, is_delegated, delegated_at, undelegated_at, updated_at
)
VALUES (
    :id, :operator_id, :staker_id, :is_delegated, :delegated_at, :undelegated_at, :updated_at
)
ON CONFLICT (id) DO UPDATE SET
    is_delegated = EXCLUDED.is_delegated,
    delegated_at = EXCLUDED.delegated_at,
    undelegated_at = EXCLUDED.undelegated_at,
    updated_at = EXCLUDED.updated_at
"""

    def generate_id(self, row: dict) -> str:
        return f"{row['operator_id']}-{row['staker_id']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "staker_id",
            "is_delegated",
            "delegated_at",
            "undelegated_at",
            "updated_at",
        ]
