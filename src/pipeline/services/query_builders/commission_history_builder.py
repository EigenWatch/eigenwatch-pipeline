# query_builders/commission_history_builder.py

from typing import Optional
from .base_builder import BaseQueryBuilder


# Fetch query - combines all commission change events
commission_history_fetch_query = """
WITH pi_changes AS (
    SELECT 
        operator_id,
        'PI' as commission_type,
        NULL::text as avs_id,
        NULL::text as operator_set_id,
        old_operator_pi_split_bips as old_bips,
        new_operator_pi_split_bips as new_bips,
        (new_operator_pi_split_bips - old_operator_pi_split_bips) as change_delta,
        block_timestamp as changed_at,
        activated_at,
        (activated_at - block_timestamp) as activation_delay_seconds,
        caller,
        block_number,
        transaction_hash,
        id as event_id
    FROM operator_pi_split_bips_set_events
    WHERE operator_id = :operator_id
),
avs_changes AS (
    SELECT 
        operator_id,
        'AVS' as commission_type,
        avs_id,
        NULL::text as operator_set_id,
        old_operator_avs_split_bips as old_bips,
        new_operator_avs_split_bips as new_bips,
        (new_operator_avs_split_bips - old_operator_avs_split_bips) as change_delta,
        block_timestamp as changed_at,
        activated_at,
        (activated_at - block_timestamp) as activation_delay_seconds,
        caller,
        block_number,
        transaction_hash,
        id as event_id
    FROM operator_avs_split_bips_set_events
    WHERE operator_id = :operator_id
),
operator_set_changes AS (
    SELECT 
        operator_id,
        'OPERATOR_SET' as commission_type,
        NULL::text as avs_id,
        operator_set_id,
        old_operator_set_split_bips as old_bips,
        new_operator_set_split_bips as new_bips,
        (new_operator_set_split_bips - old_operator_set_split_bips) as change_delta,
        block_timestamp as changed_at,
        activated_at,
        (activated_at - block_timestamp) as activation_delay_seconds,
        caller,
        block_number,
        transaction_hash,
        id as event_id
    FROM operator_set_split_bips_set_events
    WHERE operator_id = :operator_id
)
SELECT 
    operator_id,
    commission_type,
    avs_id,
    operator_set_id,
    old_bips,
    new_bips,
    change_delta,
    changed_at,
    activated_at,
    activation_delay_seconds,
    caller,
    block_number,
    event_id
FROM (
    SELECT * FROM pi_changes
    UNION ALL
    SELECT * FROM avs_changes
    UNION ALL
    SELECT * FROM operator_set_changes
) all_changes
ORDER BY changed_at, block_number;
"""

# Insert query
commission_history_insert_query = """
INSERT INTO operator_commission_history (
    operator_id, commission_type, avs_id, operator_set_id,
    old_bips, new_bips, change_delta,
    changed_at, activated_at, activation_delay_seconds,
    caller, block_number, event_id
)
VALUES (
    :operator_id, :commission_type, :avs_id, :operator_set_id,
    :old_bips, :new_bips, :change_delta,
    :changed_at, :activated_at, :activation_delay_seconds,
    :caller, :block_number, :event_id
)
ON CONFLICT DO NOTHING;
"""


class CommissionHistoryQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        query = commission_history_fetch_query
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            # Add block filter to all CTEs
            query = query.replace(
                "WHERE operator_id = :operator_id",
                "WHERE operator_id = :operator_id AND block_number <= :up_to_block",
                3,  # Replace in all 3 CTEs
            )
            params["up_to_block"] = up_to_block

        return query, params

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return commission_history_insert_query

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        # Auto-increment ID
        return None

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "commission_type",
            "avs_id",
            "operator_set_id",
            "old_bips",
            "new_bips",
            "change_delta",
            "changed_at",
            "activated_at",
            "activation_delay_seconds",
            "caller",
            "block_number",
            "event_id",
        ]
