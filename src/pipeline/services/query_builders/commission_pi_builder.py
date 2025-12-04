# services/query_builders/commission_pi_builder.py
from typing import Optional
from .base_builder import BaseQueryBuilder

commission_pi_query = """
SELECT DISTINCT ON (operator_id)
    operator_id,
    'PI' as commission_type,
    new_operator_pi_split_bips as current_bips,
    activated_at as current_activated_at,
    block_number as current_set_at_block,
    old_operator_pi_split_bips as previous_bips,
    block_timestamp as first_set_at,
    NOW() as updated_at
FROM operator_pi_split_bips_set_events
WHERE operator_id = :operator_id
ORDER BY operator_id, block_number DESC, log_index DESC
"""


class CommissionPIQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str, up_to_block: Optional[int] = None):
        return commission_pi_query, {"operator_id": operator_id}

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        return """
INSERT INTO operator_commission_rates (
    id, operator_id, commission_type, current_bips, current_activated_at,
    current_set_at_block, previous_bips, first_set_at, updated_at
)
VALUES (
    :id, :operator_id, :commission_type, :current_bips, :current_activated_at,
    :current_set_at_block, :previous_bips, :first_set_at, :updated_at
)
ON CONFLICT (id) DO UPDATE SET
    current_bips = EXCLUDED.current_bips,
    current_activated_at = EXCLUDED.current_activated_at,
    current_set_at_block = EXCLUDED.current_set_at_block,
    previous_bips = EXCLUDED.previous_bips,
    first_set_at = EXCLUDED.first_set_at,
    updated_at = EXCLUDED.updated_at
"""

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        return f"{row['operator_id']}-{row['commission_type']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "commission_type",
            "current_bips",
            "current_activated_at",
            "current_set_at_block",
            "previous_bips",
            "first_set_at",
            "updated_at",
        ]
