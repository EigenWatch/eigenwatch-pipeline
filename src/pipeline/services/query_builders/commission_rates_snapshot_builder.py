# query_builders/commission_rates_snapshot_builder.py

from .base_builder import BaseQueryBuilder
from typing import Tuple, Dict, Optional


class CommissionRatesSnapshotQueryBuilder(BaseQueryBuilder):
    """Builds queries for commission rates snapshots"""

    def build_fetch_query(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> Tuple[str, Dict]:
        """
        Get latest commission rates for all types (PI, AVS, OPERATOR_SET) up to a block.
        Fetches from EVENTS DB only.
        """

        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND block_number <= :up_to_block"
            params["up_to_block"] = up_to_block

        query = f"""
        WITH pi_commission AS (
            SELECT DISTINCT ON (operator_id)
                operator_id,
                'PI' as commission_type,
                NULL::text as avs_id,
                NULL::text as operator_set_id,
                new_operator_pi_split_bips as current_bips
            FROM operator_pi_split_bips_set_events
            WHERE operator_id = :operator_id
            {block_filter}
            ORDER BY operator_id, block_number DESC, log_index DESC
        ),
        avs_commission AS (
            SELECT DISTINCT ON (avs_id)
                operator_id,
                'AVS' as commission_type,
                avs_id,
                NULL::text as operator_set_id,
                new_operator_avs_split_bips as current_bips
            FROM operator_avs_split_bips_set_events
            WHERE operator_id = :operator_id
            {block_filter}
            ORDER BY avs_id, block_number DESC, log_index DESC
        ),
        operator_set_commission AS (
            SELECT DISTINCT ON (operator_set_id)
                operator_id,
                'OPERATOR_SET' as commission_type,
                NULL::text as avs_id,
                operator_set_id,
                new_operator_set_split_bips as current_bips
            FROM operator_set_split_bips_set_events
            WHERE operator_id = :operator_id
            {block_filter}
            ORDER BY operator_set_id, block_number DESC, log_index DESC
        )
        SELECT 
            operator_id,
            commission_type,
            avs_id,
            operator_set_id,
            current_bips
        FROM (
            SELECT * FROM pi_commission
            UNION ALL
            SELECT * FROM avs_commission
            UNION ALL
            SELECT * FROM operator_set_commission
        ) all_commissions
        ORDER BY commission_type, COALESCE(avs_id, operator_set_id)
        """

        return query, params

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        """Only used for snapshots"""
        if not is_snapshot:
            raise ValueError("Commission rates snapshots are snapshot-only")

        return """
        INSERT INTO operator_commission_rates_snapshots (
            operator_id, commission_type, avs_id, operator_set_id,
            snapshot_date, snapshot_block, current_bips
        )
        VALUES (
            :operator_id, :commission_type, :avs_id, :operator_set_id,
            :snapshot_date, :snapshot_block, :current_bips
        )
        ON CONFLICT (operator_id, commission_type, snapshot_date) DO UPDATE SET
            snapshot_block = EXCLUDED.snapshot_block,
            avs_id = EXCLUDED.avs_id,
            operator_set_id = EXCLUDED.operator_set_id,
            current_bips = EXCLUDED.current_bips
        """

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        """Snapshots use auto-increment IDs"""
        return None

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "commission_type",
            "avs_id",
            "operator_set_id",
            "current_bips",
        ]
