# query_builders/delegator_shares_snapshot_builder.py

from .base_builder import BaseQueryBuilder
from typing import Tuple, Dict, Optional


class DelegatorSharesSnapshotQueryBuilder(BaseQueryBuilder):
    """Builds queries for delegator shares snapshots"""

    def build_fetch_query(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> Tuple[str, Dict]:
        """
        Get latest shares for each delegator-strategy combination up to a specific block.
        NOW INCLUDES is_delegated status.

        NOTE: This fetches from EVENTS DB only. The is_delegated field must be
        populated by fetching delegation status separately.
        """

        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND block_number <= :up_to_block"
            params["up_to_block"] = up_to_block

        # ONLY fetch from operator_share_events (events DB)
        query = f"""
        SELECT DISTINCT ON (staker_id, strategy_id)
            :operator_id as operator_id,
            staker_id as staker_id,
            strategy_id as strategy_id,
            shares
        FROM operator_share_events
        WHERE operator_id = :operator_id
        {block_filter}
        ORDER BY staker_id, strategy_id, block_number DESC, log_index DESC
        """

        return query, params

    def fetch_delegation_status(
        self, db, operator_id: str, up_to_block: Optional[int] = None
    ) -> Dict[str, bool]:
        """
        Separate method to fetch delegation status from events DB.
        Returns dict mapping staker_id -> is_delegated.
        """
        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND block_number <= :up_to_block"
            params["up_to_block"] = up_to_block

        query = f"""
        SELECT DISTINCT ON (staker_id)
            staker_id as staker_id,
            CASE 
                WHEN delegation_type = 'DELEGATED' THEN TRUE
                ELSE FALSE
            END as is_delegated
        FROM staker_delegation_events
        WHERE operator_id = :operator_id
        {block_filter}
        ORDER BY staker_id, block_number DESC, log_index DESC
        """

        result = db.execute_query(query, params, db="events")
        return {row[0]: row[1] for row in result}

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        """Only used for snapshots"""
        if not is_snapshot:
            raise ValueError("Delegator shares snapshots are snapshot-only")

        return """
        INSERT INTO operator_delegator_shares_snapshots (
            operator_id, staker_id, strategy_id, 
            snapshot_date, snapshot_block, shares, is_delegated
        )
        VALUES (
            :operator_id, :staker_id, :strategy_id,
            :snapshot_date, :snapshot_block, :shares, :is_delegated
        )
        ON CONFLICT (operator_id, staker_id, strategy_id, snapshot_date) DO UPDATE SET
            snapshot_block = EXCLUDED.snapshot_block,
            shares = EXCLUDED.shares,
            is_delegated = EXCLUDED.is_delegated
        """

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        """Snapshots use auto-increment IDs"""
        return None

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "staker_id",
            "strategy_id",
            "shares",
        ]
