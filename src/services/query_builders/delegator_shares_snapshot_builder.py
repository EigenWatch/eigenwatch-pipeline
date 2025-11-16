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
        """

        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND block_number <= :up_to_block"
            params["up_to_block"] = up_to_block

        query = f"""
        WITH active_delegators AS (
            -- Get delegators and their delegation status as of the block
            SELECT DISTINCT ON (staker_id)
                staker_id,
                is_delegated
            FROM operator_delegator_history
            WHERE operator_id = :operator_id
            {block_filter.replace('block_number', 'event_block') if block_filter else ''}
            ORDER BY staker_id, event_block DESC, id DESC
        ),
        latest_shares AS (
            SELECT DISTINCT ON (staker_id, strategy_id)
                staker_id,
                strategy_id,
                shares
            FROM operator_delegator_shares_events
            WHERE operator_id = :operator_id
            {block_filter}
            ORDER BY staker_id, strategy_id, block_number DESC, log_index DESC
        )
        SELECT
            :operator_id as operator_id,
            ls.staker_id,
            ls.strategy_id,
            COALESCE(ls.shares, 0) as shares,
            ad.is_delegated
        FROM latest_shares ls
        INNER JOIN active_delegators ad 
            ON ls.staker_id = ad.staker_id
        WHERE ls.shares > 0
        """

        return query, params

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
            "is_delegated",
        ]
