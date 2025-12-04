# query_builders/allocation_snapshot_builder.py

from .base_builder import BaseQueryBuilder
from typing import Tuple, Dict, Optional


class AllocationSnapshotQueryBuilder(BaseQueryBuilder):
    """Builds queries for allocation snapshots"""

    def build_fetch_query(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> Tuple[str, Dict]:
        """
        Get latest allocations for each operator-set-strategy combination up to a block.
        """

        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND block_number <= :up_to_block"
            params["up_to_block"] = up_to_block

        query = f"""
        SELECT DISTINCT ON (operator_set_id, strategy_id)
            operator_id,
            operator_set_id,
            strategy_id,
            magnitude
        FROM allocation_events
        WHERE operator_id = :operator_id
        {block_filter}
        ORDER BY operator_set_id, strategy_id, block_number DESC, log_index DESC
        """

        return query, params

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        """Only used for snapshots"""
        if not is_snapshot:
            raise ValueError("Allocation snapshots are snapshot-only")

        return """
        INSERT INTO operator_allocation_snapshots (
            operator_id, operator_set_id, strategy_id,
            snapshot_date, snapshot_block, magnitude
        )
        VALUES (
            :operator_id, :operator_set_id, :strategy_id,
            :snapshot_date, :snapshot_block, :magnitude
        )
        ON CONFLICT (operator_id, operator_set_id, strategy_id, snapshot_date) DO UPDATE SET
            snapshot_block = EXCLUDED.snapshot_block,
            magnitude = EXCLUDED.magnitude
        """

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        """Snapshots use auto-increment IDs"""
        return None

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "operator_set_id",
            "strategy_id",
            "magnitude",
        ]
