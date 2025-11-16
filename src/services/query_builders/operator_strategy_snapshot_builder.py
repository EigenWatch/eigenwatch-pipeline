# query_builders/operator_strategy_snapshot_builder.py

from .base_builder import BaseQueryBuilder
from typing import Tuple, Dict, Optional


class OperatorStrategySnapshotQueryBuilder(BaseQueryBuilder):
    """Builds queries for operator-strategy daily snapshots"""

    def build_fetch_query(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> Tuple[str, Dict]:
        """
        Get operator-strategy state as of a specific block.
        """

        block_filter_mm = ""
        block_filter_em = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter_mm = "AND block_number <= :up_to_block"
            block_filter_em = "AND block_number <= :up_to_block"
            params["up_to_block"] = up_to_block

        query = f"""
        WITH latest_max_magnitude AS (
            SELECT DISTINCT ON (strategy_id)
                strategy_id,
                max_magnitude
            FROM max_magnitude_updated_events
            WHERE operator_id = :operator_id
            {block_filter_mm}
            ORDER BY strategy_id, block_number DESC, log_index DESC
        ),
        latest_encumbered_magnitude AS (
            SELECT DISTINCT ON (strategy_id)
                strategy_id,
                encumbered_magnitude
            FROM encumbered_magnitude_updated_events
            WHERE operator_id = :operator_id
            {block_filter_em}
            ORDER BY strategy_id, block_number DESC, log_index DESC
        )
        SELECT
            :operator_id AS operator_id,
            COALESCE(mm.strategy_id, em.strategy_id) AS strategy_id,
            COALESCE(mm.max_magnitude, 0) AS max_magnitude,
            COALESCE(em.encumbered_magnitude, 0) AS encumbered_magnitude,
            CASE 
                WHEN COALESCE(mm.max_magnitude, 0) > 0 
                THEN (COALESCE(em.encumbered_magnitude, 0)::NUMERIC / mm.max_magnitude::NUMERIC * 100)
                ELSE 0 
            END AS utilization_rate
        FROM latest_max_magnitude mm
        FULL OUTER JOIN latest_encumbered_magnitude em 
            ON mm.strategy_id = em.strategy_id
        """

        return query, params

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        """Only used for snapshots"""
        if not is_snapshot:
            raise ValueError("Operator strategy snapshots are snapshot-only")

        return """
        INSERT INTO operator_strategy_daily_snapshots (
            operator_id, strategy_id, snapshot_date, snapshot_block,
            max_magnitude, encumbered_magnitude, utilization_rate
        )
        VALUES (
            :operator_id, :strategy_id, :snapshot_date, :snapshot_block,
            :max_magnitude, :encumbered_magnitude, :utilization_rate
        )
        ON CONFLICT (operator_id, strategy_id, snapshot_date) DO UPDATE SET
            snapshot_block = EXCLUDED.snapshot_block,
            max_magnitude = EXCLUDED.max_magnitude,
            encumbered_magnitude = EXCLUDED.encumbered_magnitude,
            utilization_rate = EXCLUDED.utilization_rate
        """

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        """Snapshots use auto-increment IDs"""
        return None

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "strategy_id",
            "max_magnitude",
            "encumbered_magnitude",
            "utilization_rate",
        ]
