# query_builders/avs_relationship_snapshot_builder.py

from .base_builder import BaseQueryBuilder
from typing import Tuple, Dict, Optional


class AVSRelationshipSnapshotQueryBuilder(BaseQueryBuilder):
    """Builds queries for AVS relationship snapshots"""

    def build_fetch_query(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> Tuple[str, Dict]:
        """
        Reconstruct AVS relationship state from EVENTS DB up to a specific block.
        Uses operator_avs_registration_status_updated_events.
        """

        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND block_number <= :up_to_block"
            params["up_to_block"] = up_to_block

        query = f"""
        WITH latest_status AS (
            SELECT DISTINCT ON (avs_id)
                avs_id AS avs_id,
                status AS current_status,
                to_timestamp(block_timestamp) AS status_changed_at,
                block_number AS status_changed_block
            FROM operator_avs_registration_status_updated_events
            WHERE operator_id = :operator_id
            {block_filter}
            ORDER BY avs_id, block_number DESC, log_index DESC
        ),

        registration_cycles AS (
            SELECT 
                avs_id AS avs_id,
                COUNT(*) FILTER (WHERE status = 'REGISTERED') AS total_registration_cycles,
                MIN(to_timestamp(block_timestamp)) FILTER (WHERE status = 'REGISTERED') AS first_registered_at,
                MAX(to_timestamp(block_timestamp)) FILTER (WHERE status = 'REGISTERED') AS last_registered_at
            FROM operator_avs_registration_status_updated_events
            WHERE operator_id = :operator_id
            {block_filter}
            GROUP BY avs_id
        ),

        registration_intervals AS (
            SELECT
                h.avs_id AS avs_id,
                to_timestamp(h.block_timestamp) AS start_time,
                COALESCE(
                    (
                        SELECT MIN(to_timestamp(h2.block_timestamp))
                        FROM operator_avs_registration_status_updated_events h2
                        WHERE h2.operator_id = h.operator_id
                        AND h2.avs_id = h.avs_id
                        AND h2.block_timestamp > h.block_timestamp
                        AND h2.status != 'REGISTERED'
                        {block_filter.replace('block_number', 'h2.block_number') if block_filter else ''}
                    ),
                    NOW()
                ) AS end_time
            FROM operator_avs_registration_status_updated_events h
            WHERE h.operator_id = :operator_id
            AND h.status = 'REGISTERED'
            {block_filter}
        ),

        normalized_intervals AS (
            SELECT
                avs_id,
                CASE WHEN end_time < start_time THEN start_time ELSE start_time END AS start_time,
                CASE WHEN end_time < start_time THEN start_time ELSE end_time END AS end_time
            FROM registration_intervals
        ),

        sorted_intervals AS (
            SELECT 
                avs_id,
                start_time,
                end_time,
                LAG(end_time) OVER (PARTITION BY avs_id ORDER BY start_time, end_time) AS prev_end
            FROM normalized_intervals
        ),

        marked_intervals AS (
            SELECT
                avs_id,
                start_time,
                end_time,
                CASE WHEN prev_end IS NULL OR start_time > prev_end THEN 1 ELSE 0 END AS is_new_group
            FROM sorted_intervals
        ),

        grouped_intervals AS (
            SELECT
                avs_id,
                start_time,
                end_time,
                SUM(is_new_group) OVER (PARTITION BY avs_id ORDER BY start_time, end_time ROWS UNBOUNDED PRECEDING) AS grp
            FROM marked_intervals
        ),

        merged_intervals AS (
            SELECT
                avs_id,
                MIN(start_time) AS merged_start,
                MAX(end_time) AS merged_end
            FROM grouped_intervals
            GROUP BY avs_id, grp
        ),

        registration_summary AS (
            SELECT
                avs_id,
                (SUM(EXTRACT(EPOCH FROM (merged_end - merged_start))) / 86400.0)::NUMERIC AS days_registered_to_date
            FROM merged_intervals
            GROUP BY avs_id
        )

        SELECT
            :operator_id AS operator_id,
            ls.avs_id,
            ls.current_status,
            COALESCE(rs.days_registered_to_date, 0) AS days_registered_to_date,
            CASE
                WHEN ls.current_status = 'REGISTERED'
                    THEN (EXTRACT(EPOCH FROM (NOW() - ls.status_changed_at)) / 86400.0)::NUMERIC
                ELSE 0
            END AS current_period_days,
            COALESCE(rc.total_registration_cycles, 0) AS total_registration_cycles
        FROM latest_status ls
        LEFT JOIN registration_cycles rc ON ls.avs_id = rc.avs_id
        LEFT JOIN registration_summary rs ON ls.avs_id = rs.avs_id
        WHERE ls.current_status IS NOT NULL
        """

        return query, params

    def fetch_analytics_metrics(
        self, db, operator_id: str, up_to_block: Optional[int] = None
    ) -> Dict[str, Dict]:
        """
        Fetch operator set counts and commission rates from ANALYTICS DB.
        Returns dict mapping avs_id -> {active_operator_set_count, avs_commission_bips}
        """
        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND oa.allocated_at_block <= :up_to_block"
            params["up_to_block"] = up_to_block

        query = f"""
        WITH operator_set_counts AS (
            SELECT 
                os.avs_id,
                COUNT(DISTINCT oa.operator_set_id) AS active_operator_set_count
            FROM operator_allocations oa
            JOIN operator_sets os ON oa.operator_set_id = os.id
            WHERE oa.operator_id = :operator_id
            {block_filter}
            GROUP BY os.avs_id
        ),
        commission_rates AS (
            SELECT DISTINCT ON (avs_id)
                avs_id,
                current_bips AS avs_commission_bips
            FROM operator_commission_rates
            WHERE operator_id = :operator_id
            AND commission_type = 'AVS'
            ORDER BY avs_id, current_activated_at DESC
        )
        SELECT
            COALESCE(osc.avs_id, cr.avs_id) AS avs_id,
            COALESCE(osc.active_operator_set_count, 0) AS active_operator_set_count,
            cr.avs_commission_bips
        FROM operator_set_counts osc
        FULL OUTER JOIN commission_rates cr ON osc.avs_id = cr.avs_id
        """

        result = db.execute_query(query, params, db="analytics")

        # Return as dict keyed by avs_id
        metrics = {}
        for row in result:
            metrics[row[0]] = {
                "active_operator_set_count": row[1],
                "avs_commission_bips": row[2],
            }
        return metrics

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        """Only used for snapshots"""
        if not is_snapshot:
            raise ValueError("AVS relationship snapshots are snapshot-only")

        return """
        INSERT INTO operator_avs_relationship_snapshots (
            operator_id, avs_id, snapshot_date, snapshot_block,
            current_status, days_registered_to_date, current_period_days,
            total_registration_cycles, active_operator_set_count, avs_commission_bips
        )
        VALUES (
            :operator_id, :avs_id, :snapshot_date, :snapshot_block,
            :current_status, :days_registered_to_date, :current_period_days,
            :total_registration_cycles, :active_operator_set_count, :avs_commission_bips
        )
        ON CONFLICT (operator_id, avs_id, snapshot_date) DO UPDATE SET
            snapshot_block = EXCLUDED.snapshot_block,
            current_status = EXCLUDED.current_status,
            days_registered_to_date = EXCLUDED.days_registered_to_date,
            current_period_days = EXCLUDED.current_period_days,
            total_registration_cycles = EXCLUDED.total_registration_cycles,
            active_operator_set_count = EXCLUDED.active_operator_set_count,
            avs_commission_bips = EXCLUDED.avs_commission_bips
        """

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        """Snapshots use auto-increment IDs"""
        return None

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "avs_id",
            "current_status",
            "days_registered_to_date",
            "current_period_days",
            "total_registration_cycles",
        ]
