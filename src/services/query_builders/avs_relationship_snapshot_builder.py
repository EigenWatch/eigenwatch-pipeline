# query_builders/avs_relationship_snapshot_builder.py

from .base_builder import BaseQueryBuilder
from typing import Tuple, Dict, Optional


class AVSRelationshipSnapshotQueryBuilder(BaseQueryBuilder):
    """Builds queries for AVS relationship snapshots"""

    def build_fetch_query(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> Tuple[str, Dict]:
        """
        Reconstruct AVS relationship state from history up to a specific block.
        This finds the latest status for each AVS as of the block.
        """

        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND status_changed_block <= :up_to_block"
            params["up_to_block"] = up_to_block

        query = f"""
        WITH latest_status AS (
            SELECT DISTINCT ON (avs_id)
                avs_id,
                status AS current_status,
                status_changed_at,
                status_changed_block
            FROM operator_avs_registration_history
            WHERE operator_id = :operator_id
            {block_filter}
            ORDER BY avs_id, status_changed_block DESC, id DESC
        ),

        -- registration_cycles unchanged (counts + first/last registered timestamps)
        registration_cycles AS (
            SELECT 
                avs_id,
                COUNT(*) FILTER (WHERE status = 'REGISTERED') AS total_registration_cycles,
                MIN(status_changed_at) FILTER (WHERE status = 'REGISTERED') AS first_registered_at,
                MAX(status_changed_at) FILTER (WHERE status = 'REGISTERED') AS last_registered_at
            FROM operator_avs_registration_history
            WHERE operator_id = :operator_id
            {block_filter}
            GROUP BY avs_id
        ),

        -- For each REGISTERED event, find the next non-REGISTERED status_changed_at for the same (operator_id, avs_id).
        -- If none exists, treat the interval as open until NOW().
        registration_intervals AS (
            SELECT
                h.avs_id,
                h.status_changed_at AS start_time,
                COALESCE(
                    (
                        SELECT MIN(h2.status_changed_at)
                        FROM operator_avs_registration_history h2
                        WHERE h2.operator_id = h.operator_id
                        AND h2.avs_id = h.avs_id
                        AND h2.status_changed_at > h.status_changed_at
                        AND h2.status <> 'REGISTERED'
                        {block_filter} -- apply same block_filter to the lookups if present
                    ),
                    NOW()
                ) AS end_time
            FROM operator_avs_registration_history h
            WHERE h.operator_id = :operator_id
            AND h.status = 'REGISTERED'
            {block_filter}
        ),

        -- Guard against bad data where end < start
        normalized_intervals AS (
            SELECT
                avs_id,
                CASE WHEN end_time < start_time THEN start_time ELSE start_time END AS start_time,
                CASE WHEN end_time < start_time THEN start_time ELSE end_time END AS end_time
            FROM registration_intervals
        ),

        -- Sort intervals to prepare merging
        sorted_intervals AS (
            SELECT 
                avs_id,
                start_time,
                end_time,
                LAG(end_time) OVER (PARTITION BY avs_id ORDER BY start_time, end_time) AS prev_end
            FROM normalized_intervals
        ),

        -- Mark boundaries where a new island starts (start_time > prev_end)
        marked_intervals AS (
            SELECT
                avs_id,
                start_time,
                end_time,
                CASE WHEN prev_end IS NULL OR start_time > prev_end THEN 1 ELSE 0 END AS is_new_group
            FROM sorted_intervals
        ),

        -- Assign group numbers (islands) by cumulative sum of is_new_group
        grouped_intervals AS (
            SELECT
                avs_id,
                start_time,
                end_time,
                SUM(is_new_group) OVER (PARTITION BY avs_id ORDER BY start_time, end_time ROWS UNBOUNDED PRECEDING) AS grp
            FROM marked_intervals
        ),

        -- Merge intervals in each group (min start, max end)
        merged_intervals AS (
            SELECT
                avs_id,
                MIN(start_time) AS merged_start,
                MAX(end_time)   AS merged_end
            FROM grouped_intervals
            GROUP BY avs_id, grp
        ),

        -- Sum merged intervals per AVS to get total active seconds, then convert to days
        registration_summary AS (
            SELECT
                avs_id,
                (SUM(EXTRACT(EPOCH FROM (merged_end - merged_start))) / 86400.0)::NUMERIC AS days_registered_to_date
            FROM merged_intervals
            GROUP BY avs_id
        ),

        -- operator set counts (unchanged)
        operator_set_counts AS (
            SELECT 
                os.avs_id,
                COUNT(DISTINCT oa.operator_set_id) AS active_operator_set_count
            FROM operator_allocations oa
            JOIN operator_sets os ON oa.operator_set_id = os.id
            WHERE oa.operator_id = :operator_id
            {block_filter.replace('status_changed_block', 'oa.allocated_at_block') if block_filter else ''}
            GROUP BY os.avs_id
        ),

        -- commission rates (unchanged)
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
            :operator_id AS operator_id,
            ls.avs_id,
            ls.current_status,
            COALESCE(rs.days_registered_to_date, 0) AS days_registered_to_date,
            CASE
                WHEN ls.current_status = 'REGISTERED'
                    THEN (EXTRACT(EPOCH FROM (NOW() - ls.status_changed_at)) / 86400.0)::NUMERIC
                ELSE 0
            END AS current_period_days,
            COALESCE(rc.total_registration_cycles, 0) AS total_registration_cycles,
            COALESCE(osc.active_operator_set_count, 0) AS active_operator_set_count,
            cr.avs_commission_bips
        FROM latest_status ls
        LEFT JOIN registration_cycles rc ON ls.avs_id = rc.avs_id
        LEFT JOIN registration_summary rs ON ls.avs_id = rs.avs_id
        LEFT JOIN operator_set_counts osc ON ls.avs_id = osc.avs_id
        LEFT JOIN commission_rates cr ON ls.avs_id = cr.avs_id
        WHERE ls.current_status IS NOT NULL;
        """

        return query, params

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
            "active_operator_set_count",
            "avs_commission_bips",
        ]
