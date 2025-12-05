from .base_builder import BaseQueryBuilder
from typing import Tuple, Dict, Optional


class OperatorDailySnapshotQueryBuilder(BaseQueryBuilder):
    """Builds queries for operator daily snapshots"""

    def build_fetch_query(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> Tuple[str, Dict]:
        """
        Aggregate operator metrics as of a specific block.

        NOTE: This is split across multiple methods because data comes from
        both events and analytics DBs. This method only fetches from ANALYTICS DB.
        """

        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND status_changed_block <= :up_to_block"
            params["up_to_block"] = up_to_block

        # Query ANALYTICS DB only
        query = f"""
        WITH operator_avs_counts AS (
            SELECT 
                COUNT(DISTINCT avs_id) FILTER (WHERE current_status = 'REGISTERED') as active_avs_count
            FROM (
                SELECT DISTINCT ON (avs_id)
                    avs_id,
                    status as current_status
                FROM operator_avs_registration_history
                WHERE operator_id = :operator_id
                {block_filter}
                ORDER BY avs_id, status_changed_block DESC, id DESC
            ) latest_avs_status
        ),
        operator_set_counts AS (
            SELECT 
                COUNT(DISTINCT operator_set_id) as active_operator_set_count
            FROM operator_allocations
            WHERE operator_id = :operator_id
            {block_filter.replace('status_changed_block', 'allocated_at_block') if block_filter else ''}
        ),
        operator_slash_counts AS (
            SELECT 
                COUNT(*) as slash_event_count_to_date
            FROM operator_slashing_incidents
            WHERE operator_id = :operator_id
            {block_filter.replace('status_changed_block', 'slashed_at_block') if block_filter else ''}
        )
        SELECT
            :operator_id as operator_id,
            COALESCE(oac.active_avs_count, 0) as active_avs_count,
            COALESCE(osc_count.active_operator_set_count, 0) as active_operator_set_count,
            COALESCE(osc.slash_event_count_to_date, 0) as slash_event_count_to_date,
            0 as operational_days,
            TRUE as is_active
        FROM operator_avs_counts oac
        CROSS JOIN operator_set_counts osc_count
        CROSS JOIN operator_slash_counts osc
        """

        return query, params

    def fetch_events_metrics(
        self, db, operator_id: str, up_to_block: Optional[int] = None
    ) -> Dict:
        """
        Fetch metrics from EVENTS DB.
        Returns dict with delegator_count and pi_split_bips.
        """
        block_filter = ""
        params = {"operator_id": operator_id}

        if up_to_block is not None:
            block_filter = "AND block_number <= :up_to_block"
            params["up_to_block"] = up_to_block

        query = f"""
        WITH operator_delegator_counts AS (
            SELECT 
                COUNT(DISTINCT staker_id) as delegator_count
            FROM (
                SELECT DISTINCT ON (staker_id)
                    staker_id,
                    delegation_type
                FROM staker_delegation_events
                WHERE operator_id = :operator_id
                {block_filter}
                ORDER BY staker_id, block_number DESC, log_index DESC
            ) latest_delegations
            WHERE delegation_type = 'DELEGATED'
        ),
        operator_pi_commission AS (
            SELECT DISTINCT ON (operator_id)
                new_operator_pi_split_bips as pi_split_bips
            FROM operator_pi_split_bips_set_events
            WHERE operator_id = :operator_id
            {block_filter}
            ORDER BY operator_id, block_number DESC, log_index DESC
        )
        SELECT
            COALESCE(odc.delegator_count, 0) as delegator_count,
            opc.pi_split_bips
        FROM operator_delegator_counts odc
        LEFT JOIN operator_pi_commission opc ON TRUE
        """

        result = db.execute_query(query, params, db="events")
        if result and len(result) > 0:
            return {"delegator_count": result[0][0], "pi_split_bips": result[0][1]}
        return {"delegator_count": 0, "pi_split_bips": None}

    def build_insert_query(self, is_snapshot: bool = False) -> str:
        """Only used for snapshots"""
        if not is_snapshot:
            raise ValueError("Operator daily snapshots are snapshot-only")

        return """
        INSERT INTO operator_daily_snapshots (
            operator_id, snapshot_date, snapshot_block,
            delegator_count, active_avs_count, active_operator_set_count,
            pi_split_bips, slash_event_count_to_date, operational_days, is_active
        )
        VALUES (
            :operator_id, :snapshot_date, :snapshot_block,
            :delegator_count, :active_avs_count, :active_operator_set_count,
            :pi_split_bips, :slash_event_count_to_date, :operational_days, :is_active
        )
        ON CONFLICT (operator_id, snapshot_date) DO UPDATE SET
            snapshot_block = EXCLUDED.snapshot_block,
            delegator_count = EXCLUDED.delegator_count,
            active_avs_count = EXCLUDED.active_avs_count,
            active_operator_set_count = EXCLUDED.active_operator_set_count,
            pi_split_bips = EXCLUDED.pi_split_bips,
            slash_event_count_to_date = EXCLUDED.slash_event_count_to_date,
            operational_days = EXCLUDED.operational_days,
            is_active = EXCLUDED.is_active
        """

    def generate_id(self, row: dict, is_snapshot: bool = False) -> str:
        """Snapshots use auto-increment IDs"""
        return None

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "active_avs_count",
            "active_operator_set_count",
            "slash_event_count_to_date",
            "operational_days",
            "is_active",
        ]
