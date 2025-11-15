# analytics_pipeline/assets/snapshots.py
"""
Snapshot Assets - Daily snapshots of operator state
"""

from dagster import asset, OpExecutionContext, Output, DailyPartitionsDefinition
from datetime import datetime, timezone
from ..resources import DatabaseResource, ConfigResource


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    partitions_def=daily_partitions,
    description="Creates daily snapshots of operator_current_state",
    compute_kind="sql",
)
def operator_daily_snapshots_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """
    Create daily snapshot of operator state.
    Only runs if current time is past snapshot hour (default: midnight UTC).
    """
    partition_date_str = context.partition_key
    snapshot_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    # Check if we should create snapshot (only after snapshot hour)
    current_hour = datetime.now(timezone.utc).hour
    if current_hour < config.snapshot_hour_utc:
        context.log.info(
            f"Skipping snapshot creation - current hour {current_hour} "
            f"< snapshot hour {config.snapshot_hour_utc}"
        )
        return Output(0, metadata={"skipped": True})

    context.log.info(f"Creating snapshot for {snapshot_date}")

    query = """
        WITH snapshot_data AS (
            SELECT
                operator_id,
                :snapshot_date as snapshot_date,
                0 as snapshot_block,  -- Could get from latest event
                total_delegators as delegator_count,
                active_avs_count,
                active_operator_set_count,
                current_pi_split_bips as pi_split_bips,
                total_slash_events as slash_event_count_to_date,
                operational_days,
                is_active
            FROM operator_current_state
        )
        INSERT INTO operator_daily_snapshots (
            operator_id, snapshot_date, snapshot_block,
            delegator_count, active_avs_count, active_operator_set_count,
            pi_split_bips, slash_event_count_to_date, operational_days, is_active
        )
        SELECT * FROM snapshot_data
        ON CONFLICT (operator_id, snapshot_date) DO UPDATE SET
            delegator_count = EXCLUDED.delegator_count,
            active_avs_count = EXCLUDED.active_avs_count,
            active_operator_set_count = EXCLUDED.active_operator_set_count,
            pi_split_bips = EXCLUDED.pi_split_bips,
            slash_event_count_to_date = EXCLUDED.slash_event_count_to_date,
            operational_days = EXCLUDED.operational_days,
            is_active = EXCLUDED.is_active
    """

    rowcount = db.execute_update(
        query, {"snapshot_date": snapshot_date}, db="analytics"
    )

    context.log.info(f"Created {rowcount} operator snapshots for {snapshot_date}")

    return Output(
        rowcount,
        metadata={
            "snapshot_date": str(snapshot_date),
            "operators_snapshotted": rowcount,
        },
    )


@asset(
    partitions_def=daily_partitions,
    description="Creates daily snapshots of operator_strategy_state",
    compute_kind="sql",
)
def operator_strategy_daily_snapshots_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """Create daily snapshot of per-strategy state"""
    partition_date_str = context.partition_key
    snapshot_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    current_hour = datetime.now(timezone.utc).hour
    if current_hour < config.snapshot_hour_utc:
        return Output(0, metadata={"skipped": True})

    query = """
        INSERT INTO operator_strategy_daily_snapshots (
            operator_id, strategy_id, snapshot_date, snapshot_block,
            max_magnitude, encumbered_magnitude, utilization_rate
        )
        SELECT 
            operator_id,
            strategy_id,
            :snapshot_date as snapshot_date,
            0 as snapshot_block,
            max_magnitude,
            encumbered_magnitude,
            utilization_rate,
        FROM operator_strategy_state
        ON CONFLICT (operator_id, strategy_id, snapshot_date) DO UPDATE SET
            max_magnitude = EXCLUDED.max_magnitude,
            encumbered_magnitude = EXCLUDED.encumbered_magnitude,
            utilization_rate = EXCLUDED.utilization_rate,
    """

    rowcount = db.execute_update(
        query, {"snapshot_date": snapshot_date}, db="analytics"
    )

    return Output(
        rowcount,
        metadata={
            "snapshot_date": str(snapshot_date),
            "strategy_records_snapshotted": rowcount,
        },
    )
