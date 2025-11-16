# defs/assets/snapshots.py
"""
Snapshot Assets - Daily snapshots of operator state
UPDATED VERSION with new assets and refactored existing ones
"""

from dagster import asset, OpExecutionContext, Output, DailyPartitionsDefinition
from datetime import datetime

from services.processors.process_operators_snapshot import (
    process_operators_for_snapshot,
)
from utils.operator_snapshot_utils import (
    get_operators_active_by_block,
    get_snapshot_block_for_date,
)
from ..resources import DatabaseResource, ConfigResource

# Import ALL reconstructors
from services.reconstructors.operator_daily_snapshot import (
    OperatorDailySnapshotReconstructor,
)
from services.reconstructors.operator_strategy_snapshot import (
    OperatorStrategySnapshotReconstructor,
)
from services.reconstructors.allocation_snapshot import (
    AllocationSnapshotReconstructor,
)
from services.reconstructors.avs_relationship_snapshot import (
    AVSRelationshipSnapshotReconstructor,
)
from services.reconstructors.delegator_shares_snapshot import (
    DelegatorSharesSnapshotReconstructor,
)
from services.reconstructors.commission_rates_snapshot import (
    CommissionRatesSnapshotReconstructor,
)


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


# ============================================================================
# REFACTORED EXISTING SNAPSHOTS (now using reconstructors)
# ============================================================================


@asset(
    partitions_def=daily_partitions,
    description="Daily snapshots of operator state (calculated from events)",
    compute_kind="sql",
)
def operator_daily_snapshots_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """
    Create daily snapshot of operator state.
    NOW USES RECONSTRUCTOR PATTERN.
    """
    partition_date_str = context.partition_key
    snapshot_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    # Get snapshot block
    snapshot_block = get_snapshot_block_for_date(
        db,
        snapshot_date,
    )

    if snapshot_block == 0:
        context.log.warning(f"No events found for or before {snapshot_date}")
        return Output(0, metadata={"skipped": True})

    context.log.info(
        f"Creating operator snapshot for {snapshot_date} (up to block {snapshot_block})"
    )

    # Get active operators
    operators = get_operators_active_by_block(db, snapshot_block)
    context.log.info(f"Found {len(operators)} operators to snapshot")

    # Use reconstructor
    reconstructor = OperatorDailySnapshotReconstructor(db, context.log)

    total_processed = process_operators_for_snapshot(
        context,
        db,
        config,
        operators,
        reconstructor,
        snapshot_date,
        snapshot_block,
        "Operator Daily",
    )

    return Output(
        total_processed,
        metadata={
            "snapshot_date": str(snapshot_date),
            "snapshot_block": snapshot_block,
            "operators_snapshotted": total_processed,
        },
    )


@asset(
    partitions_def=daily_partitions,
    description="Daily snapshots of operator-strategy state (from events)",
    compute_kind="sql",
)
def operator_strategy_daily_snapshots_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """
    Create daily snapshot of operator-strategy state.
    NOW USES RECONSTRUCTOR PATTERN.
    """

    partition_date_str = context.partition_key
    snapshot_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    # Get snapshot block
    snapshot_block = get_snapshot_block_for_date(
        db,
        snapshot_date,
    )

    if snapshot_block == 0:
        context.log.warning(f"No events found for or before {snapshot_date}")
        return Output(0, metadata={"skipped": True})

    context.log.info(
        f"Creating strategy snapshot for {snapshot_date} (up to block {snapshot_block})"
    )

    # Get active operators
    operators = get_operators_active_by_block(db, snapshot_block)
    context.log.info(f"Found {len(operators)} operators to snapshot")

    # Use reconstructor
    reconstructor = OperatorStrategySnapshotReconstructor(db, context.log)

    total_processed = process_operators_for_snapshot(
        context,
        db,
        config,
        operators,
        reconstructor,
        snapshot_date,
        snapshot_block,
        "Strategy State",
    )

    return Output(
        total_processed,
        metadata={
            "snapshot_date": str(snapshot_date),
            "snapshot_block": snapshot_block,
            "operators_snapshotted": total_processed,
        },
    )


# ============================================================================
# EXISTING SNAPSHOTS (unchanged)
# ============================================================================


@asset(
    partitions_def=daily_partitions,
    description="Daily snapshots of operator-AVS relationships (from events)",
    compute_kind="sql",
)
def operator_avs_relationship_snapshots_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """Create daily snapshot of operator-AVS relationships"""

    partition_date_str = context.partition_key
    snapshot_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    snapshot_block = get_snapshot_block_for_date(
        db,
        snapshot_date,
    )

    if snapshot_block == 0:
        context.log.warning(f"No events found for or before {snapshot_date}")
        return Output(0, metadata={"skipped": True})

    context.log.info(
        f"Creating AVS relationship snapshot for {snapshot_date} (up to block {snapshot_block})"
    )

    operators = get_operators_active_by_block(db, snapshot_block)
    context.log.info(f"Found {len(operators)} operators to snapshot")

    reconstructor = AVSRelationshipSnapshotReconstructor(db, context.log)

    total_processed = process_operators_for_snapshot(
        context,
        db,
        config,
        operators,
        reconstructor,
        snapshot_date,
        snapshot_block,
        "AVS Relationships",
    )

    return Output(
        total_processed,
        metadata={
            "snapshot_date": str(snapshot_date),
            "snapshot_block": snapshot_block,
            "operators_snapshotted": total_processed,
        },
    )


@asset(
    partitions_def=daily_partitions,
    description="Daily snapshots of delegator shares (from events) - NOW WITH is_delegated",
    compute_kind="sql",
)
def operator_delegator_shares_snapshots_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """Create daily snapshot of delegator shares - NOW INCLUDES is_delegated"""

    partition_date_str = context.partition_key
    snapshot_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    snapshot_block = get_snapshot_block_for_date(
        db,
        snapshot_date,
    )

    if snapshot_block == 0:
        context.log.warning(f"No events found for or before {snapshot_date}")
        return Output(0, metadata={"skipped": True})

    context.log.info(
        f"Creating delegator shares snapshot for {snapshot_date} (up to block {snapshot_block})"
    )

    operators = get_operators_active_by_block(db, snapshot_block)
    context.log.info(f"Found {len(operators)} operators to snapshot")

    reconstructor = DelegatorSharesSnapshotReconstructor(db, context.log)

    total_processed = process_operators_for_snapshot(
        context,
        db,
        config,
        operators,
        reconstructor,
        snapshot_date,
        snapshot_block,
        "Delegator Shares",
    )

    return Output(
        total_processed,
        metadata={
            "snapshot_date": str(snapshot_date),
            "snapshot_block": snapshot_block,
            "operators_snapshotted": total_processed,
        },
    )


@asset(
    partitions_def=daily_partitions,
    description="Daily snapshots of commission rates (from events)",
    compute_kind="sql",
)
def operator_commission_rates_snapshots_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """Create daily snapshot of commission rates"""

    partition_date_str = context.partition_key
    snapshot_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    snapshot_block = get_snapshot_block_for_date(
        db,
        snapshot_date,
    )

    if snapshot_block == 0:
        context.log.warning(f"No events found for or before {snapshot_date}")
        return Output(0, metadata={"skipped": True})

    context.log.info(
        f"Creating commission rates snapshot for {snapshot_date} (up to block {snapshot_block})"
    )

    operators = get_operators_active_by_block(db, snapshot_block)
    context.log.info(f"Found {len(operators)} operators to snapshot")

    reconstructor = CommissionRatesSnapshotReconstructor(db, context.log)

    total_processed = process_operators_for_snapshot(
        context,
        db,
        config,
        operators,
        reconstructor,
        snapshot_date,
        snapshot_block,
        "Commission Rates",
    )

    return Output(
        total_processed,
        metadata={
            "snapshot_date": str(snapshot_date),
            "snapshot_block": snapshot_block,
            "operators_snapshotted": total_processed,
        },
    )


# ============================================================================
# NEW SNAPSHOTS
# ============================================================================


@asset(
    partitions_def=daily_partitions,
    description="Daily snapshots of operator allocations (from events)",
    compute_kind="sql",
)
def operator_allocation_snapshots_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """Create daily snapshot of operator allocations"""

    partition_date_str = context.partition_key
    snapshot_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    snapshot_block = get_snapshot_block_for_date(
        db,
        snapshot_date,
    )

    if snapshot_block == 0:
        context.log.warning(f"No events found for or before {snapshot_date}")
        return Output(0, metadata={"skipped": True})

    context.log.info(
        f"Creating allocation snapshot for {snapshot_date} (up to block {snapshot_block})"
    )

    operators = get_operators_active_by_block(db, snapshot_block)
    context.log.info(f"Found {len(operators)} operators to snapshot")

    reconstructor = AllocationSnapshotReconstructor(db, context.log)

    total_processed = process_operators_for_snapshot(
        context,
        db,
        config,
        operators,
        reconstructor,
        snapshot_date,
        snapshot_block,
        "Allocations",
    )

    return Output(
        total_processed,
        metadata={
            "snapshot_date": str(snapshot_date),
            "snapshot_block": snapshot_block,
            "operators_snapshotted": total_processed,
        },
    )


@asset(
    partitions_def=daily_partitions,
    description="Daily network-wide aggregate statistics for percentile calculations",
    compute_kind="sql",
)
def network_daily_aggregates_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """
    Calculate network-wide statistics from operator snapshots.
    This should run AFTER all operator snapshots are complete.
    """

    partition_date_str = context.partition_key
    snapshot_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    context.log.info(f"Calculating network aggregates for {snapshot_date}")

    # First check if there are any snapshots for this date
    check_query = """
    SELECT COUNT(*) FROM operator_daily_snapshots 
    WHERE snapshot_date = :snapshot_date
    """

    result = db.execute_query(
        check_query, {"snapshot_date": snapshot_date}, db="analytics"
    )

    snapshot_count = result[0][0] if result else 0

    if snapshot_count == 0:
        context.log.warning(
            f"No operator snapshots found for {snapshot_date}. Skipping network aggregates."
        )
        return Output(
            0,
            metadata={
                "snapshot_date": str(snapshot_date),
                "skipped": True,
                "reason": "no_operator_snapshots",
            },
        )

    # Query to calculate all network statistics in one go
    query = """
    WITH operator_tvs AS (
        SELECT 
            oss.operator_id,
            SUM(oss.max_magnitude) as total_tvs
        FROM operator_strategy_daily_snapshots oss
        WHERE oss.snapshot_date = :snapshot_date
        GROUP BY oss.operator_id
    ),
    snapshot_block_cte AS (
        SELECT COALESCE(MAX(snapshot_block), 0) as snapshot_block
        FROM operator_daily_snapshots 
        WHERE snapshot_date = :snapshot_date
    ),
    network_stats AS (
        SELECT
            COUNT(DISTINCT ods.operator_id) as total_operators,
            COUNT(DISTINCT ods.operator_id) FILTER (WHERE ods.is_active = TRUE) as active_operators,
            
            -- TVS stats
            SUM(ot.total_tvs) as total_tvs,
            AVG(ot.total_tvs) as mean_tvs,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ot.total_tvs) as median_tvs,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ot.total_tvs) as p25_tvs,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ot.total_tvs) as p75_tvs,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ot.total_tvs) as p90_tvs,
            
            -- Delegator stats
            SUM(ods.delegator_count) as total_delegators,
            AVG(ods.delegator_count) as mean_delegators_per_operator,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ods.delegator_count) as median_delegators_per_operator,
            
            -- AVS stats
            AVG(ods.active_avs_count) as mean_avs_per_operator,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ods.active_avs_count) as median_avs_per_operator,
            
            -- Commission stats
            AVG(ods.pi_split_bips) as mean_pi_commission_bips,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ods.pi_split_bips) as median_pi_commission_bips
            
        FROM operator_daily_snapshots ods
        LEFT JOIN operator_tvs ot ON ods.operator_id = ot.operator_id
        WHERE ods.snapshot_date = :snapshot_date
    )
    INSERT INTO network_daily_aggregates (
        snapshot_date, snapshot_block,
        total_operators, active_operators,
        total_tvs, mean_tvs, median_tvs, p25_tvs, p75_tvs, p90_tvs,
        total_delegators, mean_delegators_per_operator, median_delegators_per_operator,
        mean_avs_per_operator, median_avs_per_operator,
        mean_pi_commission_bips, median_pi_commission_bips
    )
    SELECT
        :snapshot_date,
        sb.snapshot_block,
        total_operators, active_operators,
        COALESCE(total_tvs, 0), COALESCE(mean_tvs, 0), COALESCE(median_tvs, 0),
        COALESCE(p25_tvs, 0), COALESCE(p75_tvs, 0), COALESCE(p90_tvs, 0),
        COALESCE(total_delegators, 0), COALESCE(mean_delegators_per_operator, 0),
        COALESCE(median_delegators_per_operator, 0),
        COALESCE(mean_avs_per_operator, 0), COALESCE(median_avs_per_operator, 0),
        COALESCE(mean_pi_commission_bips, 0), COALESCE(median_pi_commission_bips, 0)
    FROM network_stats
    CROSS JOIN snapshot_block_cte sb
    ON CONFLICT (snapshot_date) DO UPDATE SET
        snapshot_block = EXCLUDED.snapshot_block,
        total_operators = EXCLUDED.total_operators,
        active_operators = EXCLUDED.active_operators,
        total_tvs = EXCLUDED.total_tvs,
        mean_tvs = EXCLUDED.mean_tvs,
        median_tvs = EXCLUDED.median_tvs,
        p25_tvs = EXCLUDED.p25_tvs,
        p75_tvs = EXCLUDED.p75_tvs,
        p90_tvs = EXCLUDED.p90_tvs,
        total_delegators = EXCLUDED.total_delegators,
        mean_delegators_per_operator = EXCLUDED.mean_delegators_per_operator,
        median_delegators_per_operator = EXCLUDED.median_delegators_per_operator,
        mean_avs_per_operator = EXCLUDED.mean_avs_per_operator,
        median_avs_per_operator = EXCLUDED.median_avs_per_operator,
        mean_pi_commission_bips = EXCLUDED.mean_pi_commission_bips,
        median_pi_commission_bips = EXCLUDED.median_pi_commission_bips
    """

    rowcount = db.execute_update(
        query,
        {"snapshot_date": snapshot_date},
        db="analytics",
    )

    context.log.info(f"Network aggregates calculated for {snapshot_date}")

    return Output(
        rowcount,
        metadata={
            "snapshot_date": str(snapshot_date),
            "aggregated": True,
            "operator_snapshots_count": snapshot_count,
        },
    )
