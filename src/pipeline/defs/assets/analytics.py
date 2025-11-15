# analytics_pipeline/assets/analytics.py
"""
Analytics Assets - Risk scoring and metrics calculation
"""

from dagster import (
    asset,
    OpExecutionContext,
    Output,
    AssetIn,
    DailyPartitionsDefinition,
)
from datetime import datetime, timedelta
from ..resources import DatabaseResource, ConfigResource

daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    ins={"snapshots": AssetIn("operator_daily_snapshots_asset")},
    partitions_def=daily_partitions,
    description="Calculates volatility metrics from historical snapshots",
    compute_kind="python",
)
def volatility_metrics_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    snapshots: int,
) -> Output[int]:
    """
    Calculate volatility metrics using historical snapshot data.
    Computes standard deviation, coefficient of variation, etc.
    """
    if snapshots == 0:
        return Output(0, metadata={"skipped": True})

    partition_date_str = context.partition_key
    calc_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    # Calculate volatility for each window (7, 30, 90 days)
    for window_days in config.volatility_windows:
        start_date = calc_date - timedelta(days=window_days)

        query = f"""
            WITH historical_data AS (
                SELECT
                    operator_id,
                    snapshot_date,
                    delegator_count
                FROM operator_daily_snapshots
                WHERE snapshot_date BETWEEN :start_date AND :end_date
            ),
            volatility_calcs AS (
                SELECT
                    operator_id,
                    COUNT(*) as data_points,
                    AVG(delegator_count) as mean_value,
                    STDDEV(delegator_count) as std_dev,
                    CASE 
                        WHEN AVG(delegator_count) > 0 
                        THEN STDDEV(delegator_count) / AVG(delegator_count)
                        ELSE 0 
                    END as coefficient_of_variation
                FROM historical_data
                GROUP BY operator_id
                HAVING COUNT(*) >= :min_data_points
            )
            INSERT INTO volatility_metrics (
                entity_type, entity_id, date, metric_type,
                volatility_{window_days}d, mean_value, coefficient_of_variation,
                data_points_count, confidence_score
            )
            SELECT 
                'operator' as entity_type,
                operator_id as entity_id,
                :calc_date as date,
                'delegator_count' as metric_type,
                std_dev as volatility_{window_days}d,
                mean_value,
                coefficient_of_variation,
                data_points,
                CASE 
                    WHEN data_points >= {window_days} THEN 1.0
                    ELSE data_points::NUMERIC / {window_days}
                END as confidence_score
            FROM volatility_calcs
            ON CONFLICT (entity_type, entity_id, date, metric_type) 
            DO UPDATE SET
                volatility_{window_days}d = EXCLUDED.volatility_{window_days}d,
                mean_value = EXCLUDED.mean_value,
                coefficient_of_variation = EXCLUDED.coefficient_of_variation,
                data_points_count = EXCLUDED.data_points_count,
                confidence_score = EXCLUDED.confidence_score
        """

        db.execute_update(
            query,
            {
                "start_date": start_date,
                "end_date": calc_date,
                "calc_date": calc_date,
                "min_data_points": config.min_data_points_for_analytics,
            },
            db="analytics",
        )

    return Output(1, metadata={"calculation_date": str(calc_date)})


@asset(
    ins={"snapshots": AssetIn("operator_daily_snapshots_asset")},
    partitions_def=daily_partitions,
    description="Calculates concentration metrics (HHI, Gini, etc.)",
    compute_kind="python",
)
def concentration_metrics_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    snapshots: int,
) -> Output[int]:
    """
    Calculate concentration metrics for delegator distribution.
    """
    if snapshots == 0:
        return Output(0, metadata={"skipped": True})

    partition_date_str = context.partition_key
    calc_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    # Calculate HHI and concentration metrics per operator
    query = """
        WITH delegator_shares AS (
            SELECT
                operator_id,
                staker_id,
                SUM(shares) as total_shares
            FROM operator_delegator_shares
            GROUP BY operator_id, staker_id
        ),
        operator_totals AS (
            SELECT
                operator_id,
                SUM(total_shares) as total_operator_shares,
                COUNT(*) as total_delegators
            FROM delegator_shares
            GROUP BY operator_id
        ),
        share_proportions AS (
            SELECT
                ds.operator_id,
                ds.staker_id,
                ds.total_shares,
                ds.total_shares::NUMERIC / ot.total_operator_shares as share_proportion,
                ot.total_delegators
            FROM delegator_shares ds
            JOIN operator_totals ot ON ds.operator_id = ot.operator_id
            WHERE ot.total_operator_shares > 0
        ),
        hhi_calcs AS (
            SELECT
                operator_id,
                SUM(POW(share_proportion, 2)) as hhi_value,
                MAX(total_delegators) as total_entities
            FROM share_proportions
            GROUP BY operator_id
        )
        INSERT INTO concentration_metrics (
            entity_type, entity_id, date, concentration_type,
            hhi_value, total_entities
        )
        SELECT 
            'operator' as entity_type,
            operator_id as entity_id,
            :calc_date as date,
            'delegator_distribution' as concentration_type,
            hhi_value,
            total_entities
        FROM hhi_calcs
        ON CONFLICT (entity_type, entity_id, date, concentration_type)
        DO UPDATE SET
            hhi_value = EXCLUDED.hhi_value,
            total_entities = EXCLUDED.total_entities
    """

    rowcount = db.execute_update(query, {"calc_date": calc_date}, db="analytics")

    return Output(rowcount, metadata={"calculation_date": str(calc_date)})


@asset(
    ins={
        "volatility": AssetIn("volatility_metrics_asset"),
        "concentration": AssetIn("concentration_metrics_asset"),
    },
    partitions_def=daily_partitions,
    description="Calculates comprehensive risk scores for operators",
    compute_kind="python",
)
def operator_analytics_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    volatility: int,
    concentration: int,
) -> Output[int]:
    """
    Calculate final risk scores combining all metrics.
    """
    partition_date_str = context.partition_key
    calc_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    # Combine volatility, concentration, and operational metrics into risk score
    query = """
        WITH operator_metrics AS (
            SELECT
                ocs.operator_id,
                ocs.delegator_count as snapshot_delegator_count,
                ocs.active_avs_count as snapshot_avs_count,
                ocs.slash_event_count_to_date as slashing_event_count,
                ocs.operational_days,
                vm.volatility_30d as delegation_volatility_30d,
                vm.coefficient_of_variation as delegation_volatility_cv,
                cm.hhi_value as delegation_hhi,
                cm.total_entities as total_delegators
            FROM operator_daily_snapshots ocs
            LEFT JOIN volatility_metrics vm 
                ON vm.entity_id = ocs.operator_id 
                AND vm.date = :calc_date
                AND vm.metric_type = 'delegator_count'
            LEFT JOIN concentration_metrics cm
                ON cm.entity_id = ocs.operator_id
                AND cm.date = :calc_date
                AND cm.concentration_type = 'delegator_distribution'
            WHERE ocs.snapshot_date = :calc_date
        ),
        risk_calculations AS (
            SELECT
                operator_id,
                -- Simple risk scoring logic (can be made more sophisticated)
                CASE
                    WHEN slashing_event_count > 0 THEN 80 + (slashing_event_count * 5)
                    WHEN delegation_hhi > 0.5 THEN 60 + (delegation_hhi * 30)
                    WHEN delegation_volatility_30d > 0.3 THEN 50 + (delegation_volatility_30d * 50)
                    WHEN total_delegators < 5 THEN 40
                    ELSE 20
                END as risk_score,
                CASE
                    WHEN operational_days < 30 THEN 0.5
                    WHEN operational_days < 90 THEN 0.7
                    ELSE 1.0
                END as confidence_score,
                snapshot_delegator_count,
                snapshot_avs_count,
                slashing_event_count,
                operational_days,
                delegation_hhi,
                delegation_volatility_30d
            FROM operator_metrics
        )
        INSERT INTO operator_analytics (
            operator_id, date, risk_score, confidence_score, risk_level,
            delegation_hhi, delegation_volatility_30d,
            snapshot_delegator_count, snapshot_avs_count,
            slashing_event_count, operational_days,
            is_active, has_sufficient_data, calculated_at
        )
        SELECT 
            operator_id,
            :calc_date as date,
            LEAST(risk_score, 100) as risk_score,
            confidence_score,
            CASE
                WHEN risk_score >= 70 THEN 'HIGH'
                WHEN risk_score >= 40 THEN 'MEDIUM'
                ELSE 'LOW'
            END as risk_level,
            delegation_hhi,
            delegation_volatility_30d,
            snapshot_delegator_count,
            snapshot_avs_count,
            slashing_event_count,
            operational_days,
            TRUE as is_active,
            operational_days >= 7 as has_sufficient_data,
            NOW() as calculated_at
        FROM risk_calculations
        ON CONFLICT (operator_id, date) DO UPDATE SET
            risk_score = EXCLUDED.risk_score,
            confidence_score = EXCLUDED.confidence_score,
            risk_level = EXCLUDED.risk_level,
            delegation_hhi = EXCLUDED.delegation_hhi,
            delegation_volatility_30d = EXCLUDED.delegation_volatility_30d,
            snapshot_delegator_count = EXCLUDED.snapshot_delegator_count,
            snapshot_avs_count = EXCLUDED.snapshot_avs_count,
            slashing_event_count = EXCLUDED.slashing_event_count,
            operational_days = EXCLUDED.operational_days,
            has_sufficient_data = EXCLUDED.has_sufficient_data,
            calculated_at = EXCLUDED.calculated_at
    """

    rowcount = db.execute_update(query, {"calc_date": calc_date}, db="analytics")

    context.log.info(f"Calculated risk scores for {rowcount} operators on {calc_date}")

    return Output(
        rowcount,
        metadata={
            "calculation_date": str(calc_date),
            "operators_analyzed": rowcount,
        },
    )
