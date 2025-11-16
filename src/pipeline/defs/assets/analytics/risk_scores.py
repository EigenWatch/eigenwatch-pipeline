# defs/assets/analytics/risk_scores.py
"""
Calculate final operator risk scores by combining performance, economic, and network scores.
"""

from dagster import asset, OpExecutionContext, Output, DailyPartitionsDefinition
from datetime import datetime
import pandas as pd
from ...resources import DatabaseResource


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    partitions_def=daily_partitions,
    description="Calculate final risk scores from component scores",
    compute_kind="python",
)
def operator_analytics_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    performance_scores_asset: pd.DataFrame,
    economic_scores_asset: pd.DataFrame,
    network_scores_asset: pd.DataFrame,
) -> Output[int]:
    """
    Final Risk Score = 0.50 * performance + 0.35 * economic + 0.15 * network

    Stores complete analytics in operator_analytics table including:
    - Risk score and level
    - Confidence score
    - Component scores
    - Key metrics (HHI, volatility, etc.)
    - Snapshot values
    """

    partition_date_str = context.partition_key
    analysis_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    context.log.info(f"Calculating final risk scores for {analysis_date}")

    # Merge all component scores
    df = performance_scores_asset.merge(
        economic_scores_asset, on="operator_id", how="outer"
    ).merge(network_scores_asset, on="operator_id", how="outer")

    # Fill missing scores with neutral value 50
    score_columns = [
        "performance_score",
        "economic_score",
        "network_score",
        "slashing_score",
        "stability_score",
        "tenure_score",
        "concentration_score",
        "commission_score",
        "growth_score",
        "size_percentile",
        "distribution_quality",
    ]

    for col in score_columns:
        if col in df.columns:
            df[col] = df[col].fillna(50).astype(float)

    # Calculate final risk score
    df["risk_score"] = (
        df["performance_score"] * 0.50
        + df["economic_score"] * 0.35
        + df["network_score"] * 0.15
    ).round(2)

    # Categorize risk level
    def categorize_risk(score):
        if score >= 80:
            return "LOW"
        elif score >= 60:
            return "MEDIUM"
        elif score >= 40:
            return "HIGH"
        else:
            return "CRITICAL"

    df["risk_level"] = df["risk_score"].apply(categorize_risk)

    # Fetch additional metrics and snapshot data
    metrics_query = """
    WITH operator_metrics AS (
        SELECT 
            ods.operator_id,
            ods.delegator_count,
            ods.active_avs_count,
            ods.operational_days,
            ods.slash_event_count_to_date,
            ods.pi_split_bips,
            os.last_activity_at,
            os.first_activity_at
        FROM operator_daily_snapshots ods
        LEFT JOIN operator_state os ON ods.operator_id = os.operator_id
        WHERE ods.snapshot_date = :analysis_date
    ),
    total_shares AS (
        SELECT 
            operator_id,
            SUM(shares) as total_delegated_shares
        FROM operator_delegator_shares_snapshots
        WHERE snapshot_date = :analysis_date
          AND is_delegated = TRUE
        GROUP BY operator_id
    ),
    volatility_30d AS (
        SELECT 
            operator_id,
            volatility_30d as delegation_volatility_30d
        FROM volatility_metrics
        WHERE date = :analysis_date
          AND metric_type = 'delegation_shares'
    ),
    concentration AS (
        SELECT 
            operator_id,
            hhi_value as delegation_hhi,
            coefficient_of_variation as delegator_distribution_cv
        FROM concentration_metrics
        WHERE date = :analysis_date
          AND concentration_type = 'delegator'
    ),
    growth_rate AS (
        SELECT 
            vm.operator_id,
            vm.trend_direction as growth_rate_30d
        FROM volatility_metrics vm
        WHERE vm.date = :analysis_date
          AND vm.metric_type = 'tvs'
    ),
    slashing_totals AS (
        SELECT 
            osa.operator_id,
            SUM(osa.wad_slashed) as lifetime_slashing_amount
        FROM operator_slashing_amounts osa
        WHERE osa.operator_id IN (SELECT operator_id FROM operator_metrics)
        GROUP BY osa.operator_id
    )
    SELECT 
        om.operator_id,
        COALESCE(om.delegator_count, 0) as delegator_count,
        COALESCE(om.active_avs_count, 0) as avs_count,
        COALESCE(om.operational_days, 0) as operational_days,
        COALESCE(om.slash_event_count_to_date, 0) as slashing_event_count,
        om.pi_split_bips as commission_bips,
        COALESCE(ts.total_delegated_shares, 0) as total_delegated_shares,
        COALESCE(v30.delegation_volatility_30d, 0) as delegation_volatility_30d,
        COALESCE(c.delegation_hhi, 0) as delegation_hhi,
        COALESCE(c.delegator_distribution_cv, 0) as delegator_distribution_cv,
        COALESCE(gr.growth_rate_30d, 0) as growth_rate_30d,
        COALESCE(st.lifetime_slashing_amount, 0) as lifetime_slashing_amount,
        om.last_activity_at,
        om.first_activity_at
    FROM operator_metrics om
    LEFT JOIN total_shares ts ON om.operator_id = ts.operator_id
    LEFT JOIN volatility_30d v30 ON om.operator_id = v30.operator_id
    LEFT JOIN concentration c ON om.operator_id = c.operator_id
    LEFT JOIN growth_rate gr ON om.operator_id = gr.operator_id
    LEFT JOIN slashing_totals st ON om.operator_id = st.operator_id
    """

    metrics_result = db.execute_query(
        metrics_query, {"analysis_date": analysis_date}, db="analytics"
    )

    if not metrics_result:
        context.log.error(f"Failed to fetch operator metrics for {analysis_date}")
        return Output(0, metadata={"error": "no_metrics"})

    df_metrics = pd.DataFrame(
        metrics_result,
        columns=[
            "operator_id",
            "delegator_count",
            "avs_count",
            "operational_days",
            "slashing_event_count",
            "commission_bips",
            "total_delegated_shares",
            "delegation_volatility_30d",
            "delegation_hhi",
            "delegator_distribution_cv",
            "growth_rate_30d",
            "lifetime_slashing_amount",
            "last_activity_at",
            "first_activity_at",
        ],
    )

    # Ensure last_activity_at and first_activity_at are tz-aware UTC
    for col in ["last_activity_at", "first_activity_at"]:
        if col in df_metrics.columns:
            df_metrics[col] = pd.to_datetime(df_metrics[col], utc=True)

    # Merge metrics into main dataframe
    df = df.merge(df_metrics, on="operator_id", how="left")

    # Calculate confidence score
    def calculate_confidence_score(row):
        factors = []

        # Tenure factor (0-50 points)
        operational_days = row.get("operational_days", 0)
        if operational_days >= 30:
            factors.append(50)
        else:
            factors.append((operational_days / 30) * 50)

        # Data sufficiency factor (0-30 points)
        delegator_count = row.get("delegator_count", 0)
        if delegator_count >= 10:
            factors.append(30)
        elif delegator_count >= 2:
            factors.append(20)
        else:
            factors.append(10)

        # Activity recency factor (0-20 points)
        last_activity = row.get("last_activity_at")
        if pd.notna(last_activity):
            last_activity_ts = pd.Timestamp(last_activity)
            if last_activity_ts.tzinfo is None:
                last_activity_ts = last_activity_ts.tz_localize("UTC")
            days_since = (pd.Timestamp.now(tz="UTC") - last_activity_ts).days
            if days_since <= 7:
                factors.append(20)
            elif days_since <= 30:
                factors.append(10)
            else:
                factors.append(5)
        else:
            factors.append(5)

        return round(sum(factors), 2)

    df["confidence_score"] = df.apply(calculate_confidence_score, axis=1)

    # Calculate activity status
    def is_operator_active(row):
        last_activity = row.get("last_activity_at")
        if pd.isna(last_activity):
            return False
        last_activity_ts = pd.Timestamp(last_activity)
        if last_activity_ts.tzinfo is None:
            last_activity_ts = last_activity_ts.tz_localize("UTC")
        days_since = (pd.Timestamp.now(tz="UTC") - last_activity_ts).days
        return days_since <= 30

    df["is_active"] = df.apply(is_operator_active, axis=1)

    # Determine if operator has sufficient data
    df["has_sufficient_data"] = (df["operational_days"] >= 30) & (
        df["delegator_count"] >= 2
    )

    # Add metadata
    df["date"] = analysis_date
    df["calculated_at"] = pd.Timestamp.now(tz="UTC")

    # Prepare for database insertion
    insert_columns = [
        "operator_id",
        "date",
        "risk_score",
        "confidence_score",
        "risk_level",
        "performance_score",
        "economic_score",
        "network_position_score",
        "delegation_hhi",
        "delegation_volatility_30d",
        "growth_rate_30d",
        "size_percentile",
        "delegator_distribution_cv",
        "snapshot_total_delegated_shares",
        "snapshot_delegator_count",
        "snapshot_avs_count",
        "snapshot_commission_bips",
        "slashing_event_count",
        "lifetime_slashing_amount",
        "operational_days",
        "is_active",
        "has_sufficient_data",
        "calculated_at",
    ]

    # Rename columns to match database schema
    df_insert = df.rename(
        columns={
            "network_score": "network_position_score",
            "delegator_count": "snapshot_delegator_count",
            "avs_count": "snapshot_avs_count",
            "total_delegated_shares": "snapshot_total_delegated_shares",
            "commission_bips": "snapshot_commission_bips",
        }
    )

    # Ensure all required columns exist
    for col in insert_columns:
        if col not in df_insert.columns:
            df_insert[col] = None

    df_insert = df_insert[insert_columns]

    # Insert into database
    insert_query = """
    INSERT INTO operator_analytics (
        operator_id, date,
        risk_score, confidence_score, risk_level,
        performance_score, economic_score, network_position_score,
        delegation_hhi, delegation_volatility_30d,
        growth_rate_30d, size_percentile, delegator_distribution_cv,
        snapshot_total_delegated_shares, snapshot_delegator_count,
        snapshot_avs_count, snapshot_commission_bips,
        slashing_event_count, lifetime_slashing_amount,
        operational_days, is_active, has_sufficient_data,
        calculated_at
    )
    VALUES (
        :operator_id, :date,
        :risk_score, :confidence_score, :risk_level,
        :performance_score, :economic_score, :network_position_score,
        :delegation_hhi, :delegation_volatility_30d,
        :growth_rate_30d, :size_percentile, :delegator_distribution_cv,
        :snapshot_total_delegated_shares, :snapshot_delegator_count,
        :snapshot_avs_count, :snapshot_commission_bips,
        :slashing_event_count, :lifetime_slashing_amount,
        :operational_days, :is_active, :has_sufficient_data,
        :calculated_at
    )
    ON CONFLICT (operator_id, date) DO UPDATE SET
        risk_score = EXCLUDED.risk_score,
        confidence_score = EXCLUDED.confidence_score,
        risk_level = EXCLUDED.risk_level,
        performance_score = EXCLUDED.performance_score,
        economic_score = EXCLUDED.economic_score,
        network_position_score = EXCLUDED.network_position_score,
        delegation_hhi = EXCLUDED.delegation_hhi,
        delegation_volatility_30d = EXCLUDED.delegation_volatility_30d,
        growth_rate_30d = EXCLUDED.growth_rate_30d,
        size_percentile = EXCLUDED.size_percentile,
        delegator_distribution_cv = EXCLUDED.delegator_distribution_cv,
        snapshot_total_delegated_shares = EXCLUDED.snapshot_total_delegated_shares,
        snapshot_delegator_count = EXCLUDED.snapshot_delegator_count,
        snapshot_avs_count = EXCLUDED.snapshot_avs_count,
        snapshot_commission_bips = EXCLUDED.snapshot_commission_bips,
        slashing_event_count = EXCLUDED.slashing_event_count,
        lifetime_slashing_amount = EXCLUDED.lifetime_slashing_amount,
        operational_days = EXCLUDED.operational_days,
        is_active = EXCLUDED.is_active,
        has_sufficient_data = EXCLUDED.has_sufficient_data,
        calculated_at = EXCLUDED.calculated_at
    """

    inserted_count = 0
    for _, row in df_insert.iterrows():
        try:
            db.execute_update(insert_query, row.to_dict(), db="analytics")
            inserted_count += 1
        except Exception as exc:
            context.log.error(
                f"Failed to insert analytics for {row['operator_id']}: {exc}"
            )
            continue

    # Log summary statistics
    risk_distribution = df["risk_level"].value_counts().to_dict()

    context.log.info(f"✅ Risk scores calculated for {len(df)} operators")
    context.log.info(f"Risk distribution: {risk_distribution}")
    context.log.info(f"Average risk score: {df['risk_score'].mean():.2f}")
    context.log.info(f"Average confidence: {df['confidence_score'].mean():.2f}")
    context.log.info(
        f"Operators with sufficient data: {df['has_sufficient_data'].sum()}"
    )
    context.log.info(f"Successfully inserted: {inserted_count} records")

    return Output(
        inserted_count,
        metadata={
            "analysis_date": str(analysis_date),
            "operators_processed": len(df),
            "records_inserted": inserted_count,
            "avg_risk_score": float(df["risk_score"].mean()),
            "avg_confidence": float(df["confidence_score"].mean()),
            "risk_distribution": risk_distribution,
        },
    )
