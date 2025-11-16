# defs/assets/analytics/economic.py
"""
Calculate economic component scores (concentration, commission, growth).
"""

from dagster import asset, OpExecutionContext, DailyPartitionsDefinition
from datetime import datetime, timedelta
import pandas as pd
from decimal import Decimal
from ...resources import DatabaseResource


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


def normalize_decimals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert any Decimal objects in the DataFrame to float.
    """
    return df.applymap(lambda x: float(x) if isinstance(x, Decimal) else x)


@asset(
    partitions_def=daily_partitions,
    description="Calculate economic scores: concentration, commission, growth",
    compute_kind="python",
)
def economic_scores_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    concentration_metrics_asset: int,  # Dependency
    volatility_metrics_asset: int,  # Dependency
) -> pd.DataFrame:
    """
    Economic Score = 0.5 * concentration_score + 0.3 * commission_score + 0.2 * growth_score

    Returns DataFrame with columns:
    - operator_id
    - economic_score
    - concentration_score
    - commission_score
    - growth_score
    """

    partition_date_str = context.partition_key
    analysis_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()
    date_30d_ago = analysis_date - timedelta(days=30)

    context.log.info(f"Calculating economic scores for {analysis_date}")

    # Query for concentration data
    concentration_query = """
    SELECT 
        operator_id,
        hhi_value
    FROM concentration_metrics
    WHERE date = :analysis_date
      AND concentration_type = 'delegator'
    """

    concentration_result = db.execute_query(
        concentration_query, {"analysis_date": analysis_date}, db="analytics"
    )

    if not concentration_result:
        context.log.warning(f"No concentration data for {analysis_date}")
        return pd.DataFrame(
            columns=[
                "operator_id",
                "economic_score",
                "concentration_score",
                "commission_score",
                "growth_score",
            ]
        )

    df_concentration = pd.DataFrame(
        concentration_result, columns=["operator_id", "hhi_value"]
    )

    # Convert Decimals to float
    df_concentration = normalize_decimals(df_concentration)

    # Query for commission changes
    commission_query = """
    WITH commission_changes AS (
        SELECT 
            operator_id,
            COUNT(*) as change_count
        FROM operator_commission_history
        WHERE changed_at BETWEEN :start_date AND :analysis_date
        GROUP BY operator_id
    )
    SELECT 
        ods.operator_id,
        COALESCE(cc.change_count, 0) as commission_changes
    FROM operator_daily_snapshots ods
    LEFT JOIN commission_changes cc ON ods.operator_id = cc.operator_id
    WHERE ods.snapshot_date = :analysis_date
    """

    commission_result = db.execute_query(
        commission_query,
        {
            "analysis_date": analysis_date,
            "start_date": analysis_date - timedelta(days=90),
        },
        db="analytics",
    )

    df_commission = (
        pd.DataFrame(commission_result, columns=["operator_id", "commission_changes"])
        if commission_result
        else pd.DataFrame(columns=["operator_id", "commission_changes"])
    )

    df_commission = normalize_decimals(df_commission)

    # Query for TVS growth (30-day)
    growth_query = """
    WITH current_tvs AS (
        SELECT 
            operator_id,
            SUM(max_magnitude) as tvs
        FROM operator_strategy_daily_snapshots
        WHERE snapshot_date = :current_date
        GROUP BY operator_id
    ),
    past_tvs AS (
        SELECT 
            operator_id,
            SUM(max_magnitude) as tvs
        FROM operator_strategy_daily_snapshots
        WHERE snapshot_date = :past_date
        GROUP BY operator_id
    )
    SELECT 
        c.operator_id,
        c.tvs as current_tvs,
        COALESCE(p.tvs, 0) as past_tvs
    FROM current_tvs c
    LEFT JOIN past_tvs p ON c.operator_id = p.operator_id
    """

    growth_result = db.execute_query(
        growth_query,
        {"current_date": analysis_date, "past_date": date_30d_ago},
        db="analytics",
    )

    df_growth = (
        pd.DataFrame(growth_result, columns=["operator_id", "current_tvs", "past_tvs"])
        if growth_result
        else pd.DataFrame(columns=["operator_id", "current_tvs", "past_tvs"])
    )

    df_growth = normalize_decimals(df_growth)

    # Merge all dataframes
    df = df_concentration.copy()

    if not df_commission.empty:
        df = df.merge(df_commission, on="operator_id", how="left")
    else:
        df["commission_changes"] = 0

    if not df_growth.empty:
        df = df.merge(df_growth, on="operator_id", how="left")
    else:
        df["current_tvs"] = 0
        df["past_tvs"] = 0

    df["commission_changes"] = df["commission_changes"].fillna(0)
    df["current_tvs"] = df["current_tvs"].fillna(0)
    df["past_tvs"] = df["past_tvs"].fillna(0)

    # Calculate component scores

    # 1. Concentration Score: Inverse of HHI
    df["concentration_score"] = df["hhi_value"].apply(
        lambda hhi: max(0, 100 - (hhi * 100))
    )

    # 2. Commission Score: Fewer changes = higher score
    df["commission_score"] = df["commission_changes"].apply(
        lambda changes: max(0, 100 - (changes * 10))
    )

    # 3. Growth Score
    def calculate_growth_score(row):
        if row["past_tvs"] == 0:
            return 70
        growth_rate = ((row["current_tvs"] - row["past_tvs"]) / row["past_tvs"]) * 100
        normalized = 50 + (growth_rate / 50 * 50)
        return max(0, min(100, normalized))

    df["growth_score"] = df.apply(calculate_growth_score, axis=1)

    # 4. Final Economic Score (weighted average)
    df["economic_score"] = (
        df["concentration_score"] * 0.5
        + df["commission_score"] * 0.3
        + df["growth_score"] * 0.2
    )

    # Round to 2 decimal places
    for col in [
        "economic_score",
        "concentration_score",
        "commission_score",
        "growth_score",
    ]:
        df[col] = df[col].round(2)

    context.log.info(
        f"Economic scores calculated for {len(df)} operators. "
        f"Avg score: {df['economic_score'].mean():.2f}"
    )

    return df[
        [
            "operator_id",
            "economic_score",
            "concentration_score",
            "commission_score",
            "growth_score",
        ]
    ]
