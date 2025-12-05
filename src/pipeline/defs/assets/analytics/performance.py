# defs/assets/analytics/performance.py
"""
Calculate performance component scores (slashing, stability, tenure).
"""

from dagster import asset, OpExecutionContext, Output, DailyPartitionsDefinition
from datetime import datetime
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
    description="Calculate performance scores: slashing, stability, tenure",
    compute_kind="python",
)
def performance_scores_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    volatility_metrics_asset: int,  # Dependency ensures volatility is calculated first
) -> pd.DataFrame:
    """
    Performance Score = 0.6 * slashing_score + 0.3 * stability_score + 0.1 * tenure_score

    Returns DataFrame with columns:
    - operator_id
    - performance_score
    - slashing_score
    - stability_score
    - tenure_score
    """

    partition_date_str = context.partition_key
    analysis_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    context.log.info(f"Calculating performance scores for {analysis_date}")

    # Query to get all required data in one go
    query = """
    WITH operator_data AS (
        SELECT 
            ods.operator_id,
            ods.slash_event_count_to_date,
            ods.operational_days,
            os.first_activity_at
        FROM operator_daily_snapshots ods
        LEFT JOIN operator_state os ON ods.operator_id = os.operator_id
        WHERE ods.snapshot_date = :analysis_date
    ),
    volatility_data AS (
        SELECT 
            operator_id,
            volatility_30d
        FROM volatility_metrics
        WHERE date = :analysis_date
          AND metric_type = 'delegation_shares'
    )
    SELECT 
        od.operator_id,
        COALESCE(od.slash_event_count_to_date, 0) as slash_count,
        COALESCE(od.operational_days, 0) as operational_days,
        COALESCE(vd.volatility_30d, 0.1) as volatility_30d
    FROM operator_data od
    LEFT JOIN volatility_data vd ON od.operator_id = vd.operator_id
    """

    result = db.execute_query(query, {"analysis_date": analysis_date}, db="analytics")

    if not result:
        context.log.warning(f"No operators found for {analysis_date}")
        return pd.DataFrame(
            columns=[
                "operator_id",
                "performance_score",
                "slashing_score",
                "stability_score",
                "tenure_score",
            ]
        )

    df = pd.DataFrame(
        result,
        columns=["operator_id", "slash_count", "operational_days", "volatility_30d"],
    )

    # Convert Decimals (from SQL NUMERIC) to float
    df = normalize_decimals(df)

    # Calculate component scores

    # 1. Slashing Score: 100 points minus 25 per slashing event
    df["slashing_score"] = df["slash_count"].apply(lambda x: max(0, 100 - (x * 25)))

    # 2. Stability Score: Inverse of volatility (reciprocal form for smooth decay)
    df["stability_score"] = 100 * (1 / (1 + df["volatility_30d"]))

    # 3. Tenure Score: Days/365 capped at 100
    df["tenure_score"] = df["operational_days"].apply(
        lambda days: min(100, (days / 365) * 100)
    )

    # 4. Final Performance Score (weighted average)
    df["performance_score"] = (
        df["slashing_score"] * 0.6
        + df["stability_score"] * 0.3
        + df["tenure_score"] * 0.1
    )

    # Round to 2 decimal places
    for col in [
        "performance_score",
        "slashing_score",
        "stability_score",
        "tenure_score",
    ]:
        df[col] = df[col].round(2)

    context.log.info(
        f"Performance scores calculated for {len(df)} operators. "
        f"Avg score: {df['performance_score'].mean():.2f}"
    )

    return df[
        [
            "operator_id",
            "performance_score",
            "slashing_score",
            "stability_score",
            "tenure_score",
        ]
    ]
