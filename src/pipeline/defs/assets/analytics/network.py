# defs/assets/analytics/network.py
"""
Calculate network position component scores (size percentile, distribution quality).
"""

from dagster import asset, OpExecutionContext, Output, DailyPartitionsDefinition
from datetime import datetime
import pandas as pd
import numpy as np
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
    description="Calculate network position scores: size percentile, distribution quality",
    compute_kind="python",
)
def network_scores_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    concentration_metrics_asset: int,  # Dependency
) -> pd.DataFrame:
    """
    Network Position Score = 0.7 * size_percentile + 0.3 * distribution_quality

    Returns DataFrame with columns:
    - operator_id
    - network_score
    - size_percentile
    - distribution_quality
    """

    partition_date_str = context.partition_key
    analysis_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    context.log.info(f"Calculating network position scores for {analysis_date}")

    # Query for operator TVS and network aggregates
    query = """
    WITH operator_tvs AS (
        SELECT 
            operator_id,
            SUM(max_magnitude) as total_tvs
        FROM operator_strategy_daily_snapshots
        WHERE snapshot_date = :analysis_date
        GROUP BY operator_id
    ),
    network_stats AS (
        SELECT 
            median_tvs,
            p90_tvs,
            total_tvs as network_total_tvs
        FROM network_daily_aggregates
        WHERE snapshot_date = :analysis_date
    ),
    concentration_scores AS (
        SELECT 
            operator_id,
            hhi_value
        FROM concentration_metrics
        WHERE date = :analysis_date
          AND concentration_type = 'delegator'
    )
    SELECT 
        ot.operator_id,
        COALESCE(ot.total_tvs, 0) as operator_tvs,
        ns.median_tvs as network_median,
        ns.p90_tvs as network_p90,
        ns.network_total_tvs,
        COALESCE(cs.hhi_value, 0.5) as hhi_value
    FROM operator_tvs ot
    CROSS JOIN network_stats ns
    LEFT JOIN concentration_scores cs ON ot.operator_id = cs.operator_id
    """

    result = db.execute_query(query, {"analysis_date": analysis_date}, db="analytics")

    if not result:
        context.log.warning(f"No operators found for {analysis_date}")
        return pd.DataFrame(
            columns=[
                "operator_id",
                "network_score",
                "size_percentile",
                "distribution_quality",
            ]
        )

    df = pd.DataFrame(
        result,
        columns=[
            "operator_id",
            "operator_tvs",
            "network_median",
            "network_p90",
            "network_total_tvs",
            "hhi_value",
        ],
    )

    # Convert Decimals to float
    df = normalize_decimals(df)

    # Calculate component scores

    # 1. Size Percentile: Position relative to network
    def calculate_size_percentile(row):
        tvs = row["operator_tvs"]
        median = row["network_median"]
        p90 = row["network_p90"]

        if p90 == 0 or median == 0:
            return 50  # Default middle percentile

        if tvs >= p90:
            # Top 10%: scale from 90-100
            excess = (tvs - p90) / p90 if p90 > 0 else 0
            return min(100, 90 + (excess * 10))
        elif tvs >= median:
            # 50th-90th percentile: linear scale
            return (
                50 + ((tvs - median) / (p90 - median) * 40)
                if (p90 - median) > 0
                else 50
            )
        else:
            # Below median: scale from 0-50
            return (tvs / median * 50) if median > 0 else 0

    df["size_percentile"] = df.apply(calculate_size_percentile, axis=1)

    # 2. Distribution Quality: Inverse of concentration (reuse HHI)
    df["distribution_quality"] = df["hhi_value"].apply(
        lambda hhi: max(0, 100 - (hhi * 100))
    )

    # 3. Final Network Position Score (weighted average)
    df["network_score"] = df["size_percentile"] * 0.7 + df["distribution_quality"] * 0.3

    # Round to 2 decimal places
    for col in ["network_score", "size_percentile", "distribution_quality"]:
        df[col] = df[col].round(2)

    context.log.info(
        f"Network position scores calculated for {len(df)} operators. "
        f"Avg score: {df['network_score'].mean():.2f}"
    )

    return df[
        ["operator_id", "network_score", "size_percentile", "distribution_quality"]
    ]
