"""
Calculate volatility metrics for operators across multiple time windows.
"""

from dagster import asset, OpExecutionContext, Output, DailyPartitionsDefinition
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import List, Dict
from ...resources import DatabaseResource, ConfigResource


def get_analysis_date(context: OpExecutionContext) -> datetime.date:
    """Get the analysis date from partition key or default to yesterday."""
    if context.has_partition_key:
        return datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    else:
        return (datetime.now() - timedelta(days=1)).date()


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


def sanitize_series(series: pd.Series) -> pd.Series:
    """
    Convert Decimal/objects to float safely.
    Ensures numpy operations (isnan, polyfit, std) never crash.
    """
    return pd.to_numeric(series, errors="coerce")


def calculate_coefficient_of_variation(values: pd.Series) -> float:
    """Calculate CV = std_dev / mean"""
    values = sanitize_series(values)

    if len(values) < 2 or values.mean() == 0:
        return 0.0
    return float(values.std() / values.mean())


def calculate_trend_metrics(values: pd.Series) -> Dict[str, float]:
    """Calculate linear regression trend direction and strength"""
    values = sanitize_series(values)

    if len(values) < 2:
        return {"trend_direction": 0.0, "trend_strength": 0.0}

    x = np.arange(len(values))

    # Handle NaN values
    mask = ~np.isnan(values.values)
    if mask.sum() < 2:
        return {"trend_direction": 0.0, "trend_strength": 0.0}

    x_clean = x[mask]
    y_clean = values.values[mask]

    # Linear regression
    coeffs = np.polyfit(x_clean, y_clean, 1)
    trend_direction = coeffs[0]  # Slope

    # R-squared for strength
    y_pred = np.polyval(coeffs, x_clean)
    ss_res = np.sum((y_clean - y_pred) ** 2)
    ss_tot = np.sum((y_clean - np.mean(y_clean)) ** 2)
    r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

    return {
        "trend_direction": float(trend_direction),
        "trend_strength": float(r_squared),
    }


def calculate_volatility_for_window(daily_values: pd.Series, window_days: int) -> float:
    """Calculate volatility (CV) for a specific time window"""
    daily_values = sanitize_series(daily_values)

    if len(daily_values) < 2:
        return 0.0

    recent_values = daily_values.tail(window_days)
    return calculate_coefficient_of_variation(recent_values)


@asset(
    partitions_def=daily_partitions,
    description="Calculate volatility metrics (7d/30d/90d) for all operators",
    compute_kind="python",
)
def volatility_metrics_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:
    """
    Calculate volatility metrics for multiple dimensions:
    - delegation_shares: Total delegated shares volatility
    - tvs: Total Value Secured volatility
    - delegator_count: Number of delegators volatility
    - avs_count: Number of AVS registrations volatility
    """

    analysis_date = get_analysis_date(context)

    context.log.info(f"Calculating volatility metrics for {analysis_date}")

    # Get all operators that have data by this date
    operators_query = """
    SELECT DISTINCT operator_id
    FROM operator_daily_snapshots
    WHERE snapshot_date <= :analysis_date
    """

    operators_result = db.execute_query(
        operators_query, {"analysis_date": analysis_date}, db="analytics"
    )

    if not operators_result:
        context.log.warning(f"No operators found for {analysis_date}")
        return Output(0, metadata={"skipped": True})

    operators = [row[0] for row in operators_result]
    context.log.info(f"Processing {len(operators)} operators")

    metrics_inserted = 0
    start_date_90d = analysis_date - timedelta(days=90)

    for idx, operator_id in enumerate(operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(f"Processing operator {idx}/{len(operators)}")

        try:
            # Fetch 90 days of snapshot data
            snapshots_query = """
            SELECT 
                snapshot_date,
                delegator_count,
                active_avs_count
            FROM operator_daily_snapshots
            WHERE operator_id = :operator_id
              AND snapshot_date BETWEEN :start_date AND :end_date
            ORDER BY snapshot_date
            """

            snapshots = db.execute_query(
                snapshots_query,
                {
                    "operator_id": operator_id,
                    "start_date": start_date_90d,
                    "end_date": analysis_date,
                },
                db="analytics",
            )

            if not snapshots or len(snapshots) < 2:
                continue

            df_snapshots = pd.DataFrame(
                snapshots, columns=["snapshot_date", "delegator_count", "avs_count"]
            )

            # Fetch delegation shares
            shares_query = """
            SELECT 
                snapshot_date,
                SUM(shares) as total_shares
            FROM operator_delegator_shares_snapshots
            WHERE operator_id = :operator_id
              AND snapshot_date BETWEEN :start_date AND :end_date
              AND is_delegated = TRUE
            GROUP BY snapshot_date
            ORDER BY snapshot_date
            """

            shares = db.execute_query(
                shares_query,
                {
                    "operator_id": operator_id,
                    "start_date": start_date_90d,
                    "end_date": analysis_date,
                },
                db="analytics",
            )

            df_shares = (
                pd.DataFrame(shares, columns=["snapshot_date", "total_shares"])
                if shares
                else pd.DataFrame()
            )

            # Fetch TVS data
            tvs_query = """
            SELECT 
                snapshot_date,
                SUM(max_magnitude) as total_tvs
            FROM operator_strategy_daily_snapshots
            WHERE operator_id = :operator_id
              AND snapshot_date BETWEEN :start_date AND :end_date
            GROUP BY snapshot_date
            ORDER BY snapshot_date
            """

            tvs = db.execute_query(
                tvs_query,
                {
                    "operator_id": operator_id,
                    "start_date": start_date_90d,
                    "end_date": analysis_date,
                },
                db="analytics",
            )

            df_tvs = (
                pd.DataFrame(tvs, columns=["snapshot_date", "total_tvs"])
                if tvs
                else pd.DataFrame()
            )

            # Calculate metrics for each dimension
            metrics_to_insert = []

            # 1. Delegation shares volatility
            if not df_shares.empty and len(df_shares) >= 2:
                shares_series = sanitize_series(
                    df_shares.set_index("snapshot_date")["total_shares"]
                )
                trend = calculate_trend_metrics(shares_series)

                metrics_to_insert.append(
                    {
                        "entity_type": "operator",
                        "entity_id": operator_id,
                        "date": analysis_date,
                        "metric_type": "delegation_shares",
                        "volatility_7d": calculate_volatility_for_window(
                            shares_series, 7
                        ),
                        "volatility_30d": calculate_volatility_for_window(
                            shares_series, 30
                        ),
                        "volatility_90d": calculate_volatility_for_window(
                            shares_series, 90
                        ),
                        "mean_value": float(shares_series.mean()),
                        "coefficient_of_variation": calculate_coefficient_of_variation(
                            shares_series
                        ),
                        "trend_direction": trend["trend_direction"],
                        "trend_strength": trend["trend_strength"],
                        "data_points_count": len(shares_series),
                        "confidence_score": min(100, (len(shares_series) / 90) * 100),
                        "operator_id": operator_id,
                    }
                )

            # 2. TVS volatility
            if not df_tvs.empty and len(df_tvs) >= 2:
                tvs_series = sanitize_series(
                    df_tvs.set_index("snapshot_date")["total_tvs"]
                )
                trend = calculate_trend_metrics(tvs_series)

                metrics_to_insert.append(
                    {
                        "entity_type": "operator",
                        "entity_id": operator_id,
                        "date": analysis_date,
                        "metric_type": "tvs",
                        "volatility_7d": calculate_volatility_for_window(tvs_series, 7),
                        "volatility_30d": calculate_volatility_for_window(
                            tvs_series, 30
                        ),
                        "volatility_90d": calculate_volatility_for_window(
                            tvs_series, 90
                        ),
                        "mean_value": float(tvs_series.mean()),
                        "coefficient_of_variation": calculate_coefficient_of_variation(
                            tvs_series
                        ),
                        "trend_direction": trend["trend_direction"],
                        "trend_strength": trend["trend_strength"],
                        "data_points_count": len(tvs_series),
                        "confidence_score": min(100, (len(tvs_series) / 90) * 100),
                        "operator_id": operator_id,
                    }
                )

            # 3. Delegator count volatility
            if len(df_snapshots) >= 2:
                delegator_series = sanitize_series(
                    df_snapshots.set_index("snapshot_date")["delegator_count"]
                )
                trend = calculate_trend_metrics(delegator_series)

                metrics_to_insert.append(
                    {
                        "entity_type": "operator",
                        "entity_id": operator_id,
                        "date": analysis_date,
                        "metric_type": "delegator_count",
                        "volatility_7d": calculate_volatility_for_window(
                            delegator_series, 7
                        ),
                        "volatility_30d": calculate_volatility_for_window(
                            delegator_series, 30
                        ),
                        "volatility_90d": calculate_volatility_for_window(
                            delegator_series, 90
                        ),
                        "mean_value": float(delegator_series.mean()),
                        "coefficient_of_variation": calculate_coefficient_of_variation(
                            delegator_series
                        ),
                        "trend_direction": trend["trend_direction"],
                        "trend_strength": trend["trend_strength"],
                        "data_points_count": len(delegator_series),
                        "confidence_score": min(
                            100, (len(delegator_series) / 90) * 100
                        ),
                        "operator_id": operator_id,
                    }
                )

            # 4. AVS count volatility
            if len(df_snapshots) >= 2:
                avs_series = sanitize_series(
                    df_snapshots.set_index("snapshot_date")["avs_count"]
                )
                trend = calculate_trend_metrics(avs_series)

                metrics_to_insert.append(
                    {
                        "entity_type": "operator",
                        "entity_id": operator_id,
                        "date": analysis_date,
                        "metric_type": "avs_count",
                        "volatility_7d": calculate_volatility_for_window(avs_series, 7),
                        "volatility_30d": calculate_volatility_for_window(
                            avs_series, 30
                        ),
                        "volatility_90d": calculate_volatility_for_window(
                            avs_series, 90
                        ),
                        "mean_value": float(avs_series.mean()),
                        "coefficient_of_variation": calculate_coefficient_of_variation(
                            avs_series
                        ),
                        "trend_direction": trend["trend_direction"],
                        "trend_strength": trend["trend_strength"],
                        "data_points_count": len(avs_series),
                        "confidence_score": min(100, (len(avs_series) / 90) * 100),
                        "operator_id": operator_id,
                    }
                )

            # Insert all metrics for this operator
            if metrics_to_insert:
                insert_query = """
                INSERT INTO volatility_metrics (
                    entity_type, entity_id, date, metric_type,
                    volatility_7d, volatility_30d, volatility_90d,
                    mean_value, coefficient_of_variation,
                    trend_direction, trend_strength,
                    data_points_count, confidence_score, operator_id
                )
                VALUES (
                    :entity_type, :entity_id, :date, :metric_type,
                    :volatility_7d, :volatility_30d, :volatility_90d,
                    :mean_value, :coefficient_of_variation,
                    :trend_direction, :trend_strength,
                    :data_points_count, :confidence_score, :operator_id
                )
                ON CONFLICT (entity_type, entity_id, date, metric_type) DO UPDATE SET
                    volatility_7d = EXCLUDED.volatility_7d,
                    volatility_30d = EXCLUDED.volatility_30d,
                    volatility_90d = EXCLUDED.volatility_90d,
                    mean_value = EXCLUDED.mean_value,
                    coefficient_of_variation = EXCLUDED.coefficient_of_variation,
                    trend_direction = EXCLUDED.trend_direction,
                    trend_strength = EXCLUDED.trend_strength,
                    data_points_count = EXCLUDED.data_points_count,
                    confidence_score = EXCLUDED.confidence_score
                """

                for metric in metrics_to_insert:
                    db.execute_update(insert_query, metric, db="analytics")
                    metrics_inserted += 1

        except Exception as exc:
            context.log.error(f"Failed to process operator {operator_id}: {exc}")
            continue

    context.log.info(
        f"Volatility calculation complete: {metrics_inserted} metrics inserted "
        f"for {len(operators)} operators"
    )

    return Output(
        len(operators),
        metadata={
            "analysis_date": str(analysis_date),
            "operators_processed": len(operators),
            "metrics_inserted": metrics_inserted,
        },
    )
