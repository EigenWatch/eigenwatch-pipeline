# defs/assets/analytics/concentration.py
"""
Calculate concentration metrics for operators (HHI, Gini, top-N percentages).
"""

from dagster import asset, OpExecutionContext, Output, DailyPartitionsDefinition
from datetime import datetime
import pandas as pd
import numpy as np
from ...resources import DatabaseResource, ConfigResource


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


# ---------------------------------------------------------------------------
# SAFE METRIC HELPERS
# ---------------------------------------------------------------------------


def calculate_hhi(shares: pd.Series) -> float:
    shares = pd.to_numeric(shares.dropna(), errors="coerce").dropna()
    total = shares.sum()
    if total <= 0:
        return 0.0
    percentages = shares / total
    return float(np.sum(np.square(percentages)))


def calculate_gini(shares: pd.Series) -> float:
    shares = pd.to_numeric(shares.dropna(), errors="coerce").dropna()
    if len(shares) < 2:
        return 0.0

    shares_arr = shares.values.astype(float)
    if shares_arr.sum() <= 0:
        return 0.0

    sorted_vals = np.sort(shares_arr)
    n = len(sorted_vals)
    index = np.arange(1, n + 1)

    numerator = np.sum(index * sorted_vals)
    denominator = n * sorted_vals.sum()

    return float((2 * numerator) / denominator - (n + 1) / n)


def calculate_top_n_percentage(shares: pd.Series, n: int) -> float:
    shares = pd.to_numeric(shares.dropna(), errors="coerce").dropna()
    total = shares.sum()
    if total <= 0:
        return 0.0
    top_n = shares.nlargest(n).sum()
    return float((top_n / total) * 100.0)


def safe_coefficient_of_variation(values: pd.Series) -> float:
    values = pd.to_numeric(values.dropna(), errors="coerce").dropna()
    if len(values) < 2:
        return 0.0
    mean = values.mean()
    if mean <= 0:
        return 0.0
    std = values.std()
    cv = std / mean
    return float(cv) if np.isfinite(cv) else 0.0


# ---------------------------------------------------------------------------
# MAIN ASSET
# ---------------------------------------------------------------------------


@asset(
    partitions_def=daily_partitions,
    description="Calculate concentration metrics (HHI, Gini, top-N) for all operators",
    compute_kind="python",
)
def concentration_metrics_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Output[int]:

    partition_date_str = context.partition_key
    analysis_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    context.log.info(f"Calculating concentration metrics for {analysis_date}")

    operators_result = db.execute_query(
        """
        SELECT DISTINCT operator_id
        FROM operator_daily_snapshots
        WHERE snapshot_date = :analysis_date
        """,
        {"analysis_date": analysis_date},
        db="analytics",
    )

    if not operators_result:
        context.log.warning(f"No operators found for {analysis_date}")
        return Output(0, metadata={"skipped": True})

    operators = [row[0] for row in operators_result]
    context.log.info(f"Processing {len(operators)} operators")

    metrics_inserted = 0

    # ------------------------------
    # PROCESS EACH OPERATOR
    # ------------------------------
    for idx, operator_id in enumerate(operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(f"Processing operator {idx}/{len(operators)}")

        try:
            metrics_to_insert = []

            # ------------------------------------------------------------
            # 1. DELEGATOR CONCENTRATION
            # ------------------------------------------------------------
            delegators = db.execute_query(
                """
                SELECT staker_id, SUM(shares) as total_shares
                FROM operator_delegator_shares_snapshots
                WHERE operator_id = :operator_id
                  AND snapshot_date = :analysis_date
                  AND is_delegated = TRUE
                GROUP BY staker_id
                HAVING SUM(shares) > 0
                """,
                {"operator_id": operator_id, "analysis_date": analysis_date},
                db="analytics",
            )

            if delegators:
                df = pd.DataFrame(delegators, columns=["staker_id", "shares"])
                # 🔥 ALWAYS CONVERT TO FLOAT
                shares = pd.to_numeric(df["shares"], errors="coerce").dropna()

                metrics_to_insert.append(
                    {
                        "entity_type": "operator",
                        "entity_id": operator_id,
                        "date": analysis_date,
                        "concentration_type": "delegator",
                        "hhi_value": calculate_hhi(shares),
                        "gini_coefficient": calculate_gini(shares),
                        "coefficient_of_variation": safe_coefficient_of_variation(
                            shares
                        ),
                        "top_1_percentage": calculate_top_n_percentage(shares, 1),
                        "top_5_percentage": calculate_top_n_percentage(shares, 5),
                        "top_10_percentage": calculate_top_n_percentage(shares, 10),
                        "total_entities": len(shares),
                        "effective_entities": (
                            float(1 / calculate_hhi(shares))
                            if calculate_hhi(shares) > 0
                            else len(shares)
                        ),
                        "total_amount": float(shares.sum()),
                        "operator_id": operator_id,
                    }
                )

            # ------------------------------------------------------------
            # 2. STRATEGY CONCENTRATION
            # ------------------------------------------------------------
            strategies = db.execute_query(
                """
                SELECT strategy_id, max_magnitude
                FROM operator_strategy_daily_snapshots
                WHERE operator_id = :operator_id
                  AND snapshot_date = :analysis_date
                  AND max_magnitude > 0
                """,
                {"operator_id": operator_id, "analysis_date": analysis_date},
                db="analytics",
            )

            if strategies:
                df = pd.DataFrame(strategies, columns=["strategy_id", "magnitude"])
                # 🔥 ALWAYS CONVERT TO FLOAT
                magnitudes = pd.to_numeric(df["magnitude"], errors="coerce").dropna()

                metrics_to_insert.append(
                    {
                        "entity_type": "operator",
                        "entity_id": operator_id,
                        "date": analysis_date,
                        "concentration_type": "strategy",
                        "hhi_value": calculate_hhi(magnitudes),
                        "gini_coefficient": calculate_gini(magnitudes),
                        "coefficient_of_variation": safe_coefficient_of_variation(
                            magnitudes
                        ),
                        "top_1_percentage": calculate_top_n_percentage(magnitudes, 1),
                        "top_5_percentage": calculate_top_n_percentage(magnitudes, 5),
                        "top_10_percentage": calculate_top_n_percentage(magnitudes, 10),
                        "total_entities": len(magnitudes),
                        "effective_entities": (
                            float(1 / calculate_hhi(magnitudes))
                            if calculate_hhi(magnitudes) > 0
                            else len(magnitudes)
                        ),
                        "total_amount": float(magnitudes.sum()),
                        "operator_id": operator_id,
                    }
                )

            # ------------------------------------------------------------
            # 3. AVS CONCENTRATION
            # ------------------------------------------------------------
            avs_data = db.execute_query(
                """
                SELECT os.avs_id, SUM(oa.magnitude) as total_magnitude
                FROM operator_allocation_snapshots oa
                JOIN operator_sets os ON oa.operator_set_id = os.id
                WHERE oa.operator_id = :operator_id
                  AND oa.snapshot_date = :analysis_date
                GROUP BY os.avs_id
                HAVING SUM(oa.magnitude) > 0
                """,
                {"operator_id": operator_id, "analysis_date": analysis_date},
                db="analytics",
            )

            if avs_data:
                df = pd.DataFrame(avs_data, columns=["avs_id", "magnitude"])
                # 🔥 ALWAYS CONVERT TO FLOAT
                magnitudes = pd.to_numeric(df["magnitude"], errors="coerce").dropna()

                metrics_to_insert.append(
                    {
                        "entity_type": "operator",
                        "entity_id": operator_id,
                        "date": analysis_date,
                        "concentration_type": "avs",
                        "hhi_value": calculate_hhi(magnitudes),
                        "gini_coefficient": calculate_gini(magnitudes),
                        "coefficient_of_variation": safe_coefficient_of_variation(
                            magnitudes
                        ),
                        "top_1_percentage": calculate_top_n_percentage(magnitudes, 1),
                        "top_5_percentage": calculate_top_n_percentage(magnitudes, 5),
                        "top_10_percentage": calculate_top_n_percentage(magnitudes, 10),
                        "total_entities": len(magnitudes),
                        "effective_entities": (
                            float(1 / calculate_hhi(magnitudes))
                            if calculate_hhi(magnitudes) > 0
                            else len(magnitudes)
                        ),
                        "total_amount": float(magnitudes.sum()),
                        "operator_id": operator_id,
                    }
                )

            # ------------------------------------------------------------
            # INSERT
            # ------------------------------------------------------------
            insert_query = """
            INSERT INTO concentration_metrics (
                entity_type, entity_id, date, concentration_type,
                hhi_value, gini_coefficient, coefficient_of_variation,
                top_1_percentage, top_5_percentage, top_10_percentage,
                total_entities, effective_entities, total_amount, operator_id
            )
            VALUES (
                :entity_type, :entity_id, :date, :concentration_type,
                :hhi_value, :gini_coefficient, :coefficient_of_variation,
                :top_1_percentage, :top_5_percentage, :top_10_percentage,
                :total_entities, :effective_entities, :total_amount, :operator_id
            )
            ON CONFLICT (entity_type, entity_id, date, concentration_type) DO UPDATE SET
                hhi_value = EXCLUDED.hhi_value,
                gini_coefficient = EXCLUDED.gini_coefficient,
                coefficient_of_variation = EXCLUDED.coefficient_of_variation,
                top_1_percentage = EXCLUDED.top_1_percentage,
                top_5_percentage = EXCLUDED.top_5_percentage,
                top_10_percentage = EXCLUDED.top_10_percentage,
                total_entities = EXCLUDED.total_entities,
                effective_entities = EXCLUDED.effective_entities,
                total_amount = EXCLUDED.total_amount
            """

            for metric in metrics_to_insert:
                db.execute_update(insert_query, metric, db="analytics")
                metrics_inserted += 1

        except Exception as exc:
            context.log.error(f"Failed to process operator {operator_id}: {exc}")

    context.log.info(
        f"Concentration calculation complete: {metrics_inserted} metrics inserted "
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
