# import pandas as pd
# from dagster import asset, OpExecutionContext
# from utils.calculations import (
#     compute_commission_metrics,
#     compute_concentration_metrics,
#     compute_slashing_metrics,
#     compute_volatility_metrics,
# )


# @asset(group_name="calculations", deps=["aggregate_current_delegations"])
# def calculate_concentration_metrics(
#     context: OpExecutionContext, aggregate_current_delegations: pd.DataFrame
# ) -> pd.DataFrame:
#     """Calculate HHI and distribution metrics for each operator"""
#     if aggregate_current_delegations.empty:
#         context.log.warning(
#             "No delegation data available for concentration calculation"
#         )
#         return pd.DataFrame()

#     concentration_metrics = []

#     for operator_id in aggregate_current_delegations["operator"].unique():
#         operator_delegations = aggregate_current_delegations[
#             aggregate_current_delegations["operator"] == operator_id
#         ]

#         if len(operator_delegations) == 0:
#             continue

#         metrics = compute_concentration_metrics(
#             df=operator_delegations,
#             amount_col="current_shares",
#         )

#         if not metrics:
#             continue

#         concentration_metrics.append({"operator_id": operator_id, **metrics})

#     result_df = pd.DataFrame(concentration_metrics)
#     context.log.info(f"Calculated concentration metrics for {len(result_df)} operators")

#     return result_df


# @asset(group_name="calculations", deps=["aggregate_current_delegations"])
# def calculate_volatility_metrics(
#     context: OpExecutionContext, aggregate_current_delegations: pd.DataFrame
# ) -> pd.DataFrame:
#     """Calculate delegation volatility metrics"""
#     # HACK: This is a simplified version since we don't have historical daily data yet
#     # TODO: In production, this would use time-series delegation history
#     if aggregate_current_delegations.empty:
#         return pd.DataFrame()

#     volatility_metrics = []

#     for operator_id in aggregate_current_delegations["operator"].unique():
#         operator_delegations = aggregate_current_delegations[
#             aggregate_current_delegations["operator"] == operator_id
#         ]

#         metrics = compute_volatility_metrics(
#             df=operator_delegations, amount_col="current_shares"
#         )

#         volatility_metrics.append({"operator_id": operator_id, **metrics})

#     result_df = pd.DataFrame(volatility_metrics)
#     context.log.info(f"Calculated volatility metrics for {len(result_df)} operators")

#     return result_df


# @asset(group_name="calculations", deps=["extract_operator_slashed_events"])
# def calculate_slashing_metrics(
#     context: OpExecutionContext, extract_operator_slashed_events: pd.DataFrame
# ) -> pd.DataFrame:
#     """Calculate slashing-related performance metrics"""
#     if extract_operator_slashed_events.empty:
#         context.log.info("No slashing events found - all operators have clean records")
#         return pd.DataFrame(
#             columns=[
#                 "operator_id",
#                 "slashing_count",
#                 "total_slashed",
#                 "slashing_score",
#                 "most_recent",
#             ]
#         )

#     slashing_metrics = []

#     for operator_id in extract_operator_slashed_events["operator"].unique():
#         operator_slashings = extract_operator_slashed_events[
#             extract_operator_slashed_events["operator"] == operator_id
#         ]

#         metrics = compute_slashing_metrics(
#             df=operator_slashings,
#             amount_col="wad_slashed",
#             timestamp_col="block_timestamp",
#         )

#         slashing_metrics.append({"operator_id": operator_id, **metrics})

#     result_df = pd.DataFrame(slashing_metrics)
#     context.log.info(f"Calculated slashing metrics for {len(result_df)} operators")

#     if not result_df.empty:
#         total_slashed_operators = len(result_df)
#         context.log.info(f"Operators with slashing events: {total_slashed_operators}")
#         context.log.info(f"Total slashing events: {result_df['slashing_count'].sum()}")

#     return result_df


# @asset(group_name="calculations", deps=["extract_operator_commission_events"])
# def calculate_commission_metrics(
#     context: OpExecutionContext, extract_operator_commission_events: pd.DataFrame
# ) -> pd.DataFrame:
#     """Calculate current commission rates for operators"""
#     if extract_operator_commission_events.empty:
#         context.log.warning("No commission events found")
#         return pd.DataFrame(
#             columns=["operator_id", "current_rate", "commission_score", "changes"]
#         )

#     commission_metrics = []

#     for operator_id in extract_operator_commission_events["operator"].unique():
#         operator_commissions = extract_operator_commission_events[
#             extract_operator_commission_events["operator"] == operator_id
#         ]

#         metrics = compute_commission_metrics(
#             df=operator_commissions,
#             rate_col="new_commission_bips",
#             timestamp_col="block_timestamp",
#         )

#         commission_metrics.append(
#             {
#                 "operator_id": operator_id,
#                 "current_commission_bips": metrics["current_rate"],
#                 "commission_score": metrics["commission_score"],
#                 "commission_changes": metrics["changes"],
#             }
#         )

#     result_df = pd.DataFrame(commission_metrics)
#     context.log.info(f"Calculated commission metrics for {len(result_df)} operators")

#     return result_df
