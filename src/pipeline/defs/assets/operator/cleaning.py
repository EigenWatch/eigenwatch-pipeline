# import os
# import pandas as pd
# from dagster import asset, OpExecutionContext

# DEFAULT_REGISTERED_AT = os.getenv("DEFAULT_REGISTERED_AT", "2025-05-01T00:00:00Z")


# @asset(group_name="data_preparation", deps=["extract_operators"])
# def clean_operators(
#     context: OpExecutionContext, extract_operators: pd.DataFrame
# ) -> pd.DataFrame:
#     """Clean and filter operator data for analysis"""

#     # Filter for registered operators only
#     # HACK: Instead of simply checking if is_registered is true,
#     # we first check if they have a registration date. If they do not,
#     # we assume they are still registered (due to limited indexing).
#     # If they do, we rely on is_registered.
#     # TODO:Review the above hack when we are able to index enough data
#     active_operators = extract_operators[
#         extract_operators["registered_at"].isna()
#         | (extract_operators["is_registered"] == True)
#     ].copy()

#     # Remove operators with invalid addresses (null or empty)
#     active_operators = active_operators[
#         active_operators["address"].notna() & (active_operators["address"] != "")
#     ]

#     # Ensure registered_at is datetime
#     # HACK: Because we are indexing only a limited time frame,
#     # if `registered_at` is missing, we provide a default registration
#     # timestamp corresponding to when indexing started.
#     # This ensures such operators are treated as active.
#     # TODO: Remove this hack once we are indexing the full historical data.
#     active_operators["registered_at"] = pd.to_datetime(
#         active_operators["registered_at"], errors="coerce", utc=True
#     )

#     # Fill missing registered_at with default
#     mask_missing = active_operators["registered_at"].isna()
#     active_operators.loc[mask_missing, "registered_at"] = pd.to_datetime(
#         DEFAULT_REGISTERED_AT, utc=True
#     )

#     # Leave registered_at_block and registered_at_transaction as null
#     # but force is_registered = True for these rows (since we assume active)
#     active_operators.loc[mask_missing, "is_registered"] = True

#     # Calculate operational days
#     current_date = pd.Timestamp.now(tz="UTC").normalize()
#     active_operators["operational_days"] = (
#         current_date - active_operators["registered_at"]
#     ).dt.days

#     # Fill missing values with defaults
#     active_operators["delegator_count"] = (
#         active_operators["delegator_count"].fillna(0).astype(int)
#     )
#     active_operators["slashing_event_count"] = (
#         active_operators["slashing_event_count"].fillna(0).astype(int)
#     )
#     active_operators["avs_registration_count"] = (
#         active_operators["avs_registration_count"].fillna(0).astype(int)
#     )

#     context.log.info(
#         f"Cleaned operators: {len(active_operators)} active from {len(extract_operators)} total"
#     )
#     return active_operators


# @asset(group_name="data_preparation", deps=["extract_operator_share_events"])
# def aggregate_current_delegations(
#     context: OpExecutionContext, extract_operator_share_events: pd.DataFrame
# ) -> pd.DataFrame:
#     """Calculate current delegation amounts per operator-staker pair"""

#     if extract_operator_share_events.empty:
#         context.log.warning("No share events found")
#         return pd.DataFrame(columns=["operator", "staker", "current_shares"])

#     # Sort by timestamp to get chronological order
#     events_sorted = extract_operator_share_events.sort_values("block_timestamp").copy()

#     # Calculate cumulative shares for each operator-staker pair
#     delegation_amounts = []

#     for (operator, staker), group in events_sorted.groupby(["operator", "staker"]):
#         current_shares = 0

#         for _, event in group.iterrows():
#             if event["event_type"] == "INCREASED":
#                 current_shares += event["shares"]
#             elif event["event_type"] == "DECREASED":
#                 current_shares -= event["shares"]

#         # Only include active delegations (positive amounts)
#         if current_shares > 0:
#             delegation_amounts.append(
#                 {
#                     "operator": operator,
#                     "staker": staker,
#                     "current_shares": current_shares,
#                 }
#             )

#     result_df = pd.DataFrame(delegation_amounts)

#     if not result_df.empty:
#         context.log.info(
#             f"Calculated current delegations for {result_df['operator'].nunique()} operators"
#         )
#         context.log.info(f"Total delegation relationships: {len(result_df)}")
#     else:
#         context.log.warning("No active delegations found after aggregation")

#     return result_df
