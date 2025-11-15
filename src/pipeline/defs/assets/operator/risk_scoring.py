# import pandas as pd
# import numpy as np
# from dagster import asset, OpExecutionContext

# from utils.debug_log import debug_log


# @asset(
#     group_name="risk_scoring",
#     deps=[
#         "clean_operators",
#         "calculate_concentration_metrics",
#         "calculate_volatility_metrics",
#         "calculate_slashing_metrics",
#         "calculate_commission_metrics",
#         "aggregate_current_delegations",
#     ],
# )
# def calculate_operator_risk_scores(
#     context: OpExecutionContext,
#     clean_operators: pd.DataFrame,
#     calculate_concentration_metrics: pd.DataFrame,
#     calculate_volatility_metrics: pd.DataFrame,
#     calculate_slashing_metrics: pd.DataFrame,
#     calculate_commission_metrics: pd.DataFrame,
#     aggregate_current_delegations: pd.DataFrame,
# ) -> pd.DataFrame:
#     """Calculate final risk scores for operators"""
#     risk_scores = []

#     for _, operator in clean_operators.iterrows():
#         operator_id = operator["id"]

#         # Get metrics for this operator
#         concentration = calculate_concentration_metrics[
#             calculate_concentration_metrics["operator_id"] == operator_id
#         ]
#         volatility = calculate_volatility_metrics[
#             calculate_volatility_metrics["operator_id"] == operator_id
#         ]
#         slashing = calculate_slashing_metrics[
#             calculate_slashing_metrics["operator_id"] == operator_id
#         ]
#         commission = calculate_commission_metrics[
#             calculate_commission_metrics["operator_id"] == operator_id
#         ]

#         # Extract values with defaults
#         hhi_value = (
#             concentration["hhi_value"].iloc[0] if not concentration.empty else 0.5
#         )
#         volatility_30d = (
#             volatility["volatility_30d"].iloc[0] if not volatility.empty else 0.1
#         )
#         slashing_score = (
#             slashing["slashing_score"].iloc[0] if not slashing.empty else 100
#         )
#         commission_score = (
#             commission["commission_score"].iloc[0] if not commission.empty else 75
#         )

#         # Calculate component scores

#         # 1. Performance Score (50% weight)
#         # - Slashing score (60%), Stability score (30%), Tenure score (10%)
#         # Reciprocal form for stability chosen for smooth decay: distinguishes "very unstable" vs "extremely unstable",
#         stability_score = 100 * (1 / (1 + volatility_30d))
#         tenure_score = min(100, (operator["operational_days"] / 365) * 100)

#         performance_score = (
#             slashing_score * 0.6 + stability_score * 0.3 + tenure_score * 0.1
#         )

#         # 2. Economic Score (35% weight)
#         # - Concentration score (50%), Commission score (30%), Growth score (20%)
#         concentration_score = max(0, 100 - (hhi_value * 100))
#         growth_score = 70  # TODO: Default moderate growth (would calculate from historical data). Review your initial documentations

#         economic_score = (
#             concentration_score * 0.5 + commission_score * 0.3 + growth_score * 0.2
#         )

#         # 3. Network Position Score (15% weight)
#         # Simplified calculation based on delegator count and total stake
#         # TODO: Review other ways of this being done
#         delegator_count = operator["delegator_count"]
#         total_stake = (
#             aggregate_current_delegations[
#                 aggregate_current_delegations["operator"] == operator_id
#             ]["current_shares"].sum()
#             if not aggregate_current_delegations.empty
#             else 0
#         )

#         # Size percentile (simplified)
#         size_score = min(100, np.log10(max(1, total_stake)) * 20)  # Log scale
#         distribution_quality = concentration_score  # Reuse concentration inverse

#         network_position_score = size_score * 0.7 + distribution_quality * 0.3

#         # Final composite risk score
#         risk_score = (
#             performance_score * 0.50
#             + economic_score * 0.35
#             + network_position_score * 0.15
#         )

#         # Risk level categorization
#         if risk_score >= 80:
#             risk_level = "LOW"
#         elif risk_score >= 60:
#             risk_level = "MEDIUM"
#         elif risk_score >= 40:
#             risk_level = "HIGH"
#         else:
#             risk_level = "CRITICAL"

#         # Confidence score based on data availability and tenure
#         # TODO: Review how confidence factor is gotten, especially the part about recent activity bonus
#         confidence_factors = []
#         if operator["operational_days"] >= 30:
#             confidence_factors.append(50)
#         else:
#             confidence_factors.append((operator["operational_days"] / 30) * 50)

#         if delegator_count >= 10:
#             confidence_factors.append(30)
#         elif delegator_count >= 2:
#             confidence_factors.append(20)
#         else:
#             confidence_factors.append(10)

#         if operator["operational_days"] <= 7:
#             confidence_factors.append(20)  # Recent activity bonus
#         else:
#             confidence_factors.append(10)

#         confidence_score = sum(confidence_factors)

#         # Create final record
#         risk_scores.append(
#             {
#                 "operator_id": operator_id,
#                 "date": pd.Timestamp.now().date(),
#                 "risk_score": round(risk_score, 2),
#                 "confidence_score": round(confidence_score, 2),
#                 "risk_level": risk_level,
#                 "performance_score": round(performance_score, 2),
#                 "economic_score": round(economic_score, 2),
#                 "network_position_score": round(network_position_score, 2),
#                 # Detailed metrics for reference
#                 "delegation_hhi": round(hhi_value, 4),
#                 "delegation_volatility_30d": round(volatility_30d, 4),
#                 "slashing_event_count": (
#                     slashing["slashing_count"].iloc[0] if not slashing.empty else 0
#                 ),
#                 "operational_days": operator["operational_days"],
#                 # Snapshot values
#                 "snapshot_delegator_count": delegator_count,
#                 "snapshot_total_delegated_shares": total_stake,
#                 "snapshot_avs_count": operator["avs_registration_count"],
#                 "snapshot_commission_bips": (
#                     commission["current_commission_bips"].iloc[0]
#                     if not commission.empty
#                     else None
#                 ),
#                 # Status flags
#                 "is_active": (
#                     (
#                         pd.Timestamp.now().date() - operator["last_activity_at"].date()
#                     ).days
#                     <= 30
#                     if pd.notna(operator["last_activity_at"])
#                     else False
#                 ),
#                 "has_sufficient_data": operator["operational_days"] >= 30
#                 and delegator_count >= 2,
#                 # Metadata
#                 "calculated_at": pd.Timestamp.now(),
#             }
#         )

#     result_df = pd.DataFrame(risk_scores)

#     debug_log(result_df)

#     # Log summary statistics
#     if not result_df.empty:
#         context.log.info(f"Calculated risk scores for {len(result_df)} operators")
#         context.log.info(f"Risk level distribution:")
#         context.log.info(f"  LOW: {(result_df['risk_level'] == 'LOW').sum()}")
#         context.log.info(f"  MEDIUM: {(result_df['risk_level'] == 'MEDIUM').sum()}")
#         context.log.info(f"  HIGH: {(result_df['risk_level'] == 'HIGH').sum()}")
#         context.log.info(f"  CRITICAL: {(result_df['risk_level'] == 'CRITICAL').sum()}")
#         context.log.info(f"Average risk score: {result_df['risk_score'].mean():.2f}")
#         context.log.info(
#             f"Average confidence: {result_df['confidence_score'].mean():.2f}"
#         )

#     return result_df
