import pandas as pd
import numpy as np
from dagster import asset, OpExecutionContext


@asset(group_name="calculations", deps=["aggregate_current_delegations"])
def calculate_concentration_metrics(
    context: OpExecutionContext, aggregate_current_delegations: pd.DataFrame
) -> pd.DataFrame:
    """Calculate HHI and distribution metrics for each operator"""

    if aggregate_current_delegations.empty:
        context.log.warning(
            "No delegation data available for concentration calculation"
        )
        return pd.DataFrame()

    concentration_metrics = []

    for operator_id in aggregate_current_delegations["operator"].unique():
        operator_delegations = aggregate_current_delegations[
            aggregate_current_delegations["operator"] == operator_id
        ]

        if len(operator_delegations) == 0:
            continue

        # Calculate total and percentages
        total_shares = operator_delegations["current_shares"].sum()
        if total_shares <= 0:
            continue

        operator_delegations = operator_delegations.copy()
        operator_delegations["percentage"] = (
            operator_delegations["current_shares"] / total_shares
        )

        # Calculate HHI (sum of squared percentages)
        hhi = (operator_delegations["percentage"] ** 2).sum()

        # Calculate coefficient of variation for distribution quality
        amounts = operator_delegations["current_shares"]
        cv = amounts.std() / amounts.mean() if amounts.mean() > 0 else 0

        # Calculate top delegator percentages
        sorted_percentages = operator_delegations["percentage"].sort_values(
            ascending=False
        )
        top_1_pct = sorted_percentages.iloc[0] if len(sorted_percentages) > 0 else 0
        top_5_pct = (
            sorted_percentages.head(5).sum()
            if len(sorted_percentages) >= 5
            else sorted_percentages.sum()
        )

        concentration_metrics.append(
            {
                "operator_id": operator_id,
                "hhi_value": hhi,
                "coefficient_of_variation": cv,
                "top_1_percentage": top_1_pct * 100,  # Convert to percentage
                "top_5_percentage": top_5_pct * 100,
                "total_delegators": len(operator_delegations),
                "effective_delegators": (
                    1 / hhi if hhi > 0 else len(operator_delegations)
                ),
                "total_delegated_shares": total_shares,
            }
        )

    result_df = pd.DataFrame(concentration_metrics)
    context.log.info(f"Calculated concentration metrics for {len(result_df)} operators")

    return result_df


@asset(group_name="calculations", deps=["aggregate_current_delegations"])
def calculate_volatility_metrics(
    context: OpExecutionContext, aggregate_current_delegations: pd.DataFrame
) -> pd.DataFrame:
    """Calculate delegation volatility metrics"""

    # Note: This is a simplified version since we don't have historical daily data yet
    # In production, this would use time-series delegation history

    if aggregate_current_delegations.empty:
        return pd.DataFrame()

    volatility_metrics = []

    for operator_id in aggregate_current_delegations["operator"].unique():
        operator_delegations = aggregate_current_delegations[
            aggregate_current_delegations["operator"] == operator_id
        ]

        if len(operator_delegations) < 2:
            # Not enough data for meaningful volatility calculation
            volatility_metrics.append(
                {
                    "operator_id": operator_id,
                    "volatility_7d": 0.0,
                    "volatility_30d": 0.0,
                    "volatility_90d": 0.0,
                    "coefficient_of_variation": 0.0,
                    "data_points_available": len(operator_delegations),
                }
            )
            continue

        # Use current delegator amount distribution as proxy for volatility
        amounts = operator_delegations["current_shares"]
        mean_amount = amounts.mean()
        std_amount = amounts.std()

        cv = std_amount / mean_amount if mean_amount > 0 else 0

        # Simplified volatility estimates (in production, use historical time series)
        volatility_metrics.append(
            {
                "operator_id": operator_id,
                "volatility_7d": cv,  # Proxy using current distribution
                "volatility_30d": cv * 0.8,  # Scaled approximation
                "volatility_90d": cv * 0.6,  # Scaled approximation
                "coefficient_of_variation": cv,
                "data_points_available": len(operator_delegations),
            }
        )

    result_df = pd.DataFrame(volatility_metrics)
    context.log.info(f"Calculated volatility metrics for {len(result_df)} operators")

    return result_df


@asset(group_name="calculations", deps=["extract_operator_slashed_events"])
def calculate_slashing_metrics(
    context: OpExecutionContext, extract_operator_slashed_events: pd.DataFrame
) -> pd.DataFrame:
    """Calculate slashing-related performance metrics"""

    # Get all unique operators first (including those with no slashing events)
    if extract_operator_slashed_events.empty:
        context.log.info("No slashing events found - all operators have clean records")
        return pd.DataFrame(
            columns=[
                "operator_id",
                "slashing_count",
                "total_slashed_amount",
                "slashing_score",
            ]
        )

    slashing_metrics = []

    # Process operators with slashing events
    for operator_id in extract_operator_slashed_events["operator"].unique():
        operator_slashings = extract_operator_slashed_events[
            extract_operator_slashed_events["operator"] == operator_id
        ]

        slashing_count = len(operator_slashings)

        # Parse wad_slashed amounts (they might be arrays or strings)
        total_slashed = 0
        for _, event in operator_slashings.iterrows():
            wad_slashed = event["wad_slashed"]
            if pd.notna(wad_slashed):
                # Handle different formats of wad_slashed
                if isinstance(wad_slashed, str):
                    try:
                        # Parse array-like strings
                        amounts = (
                            eval(wad_slashed)
                            if wad_slashed.startswith("[")
                            else [float(wad_slashed)]
                        )
                        total_slashed += sum(amounts)
                    except:
                        pass
                elif isinstance(wad_slashed, (int, float)):
                    total_slashed += wad_slashed

        # Calculate slashing score (lower is worse)
        # Base score of 100, minus 25 points per slashing event
        slashing_score = max(0, 100 - (slashing_count * 25))

        slashing_metrics.append(
            {
                "operator_id": operator_id,
                "slashing_count": slashing_count,
                "total_slashed_amount": total_slashed,
                "slashing_score": slashing_score,
                "most_recent_slashing": operator_slashings["block_timestamp"].max(),
            }
        )

    result_df = pd.DataFrame(slashing_metrics)
    context.log.info(f"Calculated slashing metrics for {len(result_df)} operators")

    if not result_df.empty:
        total_slashed_operators = len(result_df)
        context.log.info(f"Operators with slashing events: {total_slashed_operators}")
        context.log.info(f"Total slashing events: {result_df['slashing_count'].sum()}")

    return result_df


@asset(group_name="calculations", deps=["extract_operator_commission_events"])
def calculate_commission_metrics(
    context: OpExecutionContext, extract_operator_commission_events: pd.DataFrame
) -> pd.DataFrame:
    """Calculate current commission rates for operators"""

    if extract_operator_commission_events.empty:
        context.log.warning("No commission events found")
        return pd.DataFrame(
            columns=["operator_id", "current_commission_bips", "commission_score"]
        )

    commission_metrics = []

    for operator_id in extract_operator_commission_events["operator"].unique():
        operator_commissions = extract_operator_commission_events[
            extract_operator_commission_events["operator"] == operator_id
        ].sort_values("block_timestamp")

        # Get most recent commission rate
        latest_commission = operator_commissions.iloc[-1]
        current_rate = latest_commission["new_commission_bips"]

        # Calculate commission score based on reasonableness
        # 500-1500 bips (5-15%) is considered optimal
        if pd.isna(current_rate):
            commission_score = 50  # Default moderate score
        elif 500 <= current_rate <= 1500:  # 5-15%
            commission_score = 100  # Optimal range
        elif current_rate < 500:  # Below 5%
            commission_score = 70  # Potentially unsustainable
        elif 1500 < current_rate <= 2500:  # 15-25%
            commission_score = 60  # High but acceptable
        else:  # Above 25%
            commission_score = 25  # Excessive

        commission_metrics.append(
            {
                "operator_id": operator_id,
                "current_commission_bips": current_rate,
                "commission_score": commission_score,
                "commission_changes": len(operator_commissions),
            }
        )

    result_df = pd.DataFrame(commission_metrics)
    context.log.info(f"Calculated commission metrics for {len(result_df)} operators")

    return result_df


@asset(
    group_name="risk_scoring",
    deps=[
        "clean_operators",
        "calculate_concentration_metrics",
        "calculate_volatility_metrics",
        "calculate_slashing_metrics",
        "calculate_commission_metrics",
        "aggregate_current_delegations",
    ],
)
def calculate_operator_risk_scores(
    context: OpExecutionContext,
    clean_operators: pd.DataFrame,
    calculate_concentration_metrics: pd.DataFrame,
    calculate_volatility_metrics: pd.DataFrame,
    calculate_slashing_metrics: pd.DataFrame,
    calculate_commission_metrics: pd.DataFrame,
    aggregate_current_delegations: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate final risk scores for operators"""

    risk_scores = []

    for _, operator in clean_operators.iterrows():
        operator_id = operator["id"]

        # Get metrics for this operator
        concentration = calculate_concentration_metrics[
            calculate_concentration_metrics["operator_id"] == operator_id
        ]
        volatility = calculate_volatility_metrics[
            calculate_volatility_metrics["operator_id"] == operator_id
        ]
        slashing = calculate_slashing_metrics[
            calculate_slashing_metrics["operator_id"] == operator_id
        ]
        commission = calculate_commission_metrics[
            calculate_commission_metrics["operator_id"] == operator_id
        ]

        # Extract values with defaults
        hhi_value = (
            concentration["hhi_value"].iloc[0] if not concentration.empty else 0.5
        )
        volatility_30d = (
            volatility["volatility_30d"].iloc[0] if not volatility.empty else 0.1
        )
        slashing_score = (
            slashing["slashing_score"].iloc[0] if not slashing.empty else 100
        )
        commission_score = (
            commission["commission_score"].iloc[0] if not commission.empty else 75
        )

        # Calculate component scores

        # 1. Performance Score (50% weight)
        # - Slashing score (60%), Stability score (30%), Tenure score (10%)
        stability_score = max(0, 100 - (volatility_30d * 100))
        tenure_score = min(100, (operator["operational_days"] / 365) * 100)

        performance_score = (
            slashing_score * 0.6 + stability_score * 0.3 + tenure_score * 0.1
        )

        # 2. Economic Score (35% weight)
        # - Concentration score (50%), Commission score (30%), Growth score (20%)
        concentration_score = max(0, 100 - (hhi_value * 100))
        growth_score = (
            70  # Default moderate growth (would calculate from historical data)
        )

        economic_score = (
            concentration_score * 0.5 + commission_score * 0.3 + growth_score * 0.2
        )

        # 3. Network Position Score (15% weight)
        # Simplified calculation based on delegator count and total stake
        delegator_count = operator["delegator_count"]
        total_stake = (
            aggregate_current_delegations[
                aggregate_current_delegations["operator"] == operator_id
            ]["current_shares"].sum()
            if not aggregate_current_delegations.empty
            else 0
        )

        # Size percentile (simplified)
        size_score = min(100, np.log10(max(1, total_stake)) * 20)  # Log scale
        distribution_quality = concentration_score  # Reuse concentration inverse

        network_position_score = size_score * 0.7 + distribution_quality * 0.3

        # Final composite risk score
        risk_score = (
            performance_score * 0.50
            + economic_score * 0.35
            + network_position_score * 0.15
        )

        # Risk level categorization
        if risk_score >= 80:
            risk_level = "LOW"
        elif risk_score >= 60:
            risk_level = "MEDIUM"
        elif risk_score >= 40:
            risk_level = "HIGH"
        else:
            risk_level = "CRITICAL"

        # Confidence score based on data availability and tenure
        confidence_factors = []
        if operator["operational_days"] >= 30:
            confidence_factors.append(50)
        else:
            confidence_factors.append((operator["operational_days"] / 30) * 50)

        if delegator_count >= 10:
            confidence_factors.append(30)
        elif delegator_count >= 2:
            confidence_factors.append(20)
        else:
            confidence_factors.append(10)

        if operator["operational_days"] <= 7:
            confidence_factors.append(20)  # Recent activity bonus
        else:
            confidence_factors.append(10)

        confidence_score = sum(confidence_factors)

        # Create final record
        risk_scores.append(
            {
                "operator_id": operator_id,
                "date": pd.Timestamp.now().date(),
                "risk_score": round(risk_score, 2),
                "confidence_score": round(confidence_score, 2),
                "risk_level": risk_level,
                "performance_score": round(performance_score, 2),
                "economic_score": round(economic_score, 2),
                "network_position_score": round(network_position_score, 2),
                # Detailed metrics for reference
                "delegation_hhi": round(hhi_value, 4),
                "delegation_volatility_30d": round(volatility_30d, 4),
                "slashing_event_count": (
                    slashing["slashing_count"].iloc[0] if not slashing.empty else 0
                ),
                "operational_days": operator["operational_days"],
                # Snapshot values
                "snapshot_delegator_count": delegator_count,
                "snapshot_total_delegated_shares": total_stake,
                "snapshot_avs_count": operator["avs_registration_count"],
                "snapshot_commission_bips": (
                    commission["current_commission_bips"].iloc[0]
                    if not commission.empty
                    else None
                ),
                # Status flags
                "is_active": (
                    (
                        pd.Timestamp.now().date() - operator["last_activity_at"].date()
                    ).days
                    <= 30
                    if pd.notna(operator["last_activity_at"])
                    else False
                ),
                "has_sufficient_data": operator["operational_days"] >= 30
                and delegator_count >= 2,
                # Metadata
                "calculated_at": pd.Timestamp.now(),
            }
        )

    result_df = pd.DataFrame(risk_scores)

    # Log summary statistics
    if not result_df.empty:
        context.log.info(f"Calculated risk scores for {len(result_df)} operators")
        context.log.info(f"Risk level distribution:")
        context.log.info(f"  LOW: {(result_df['risk_level'] == 'LOW').sum()}")
        context.log.info(f"  MEDIUM: {(result_df['risk_level'] == 'MEDIUM').sum()}")
        context.log.info(f"  HIGH: {(result_df['risk_level'] == 'HIGH').sum()}")
        context.log.info(f"  CRITICAL: {(result_df['risk_level'] == 'CRITICAL').sum()}")
        context.log.info(f"Average risk score: {result_df['risk_score'].mean():.2f}")
        context.log.info(
            f"Average confidence: {result_df['confidence_score'].mean():.2f}"
        )

    return result_df
