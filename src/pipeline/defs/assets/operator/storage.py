from dagster import asset, OpExecutionContext
import pandas as pd

from pipeline.defs.resources import AnalyticsDBResource


@asset(group_name="storage", deps=["calculate_operator_risk_scores"])
def validate_risk_scores(
    context: OpExecutionContext, calculate_operator_risk_scores: pd.DataFrame
) -> pd.DataFrame:
    """Validate calculated risk scores before storage"""

    df = calculate_operator_risk_scores.copy()

    # Data quality checks
    validation_errors = []

    # Check for required columns
    required_columns = ["operator_id", "date", "risk_score", "confidence_score"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        validation_errors.append(f"Missing required columns: {missing_columns}")

    # Check for null values in critical fields
    for col in required_columns:
        if col in df.columns and df[col].isnull().any():
            null_count = df[col].isnull().sum()
            validation_errors.append(f"Column {col} has {null_count} null values")

    # Validate score ranges
    score_columns = [
        "risk_score",
        "confidence_score",
        "performance_score",
        "economic_score",
        "network_position_score",
    ]
    for col in score_columns:
        if col in df.columns:
            out_of_range = df[(df[col] < 0) | (df[col] > 100)]
            if not out_of_range.empty:
                validation_errors.append(
                    f"Column {col} has {len(out_of_range)} values outside 0-100 range"
                )

    # Validate HHI range
    if "delegation_hhi" in df.columns:
        invalid_hhi = df[(df["delegation_hhi"] < 0) | (df["delegation_hhi"] > 1)]
        if not invalid_hhi.empty:
            validation_errors.append(
                f"HHI values outside 0-1 range: {len(invalid_hhi)} records"
            )

    # Check for reasonable operational days
    if "operational_days" in df.columns:
        negative_days = df[df["operational_days"] < 0]
        if not negative_days.empty:
            validation_errors.append(
                f"Negative operational days: {len(negative_days)} records"
            )

    if validation_errors:
        context.log.error(f"Data validation failed: {'; '.join(validation_errors)}")
        raise ValueError(f"Data validation failed: {'; '.join(validation_errors)}")

    # Log validation success metrics
    context.log.info(f"✅ Data validation passed for {len(df)} operator records")
    context.log.info(
        f"Risk level distribution: {df['risk_level'].value_counts().to_dict()}"
    )
    context.log.info(f"Average risk score: {df['risk_score'].mean():.2f}")
    context.log.info(f"Average confidence: {df['confidence_score'].mean():.2f}")

    return df


@asset(group_name="storage", deps=["validate_risk_scores"])
def store_operator_analytics(
    context: OpExecutionContext,
    validate_risk_scores: pd.DataFrame,
    analytics_db: AnalyticsDBResource,
) -> None:
    """Store validated operator analytics to PostgreSQL"""

    # Ensure tables exist
    analytics_db.create_tables()

    try:
        # Prepare DataFrame for storage
        df = validate_risk_scores.copy()

        # Handle data type conversions
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["calculated_at"] = pd.to_datetime(df["calculated_at"])

        # Convert numpy types to Python native types for SQLAlchemy compatibility
        numeric_columns = [
            "risk_score",
            "confidence_score",
            "performance_score",
            "economic_score",
            "network_position_score",
            "delegation_hhi",
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)

        int_columns = [
            "snapshot_delegator_count",
            "snapshot_avs_count",
            "slashing_event_count",
            "operational_days",
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].astype(int)

        # Handle negative values that violate constraints
        df.loc[df["snapshot_delegator_count"] < 0, "snapshot_delegator_count"] = 0
        df.loc[df["snapshot_avs_count"] < 0, "snapshot_avs_count"] = 0
        df.loc[df["slashing_event_count"] < 0, "slashing_event_count"] = 0
        df.loc[df["operational_days"] < 0, "operational_days"] = 0

        # Store using pandas method for performance
        analytics_db.store_dataframe(
            df, "operator_analytics", if_exists="replace"
        )  # TODO: Look into having duplicate operator_id + date rows. I think it has to do with block ranges.

        context.log.info(f"✅ Successfully stored {len(df)} operator analytics records")

        # Log storage statistics
        unique_operators = df["operator_id"].nunique()
        date_range = f"{df['date'].min()} to {df['date'].max()}"
        context.log.info(
            f"Stored data for {unique_operators} unique operators, date range: {date_range}"
        )

    except Exception as e:
        context.log.error(f"❌ Failed to store operator analytics: {str(e)}")
        raise


@asset(group_name="storage", deps=["calculate_concentration_metrics"])
def store_concentration_metrics(
    context: OpExecutionContext,
    calculate_concentration_metrics: pd.DataFrame,
    analytics_db: AnalyticsDBResource,
) -> None:
    """Store concentration metrics to supporting table"""

    if calculate_concentration_metrics.empty:
        context.log.info("No concentration metrics to store")
        return

    # Prepare data for storage
    df = calculate_concentration_metrics.copy()
    df["entity_type"] = "operator"
    df["entity_id"] = df["operator_id"]
    df["date"] = pd.Timestamp.now().date()
    df["concentration_type"] = "delegator"

    # Calculate derived metrics
    df["gini_coefficient"] = (
        None  # Would calculate if we had detailed distribution data
    )
    df["effective_entities"] = df.apply(
        lambda row: (
            1 / row["hhi_value"] if row["hhi_value"] > 0 else row["total_delegators"]
        ),
        axis=1,
    )

    try:
        analytics_db.store_dataframe(df, "concentration_metrics", if_exists="append")

        context.log.info(f"✅ Stored concentration metrics for {len(df)} operators")

    except Exception as e:
        context.log.error(f"❌ Failed to store concentration metrics: {str(e)}")
        raise


@asset(group_name="storage", deps=["calculate_volatility_metrics"])
def store_volatility_metrics(
    context: OpExecutionContext,
    calculate_volatility_metrics: pd.DataFrame,
    analytics_db: AnalyticsDBResource,
) -> None:
    """Store volatility metrics to supporting table"""

    if calculate_volatility_metrics.empty:
        context.log.info("No volatility metrics to store")
        return

    # Prepare data for storage
    df = calculate_volatility_metrics.copy()
    df["entity_type"] = "operator"
    df["entity_id"] = df["operator_id"]
    df["date"] = pd.Timestamp.now().date()
    df["metric_type"] = "delegation"

    # Add missing columns with defaults
    df["mean_value"] = None
    df["trend_direction"] = None
    df["trend_strength"] = None

    df = df.drop(columns=["operator_id", "data_points"])

    try:
        analytics_db.store_dataframe(df, "volatility_metrics", if_exists="append")

        context.log.info(f"✅ Stored volatility metrics for {len(df)} operators")

    except Exception as e:
        context.log.error(f"❌ Failed to store volatility metrics: {str(e)}")
        raise


@asset(group_name="storage")
def generate_storage_summary(
    context: OpExecutionContext,
    analytics_db: AnalyticsDBResource,
) -> dict:
    """Generate summary of stored data for monitoring"""

    try:
        # Query summary statistics using raw SQL
        summary_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT operator_id) as unique_operators,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            AVG(risk_score) as avg_risk_score,
            AVG(confidence_score) as avg_confidence,
            COUNT(CASE WHEN risk_level = 'LOW' THEN 1 END) as low_risk_count,
            COUNT(CASE WHEN risk_level = 'MEDIUM' THEN 1 END) as medium_risk_count,
            COUNT(CASE WHEN risk_level = 'HIGH' THEN 1 END) as high_risk_count,
            COUNT(CASE WHEN risk_level = 'CRITICAL' THEN 1 END) as critical_risk_count,
            COUNT(CASE WHEN has_sufficient_data = true THEN 1 END) as sufficient_data_count
        FROM operator_analytics
        WHERE date = CURRENT_DATE
        """

        result_df = analytics_db.execute_query(summary_query)

        if result_df.empty:
            summary = {
                "total_records": 0,
                "unique_operators": 0,
                "earliest_date": None,
                "latest_date": None,
                "avg_risk_score": 0,
                "avg_confidence": 0,
                "risk_distribution": {"LOW": 0, "MEDIUM": 0, "HIGH": 0, "CRITICAL": 0},
                "sufficient_data_count": 0,
                "data_quality_percentage": 0,
            }
        else:
            result = result_df.iloc[0]
            summary = {
                "total_records": int(result["total_records"]),
                "unique_operators": int(result["unique_operators"]),
                "earliest_date": (
                    str(result["earliest_date"]) if result["earliest_date"] else None
                ),
                "latest_date": (
                    str(result["latest_date"]) if result["latest_date"] else None
                ),
                "avg_risk_score": (
                    float(result["avg_risk_score"]) if result["avg_risk_score"] else 0
                ),
                "avg_confidence": (
                    float(result["avg_confidence"]) if result["avg_confidence"] else 0
                ),
                "risk_distribution": {
                    "LOW": int(result["low_risk_count"] or 0),
                    "MEDIUM": int(result["medium_risk_count"] or 0),
                    "HIGH": int(result["high_risk_count"] or 0),
                    "CRITICAL": int(result["critical_risk_count"] or 0),
                },
                "sufficient_data_count": int(result["sufficient_data_count"] or 0),
                "data_quality_percentage": (
                    (result["sufficient_data_count"] / result["total_records"] * 100)
                    if result["total_records"] > 0
                    else 0
                ),
            }

        # Log comprehensive summary
        context.log.info("=== STORAGE SUMMARY ===")
        context.log.info(f"📊 Total records stored: {summary['total_records']}")
        context.log.info(f"👥 Unique operators: {summary['unique_operators']}")
        context.log.info(f"📈 Average risk score: {summary['avg_risk_score']:.2f}")
        context.log.info(f"🎯 Average confidence: {summary['avg_confidence']:.2f}")
        context.log.info(f"📋 Risk distribution: {summary['risk_distribution']}")
        context.log.info(
            f"✅ Data quality: {summary['data_quality_percentage']:.1f}% have sufficient data"
        )
        context.log.info("=== END SUMMARY ===")

        return summary

    except Exception as e:
        context.log.error(f"❌ Failed to generate storage summary: {str(e)}")
        return {"error": str(e)}
