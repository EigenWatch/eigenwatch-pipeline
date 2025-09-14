from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Integer,
    Numeric,
    Date,
    DateTime,
    Boolean,
    CheckConstraint,
    PrimaryKeyConstraint,
    Index,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OperatorAnalytics(Base):
    """SQLAlchemy model for operator analytics table"""

    __tablename__ = "operator_analytics"

    # Primary Keys
    operator_id = Column(String(50), nullable=False)
    date = Column(Date, nullable=False)

    # Risk Assessment Scores
    risk_score = Column(
        Numeric(5, 2),
        CheckConstraint("risk_score >= 0 AND risk_score <= 100"),
        nullable=False,
    )
    confidence_score = Column(
        Numeric(5, 2),
        CheckConstraint("confidence_score >= 0 AND confidence_score <= 100"),
        nullable=False,
    )
    risk_level = Column(
        String(10),
        CheckConstraint("risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')"),
        nullable=False,
    )

    # Component Scores
    performance_score = Column(
        Numeric(5, 2),
        CheckConstraint("performance_score >= 0 AND performance_score <= 100"),
    )
    economic_score = Column(
        Numeric(5, 2),
        CheckConstraint("economic_score >= 0 AND economic_score <= 100"),
    )
    network_position_score = Column(
        Numeric(5, 2),
        CheckConstraint(
            "network_position_score >= 0 AND network_position_score <= 100"
        ),
    )

    # Calculated Metrics
    delegation_hhi = Column(
        Numeric(6, 4),
        CheckConstraint("delegation_hhi >= 0 AND delegation_hhi <= 1"),
    )
    delegation_volatility_7d = Column(Numeric(8, 4))
    delegation_volatility_30d = Column(Numeric(8, 4))
    delegation_volatility_90d = Column(Numeric(8, 4))
    growth_rate_30d = Column(Numeric(8, 4))
    growth_consistency_score = Column(
        Numeric(5, 2),
        CheckConstraint(
            "growth_consistency_score IS NULL OR (growth_consistency_score >= 0 AND growth_consistency_score <= 100)"
        ),
    )
    size_percentile = Column(
        Numeric(5, 2),
        CheckConstraint(
            "size_percentile IS NULL OR (size_percentile >= 0 AND size_percentile <= 100)"
        ),
    )
    delegator_distribution_cv = Column(
        Numeric(8, 4),
        CheckConstraint(
            "delegator_distribution_cv IS NULL OR delegator_distribution_cv >= 0"
        ),
    )

    # Snapshot Metrics (point-in-time values)
    snapshot_total_delegated_shares = Column(
        Numeric,
        CheckConstraint("snapshot_total_delegated_shares >= 0"),
        default=0,
    )
    snapshot_delegator_count = Column(
        Integer,
        CheckConstraint("snapshot_delegator_count >= 0"),
        default=0,
    )
    snapshot_avs_count = Column(
        Integer,
        CheckConstraint("snapshot_avs_count >= 0"),
        default=0,
    )
    snapshot_commission_bips = Column(
        Numeric(8, 2),
        CheckConstraint(
            "snapshot_commission_bips IS NULL OR snapshot_commission_bips >= 0"
        ),
    )

    # Operational Metrics
    slashing_event_count = Column(
        Integer,
        CheckConstraint("slashing_event_count >= 0"),
        default=0,
    )
    lifetime_slashing_amount = Column(
        Numeric(20, 6),
        CheckConstraint("lifetime_slashing_amount >= 0"),
        default=0,
    )
    operational_days = Column(Integer, CheckConstraint("operational_days >= 0"))

    # Status Flags
    is_active = Column(Boolean, default=True)
    has_sufficient_data = Column(Boolean, default=False)

    # Metadata
    calculated_at = Column(DateTime, default=datetime.now)
    calculation_duration_ms = Column(
        Integer,
        CheckConstraint(
            "calculation_duration_ms IS NULL OR calculation_duration_ms >= 0"
        ),
    )

    __table_args__ = (
        PrimaryKeyConstraint("operator_id", "date"),
        Index("idx_operator_analytics_date_risk", "date", "risk_score"),
        Index("idx_operator_analytics_date_active", "date", "is_active"),
    )


class VolatilityMetrics(Base):
    """Supporting table for detailed volatility calculations"""

    __tablename__ = "volatility_metrics"

    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(50), nullable=False)
    date = Column(Date, nullable=False)
    metric_type = Column(String(20), nullable=False)

    volatility_7d = Column(Numeric(8, 4))
    volatility_30d = Column(Numeric(8, 4))
    volatility_90d = Column(Numeric(8, 4))

    mean_value = Column(Numeric(20, 6))
    coefficient_of_variation = Column(Numeric(8, 4))
    trend_direction = Column(Numeric(8, 4))
    trend_strength = Column(Numeric(6, 4))

    data_points_count = Column(Integer, CheckConstraint("data_points_count >= 0"))
    confidence_score = Column(
        Numeric(5, 2),
        CheckConstraint(
            "confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 100)"
        ),
    )

    __table_args__ = (
        PrimaryKeyConstraint("entity_type", "entity_id", "date", "metric_type"),
    )


class ConcentrationMetrics(Base):
    """Supporting table for concentration calculations"""

    __tablename__ = "concentration_metrics"

    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(50), nullable=False)
    date = Column(Date, nullable=False)
    concentration_type = Column(String(20), nullable=False)

    hhi_value = Column(
        Numeric(6, 4),
        CheckConstraint("hhi_value >= 0 AND hhi_value <= 1"),
    )
    gini_coefficient = Column(
        Numeric(6, 4),
        CheckConstraint(
            "gini_coefficient IS NULL OR (gini_coefficient >= 0 AND gini_coefficient <= 1)"
        ),
    )
    top_1_percentage = Column(
        Numeric(5, 2),
        CheckConstraint(
            "top_1_percentage IS NULL OR (top_1_percentage >= 0 AND top_1_percentage <= 100)"
        ),
    )
    top_5_percentage = Column(
        Numeric(5, 2),
        CheckConstraint(
            "top_5_percentage IS NULL OR (top_5_percentage >= 0 AND top_5_percentage <= 100)"
        ),
    )
    top_10_percentage = Column(
        Numeric(5, 2),
        CheckConstraint(
            "top_10_percentage IS NULL OR (top_10_percentage >= 0 AND top_10_percentage <= 100)"
        ),
    )

    total_entities = Column(Integer, CheckConstraint("total_entities >= 0"))
    effective_entities = Column(
        Numeric(8, 2),
        CheckConstraint("effective_entities IS NULL OR effective_entities >= 0"),
    )

    __table_args__ = (
        PrimaryKeyConstraint("entity_type", "entity_id", "date", "concentration_type"),
    )
