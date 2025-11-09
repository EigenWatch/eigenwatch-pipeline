from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Integer,
    Numeric,
    Date,
    DateTime,
    Boolean,
    Index,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OperatorAnalytics(Base):
    """SQLAlchemy model for operator analytics table"""

    __tablename__ = "operator_analytics"

    # Primary Key
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    # Existing Composite Key as Unique Constraint
    operator_id = Column(String(50), nullable=False)
    date = Column(Date, nullable=False)

    # Risk Assessment Scores
    risk_score = Column(Numeric(5, 2), nullable=False)
    confidence_score = Column(Numeric(5, 2), nullable=False)
    risk_level = Column(String(10), nullable=False)

    # Component Scores
    performance_score = Column(Numeric(5, 2))
    economic_score = Column(Numeric(5, 2))
    network_position_score = Column(Numeric(5, 2))

    # Calculated Metrics
    delegation_hhi = Column(Numeric(6, 4))
    delegation_volatility_7d = Column(Numeric(8, 4))
    delegation_volatility_30d = Column(Numeric(8, 4))
    delegation_volatility_90d = Column(Numeric(8, 4))
    growth_rate_30d = Column(Numeric(8, 4))
    growth_consistency_score = Column(Numeric(5, 2))
    size_percentile = Column(Numeric(5, 2))
    delegator_distribution_cv = Column(Numeric(8, 4))

    # Snapshot Metrics (point-in-time values)
    snapshot_total_delegated_shares = Column(Numeric, default=0)
    snapshot_delegator_count = Column(Integer, default=0)
    snapshot_avs_count = Column(Integer, default=0)
    snapshot_commission_bips = Column(Numeric(8, 2))

    # Operational Metrics
    slashing_event_count = Column(Integer, default=0)
    lifetime_slashing_amount = Column(Numeric(20, 6), default=0)
    operational_days = Column(Integer)

    # Status Flags
    is_active = Column(Boolean, default=True)
    has_sufficient_data = Column(Boolean, default=False)

    # Metadata
    calculated_at = Column(DateTime, default=datetime.now)
    calculation_duration_ms = Column(Integer)

    __table_args__ = (
        # UniqueConstraint("operator_id", "date", name="uix_operator_analytics"),
        Index("idx_operator_analytics_date_risk", "date", "risk_score"),
        Index("idx_operator_analytics_date_active", "date", "is_active"),
    )


class VolatilityMetrics(Base):
    """Supporting table for detailed volatility calculations"""

    __tablename__ = "volatility_metrics"

    # Primary Key
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    # Existing Composite Key as Unique Constraint
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

    data_points_count = Column(Integer)
    confidence_score = Column(Numeric(5, 2))

    __table_args__ = (
        UniqueConstraint(
            "entity_type",
            "entity_id",
            "date",
            "metric_type",
            name="uix_volatility_metrics",
        ),
    )


class ConcentrationMetrics(Base):
    """Supporting table for concentration calculations"""

    __tablename__ = "concentration_metrics"

    # Primary Key
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # Existing Composite Key as Unique Constraint
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(50), nullable=False)
    date = Column(Date, nullable=False)
    concentration_type = Column(String(20), nullable=False)

    # Concentration metrics
    hhi_value = Column(Numeric(6, 4))
    gini_coefficient = Column(Numeric(6, 4))
    coefficient_of_variation = Column(Numeric)  # Added back

    top_1_percentage = Column(Numeric(5, 2))
    top_5_percentage = Column(Numeric(5, 2))
    top_10_percentage = Column(Numeric(5, 2))

    total_entities = Column(Integer)
    effective_entities = Column(Numeric(8, 2))
    total_amount = Column(Numeric)  # Added back, numeric for large values

    # Operator field (address / identifier)
    operator_id = Column(String(50), nullable=True)  # Added back

    __table_args__ = (
        UniqueConstraint(
            "entity_type",
            "entity_id",
            "date",
            "concentration_type",
            name="uix_concentration_metrics",
        ),
    )
