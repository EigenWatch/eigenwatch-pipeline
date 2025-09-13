import datetime
from sqlmodel import SQLModel, Field
from typing import Optional


# SQLModel schema definitions
class OperatorAnalytics(SQLModel, table=True):
    """SQLModel for operator analytics table"""

    __tablename__ = "operator_analytics"

    # Primary Keys
    operator_id: str = Field(primary_key=True, max_length=50)
    date: datetime.date = Field(primary_key=True)

    # Risk Assessment Scores
    risk_score: float = Field(ge=0, le=100, description="Composite risk score 0-100")
    confidence_score: float = Field(
        ge=0, le=100, description="Confidence in risk assessment"
    )
    risk_level: str = Field(max_length=10, description="LOW, MEDIUM, HIGH, CRITICAL")

    # Component Scores
    performance_score: float = Field(
        ge=0, le=100, description="Performance component (50% weight)"
    )
    economic_score: float = Field(
        ge=0, le=100, description="Economic component (35% weight)"
    )
    network_position_score: float = Field(
        ge=0, le=100, description="Network position (15% weight)"
    )

    # Calculated Metrics
    delegation_hhi: float = Field(ge=0, le=1, description="Delegator concentration HHI")
    delegation_volatility_7d: Optional[float] = Field(
        default=None, ge=0, description="7-day volatility"
    )
    delegation_volatility_30d: Optional[float] = Field(
        default=None, ge=0, description="30-day volatility"
    )
    delegation_volatility_90d: Optional[float] = Field(
        default=None, ge=0, description="90-day volatility"
    )
    growth_rate_30d: Optional[float] = Field(
        default=None, description="30-day growth rate"
    )
    growth_consistency_score: Optional[float] = Field(default=None, ge=0, le=100)
    size_percentile: Optional[float] = Field(default=None, ge=0, le=100)
    delegator_distribution_cv: Optional[float] = Field(default=None, ge=0)

    # Snapshot Metrics (point-in-time values)
    snapshot_total_delegated_eth: Optional[float] = Field(default=0, ge=0)
    snapshot_delegator_count: int = Field(default=0, ge=0)
    snapshot_avs_count: int = Field(default=0, ge=0)
    snapshot_commission_rate: Optional[float] = Field(default=None, ge=0)

    # Operational Metrics
    slashing_event_count: int = Field(default=0, ge=0)
    lifetime_slashing_amount: Optional[float] = Field(default=0, ge=0)
    operational_days: int = Field(ge=0)

    # Status Flags
    is_active: bool = Field(default=True)
    has_sufficient_data: bool = Field(default=False)

    # Metadata
    calculated_at: datetime = Field(default_factory=datetime.now)
    calculation_duration_ms: Optional[int] = Field(default=None, ge=0)


class VolatilityMetrics(SQLModel, table=True):
    """Supporting table for detailed volatility calculations"""

    __tablename__ = "volatility_metrics"

    entity_type: str = Field(primary_key=True, max_length=20)
    entity_id: str = Field(primary_key=True, max_length=50)
    date: datetime.date = Field(primary_key=True)
    metric_type: str = Field(primary_key=True, max_length=20)

    volatility_7d: Optional[float] = Field(default=None)
    volatility_30d: Optional[float] = Field(default=None)
    volatility_90d: Optional[float] = Field(default=None)

    mean_value: Optional[float] = Field(default=None)
    coefficient_of_variation: Optional[float] = Field(default=None)
    trend_direction: Optional[float] = Field(default=None)
    trend_strength: Optional[float] = Field(default=None)

    data_points_count: int = Field(ge=0)
    confidence_score: Optional[float] = Field(default=None, ge=0, le=100)


class ConcentrationMetrics(SQLModel, table=True):
    """Supporting table for concentration calculations"""

    __tablename__ = "concentration_metrics"

    entity_type: str = Field(primary_key=True, max_length=20)
    entity_id: str = Field(primary_key=True, max_length=50)
    date: datetime.date = Field(primary_key=True)
    concentration_type: str = Field(primary_key=True, max_length=20)

    hhi_value: float = Field(ge=0, le=1)
    gini_coefficient: Optional[float] = Field(default=None, ge=0, le=1)
    top_1_percentage: Optional[float] = Field(default=None, ge=0, le=100)
    top_5_percentage: Optional[float] = Field(default=None, ge=0, le=100)
    top_10_percentage: Optional[float] = Field(default=None, ge=0, le=100)

    total_entities: int = Field(ge=0)
    effective_entities: Optional[float] = Field(default=None, ge=0)
