# CORE OPERATOR TABLES
from datetime import datetime
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Column,
    Date,
    ForeignKey,
    String,
    Numeric,
    Integer,
    Boolean,
    DateTime,
    Index,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from .base import Base, TimestampMixin


class PipelineCheckpoint(Base, TimestampMixin):
    __tablename__ = "pipeline_checkpoints"

    pipeline_name = Column(String(100), primary_key=True)
    last_processed_at = Column(DateTime, nullable=False)
    last_processed_block = Column(BigInteger)
    operators_processed_count = Column(Integer)
    total_events_processed = Column(Integer)
    run_duration_seconds = Column(Integer)
    run_metadata = Column(JSONB)


class OperatorState(Base, TimestampMixin):
    __tablename__ = "operator_state"

    # Primary Key
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    # Identity & Metadata
    operator_address = Column(String, nullable=False, unique=True)
    current_metadata_uri = Column(String)

    # Registration Info
    registered_at = Column(DateTime)  # From operator_registered_events (if available)
    registration_block = Column(Integer)
    first_activity_at = Column(DateTime, nullable=True)  # Fallback: earliest event
    first_activity_block = Column(Integer, nullable=True)
    first_activity_type = Column(String)  # "REGISTRATION", "ALLOCATION", etc.

    # Delegation Configuration
    current_delegation_approver = Column(String, nullable=False)
    is_permissioned = Column(Boolean, default=False)
    delegation_approver_updated_at = Column(DateTime)

    # Commission - PI only (others in commission_rates table)
    current_pi_split_bips = Column(Integer, default=0)
    pi_split_activated_at = Column(DateTime)

    # Counts
    active_avs_count = Column(Integer, default=0)
    active_operator_set_count = Column(Integer, default=0)
    registered_avs_count = Column(Integer, default=0)
    total_delegators = Column(Integer, default=0)
    active_delegators = Column(Integer, default=0)

    # Performance & Safety
    total_slash_events = Column(Integer, default=0)
    last_slashed_at = Column(DateTime)
    force_undelegation_count = Column(Integer, default=0)

    # Activity Timestamps
    last_allocation_at = Column(DateTime)
    last_commission_change_at = Column(DateTime)
    last_metadata_update_at = Column(DateTime)
    last_activity_at = Column(DateTime)

    # Operational Status
    is_active = Column(Boolean, default=True)
    operational_days = Column(Integer, default=0)

    __table_args__ = (
        Index("idx_operator_active", "is_active"),
        Index("idx_operator_first_activity", "first_activity_at"),
        Index("idx_operator_slashes", "total_slash_events"),
    )


class OperatorStrategyState(Base, TimestampMixin):
    __tablename__ = "operator_strategy_state"

    id = Column(String, primary_key=True)  # operator_id-strategy_id composite
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id = Column(
        String,
        ForeignKey("strategies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # TVS (Max Magnitude)
    max_magnitude = Column(Numeric, nullable=False, default=0)
    max_magnitude_updated_at = Column(DateTime)
    max_magnitude_updated_block = Column(Integer)

    # Encumbered (Allocated) Magnitude
    encumbered_magnitude = Column(Numeric, nullable=False, default=0)
    encumbered_magnitude_updated_at = Column(DateTime)
    encumbered_magnitude_updated_block = Column(Integer)

    # Utilization Rate (per strategy)
    utilization_rate = Column(Numeric, default=0)  # (encumbered/max) * 100

    __table_args__ = (
        Index("idx_operator_strategy_operator", "operator_id"),
        Index("idx_operator_strategy_max_mag", "max_magnitude"),
        Index("idx_operator_strategy_utilization", "utilization_rate"),
    )


class OperatorCommissionRate(Base, TimestampMixin):
    __tablename__ = "operator_commission_rates"

    id = Column(String, primary_key=True)  # operator_id-type-target_id composite
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Commission Type & Target
    commission_type = Column(String, nullable=False)  # 'PI', 'AVS', 'OPERATOR_SET'
    avs_id = Column(String, ForeignKey("avs.id", ondelete="SET NULL"))
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="SET NULL")
    )

    # Current Commission
    current_bips = Column(Integer, nullable=False)
    current_activated_at = Column(DateTime, nullable=False)
    current_set_at_block = Column(Integer, nullable=False)

    # Upcoming Commission (if scheduled)
    upcoming_bips = Column(Integer)
    upcoming_activated_at = Column(DateTime)
    upcoming_set_at_block = Column(Integer)

    # Historical Stats
    previous_bips = Column(Integer)
    total_changes = Column(Integer, default=1)
    first_set_at = Column(DateTime)

    __table_args__ = (
        Index("idx_commission_operator", "operator_id"),
        Index("idx_commission_type", "commission_type"),
        Index("idx_commission_avs", "operator_id", "avs_id"),
        Index("idx_commission_bips", "current_bips"),
    )


class OperatorAllocation(Base, TimestampMixin):
    __tablename__ = "operator_allocations"

    id = Column(String, primary_key=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    operator_set_id = Column(
        String,
        ForeignKey("operator_sets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id = Column(
        String,
        ForeignKey("strategies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    magnitude = Column(Numeric, nullable=False)
    effect_block = Column(Integer, nullable=False)
    allocated_at = Column(DateTime, nullable=False)
    allocated_at_block = Column(Integer, nullable=False)

    __table_args__ = (
        Index("idx_allocation_operator", "operator_id"),
        Index("idx_allocation_operator_avs", "operator_id", "operator_set_id"),
        Index("idx_allocation_effect", "effect_block"),
    )


class OperatorAVSRegistrationHistory(Base, TimestampMixin):
    __tablename__ = "operator_avs_registration_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    avs_id = Column(
        String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Registration Event
    status = Column(String, nullable=False)  # 'REGISTERED', 'UNREGISTERED'
    status_changed_at = Column(DateTime, nullable=False, index=True)
    status_changed_block = Column(Integer, nullable=False)

    # Event reference
    event_id = Column(String)
    transaction_hash = Column(String)

    __table_args__ = (
        Index("idx_avs_reg_hist_operator", "operator_id", "status_changed_at"),
        Index("idx_avs_reg_hist_avs", "avs_id", "status_changed_at"),
        Index("idx_avs_reg_hist_status", "status"),
        Index(
            "idx_avs_reg_hist_period",
            "operator_id",
            "avs_id",
        ),
    )


class OperatorAVSRelationship(Base, TimestampMixin):
    __tablename__ = "operator_avs_relationships"

    id = Column(String, primary_key=True)  # operator_id-avs_id
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    avs_id = Column(
        String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Current Registration Status
    current_status = Column(
        String, nullable=False, index=True
    )  # 'REGISTERED', 'UNREGISTERED'
    current_status_since = Column(DateTime, nullable=False)

    # Registration History Summary
    first_registered_at = Column(DateTime)
    last_registered_at = Column(DateTime)
    last_unregistered_at = Column(DateTime)
    total_registration_cycles = Column(Integer, default=0)  # Number of times registered
    # TODO: Look into this to ensure it is calculated properly
    total_days_registered = Column(Integer, default=0)  # Cumulative days

    # Current Period (if registered)
    current_period_start = Column(DateTime)
    current_period_days = Column(Integer, default=0)

    # Operator Set Participation
    active_operator_set_ids = Column(JSONB, default=[])  # ["set_id1", "set_id2"]
    active_operator_set_count = Column(Integer, default=0)

    # Current Commission (cached)
    avs_commission_bips = Column(Integer)

    # Activity
    last_activity_at = Column(DateTime)

    __table_args__ = (
        Index("idx_avs_rel_status", "current_status"),
        Index("idx_avs_rel_operator_status", "operator_id", "current_status"),
        Index("idx_avs_rel_cycles", "total_registration_cycles"),
    )


class OperatorAVSAllocationSummary(Base, TimestampMixin):
    __tablename__ = "operator_avs_allocation_summary"

    id = Column(String, primary_key=True)  # operator_id-avs_id-strategy_id
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    avs_id = Column(
        String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id = Column(
        String,
        ForeignKey("strategies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Allocation across all operator sets for this AVS-Strategy combo
    total_allocated_magnitude = Column(Numeric, default=0)
    active_allocation_count = Column(Integer, default=0)  # Number of operator sets

    # Latest allocation timestamp
    last_allocated_at = Column(DateTime)

    __table_args__ = (
        Index("idx_avs_alloc_summary_operator", "operator_id"),
        Index("idx_avs_alloc_summary_avs", "avs_id"),
        Index("idx_avs_alloc_summary_magnitude", "total_allocated_magnitude"),
    )


class OperatorDelegatorHistory(Base, TimestampMixin):
    __tablename__ = "operator_delegator_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Delegation Event
    delegation_type = Column(
        String, nullable=False
    )  # 'DELEGATED', 'UNDELEGATED', 'FORCE_UNDELEGATED'
    event_timestamp = Column(DateTime, nullable=False, index=True)
    event_block = Column(Integer, nullable=False)

    # Event reference
    transaction_hash = Column(String)

    __table_args__ = (
        Index("idx_delegator_hist_operator", "operator_id", "event_timestamp"),
        Index("idx_delegator_hist_staker", "staker_id", "event_timestamp"),
        Index("idx_delegator_hist_type", "delegation_type"),
    )


class OperatorDelegator(Base, TimestampMixin):
    __tablename__ = "operator_delegators"

    id = Column(String, primary_key=True)  # operator_id-staker_id
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Current Status
    is_delegated = Column(Boolean, default=True, index=True)
    delegated_at = Column(DateTime, nullable=True)
    undelegated_at = Column(DateTime)

    __table_args__ = (
        Index("idx_delegator_operator_status", "operator_id", "is_delegated"),
    )


class OperatorDelegatorShares(Base, TimestampMixin):
    __tablename__ = "operator_delegator_shares"

    id = Column(String, primary_key=True)  # operator_id-staker_id-strategy_id
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id = Column(
        String,
        ForeignKey("strategies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Share Amount
    shares = Column(Numeric, nullable=False, default=0)
    shares_updated_at = Column(DateTime)
    shares_updated_block = Column(Integer)

    __table_args__ = (
        Index("idx_delegator_shares_operator", "operator_id"),
        Index("idx_delegator_shares_staker", "staker_id"),
        Index("idx_delegator_shares_amount", "shares"),
    )


class OperatorSlashingIncident(Base, TimestampMixin):
    __tablename__ = "operator_slashing_incidents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )

    # Slashing Details
    slashed_at = Column(DateTime, nullable=False, index=True)
    slashed_at_block = Column(Integer, nullable=False)
    description = Column(String, nullable=False)

    # Event Reference
    event_id = Column(Integer)
    transaction_hash = Column(String)

    __table_args__ = (
        Index("idx_slash_operator_date", "operator_id", "slashed_at"),
        Index("idx_slash_avs", "operator_set_id"),
    )


class OperatorSlashingAmount(Base, TimestampMixin):
    __tablename__ = "operator_slashing_amounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    slashing_incident_id = Column(
        Integer,
        ForeignKey("operator_slashing_incidents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id = Column(
        String,
        ForeignKey("strategies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Slashed Amount
    wad_slashed = Column(Numeric, nullable=False)

    __table_args__ = (
        Index("idx_slash_amount_operator", "operator_id"),
        Index("idx_slash_amount_strategy", "strategy_id"),
        Index("idx_slash_amount_incident", "slashing_incident_id"),
    )


class OperatorRegistration(Base, TimestampMixin):
    """Current operator registration information"""

    __tablename__ = "operator_registration"

    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    # Registration details
    registered_at = Column(DateTime, nullable=False)
    registration_block = Column(Integer, nullable=False)
    delegation_approver = Column(String, nullable=False)

    # Event reference
    transaction_hash = Column(String, nullable=False)

    __table_args__ = (
        Index("idx_operator_registration_date", "registered_at"),
        Index("idx_operator_registration_block", "registration_block"),
    )


class OperatorDelegationApproverHistory(Base, TimestampMixin):
    """History of delegation approver changes"""

    __tablename__ = "operator_delegation_approver_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Change details
    old_delegation_approver = Column(String)  # NULL for first registration
    new_delegation_approver = Column(String, nullable=False)
    changed_at = Column(DateTime, nullable=False, index=True)
    changed_at_block = Column(Integer, nullable=False)

    # Event reference
    transaction_hash = Column(String, nullable=False)

    __table_args__ = (
        Index("idx_delegation_approver_hist_operator", "operator_id", "changed_at"),
    )


class OperatorMetadata(Base, TimestampMixin):
    """Current operator metadata"""

    __tablename__ = "operator_metadata"

    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    # Current metadata
    metadata_uri = Column(String, nullable=False)
    metadata_json = Column(JSONB)  # Fetched metadata content
    metadata_fetched_at = Column(DateTime)

    # Update tracking
    last_updated_at = Column(DateTime, nullable=False)
    last_updated_block = Column(Integer, nullable=False)
    total_updates = Column(Integer, default=1)

    __table_args__ = (Index("idx_operator_metadata_updated", "last_updated_at"),)


class OperatorMetadataHistory(Base, TimestampMixin):
    """History of metadata updates"""

    __tablename__ = "operator_metadata_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Metadata details
    metadata_uri = Column(String, nullable=False)
    metadata_json = Column(JSONB)  # Snapshot of fetched metadata at this time
    metadata_fetched_at = Column(DateTime)

    # Update details
    updated_at = Column(DateTime, nullable=False, index=True)
    updated_at_block = Column(Integer, nullable=False)

    # Event reference
    transaction_hash = Column(String, nullable=False)

    __table_args__ = (
        Index("idx_metadata_history_operator", "operator_id", "updated_at"),
        Index("idx_metadata_history_block", "updated_at_block"),
    )


# TIME-SERIES TABLES
class OperatorDailySnapshot(Base):
    __tablename__ = "operator_daily_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    snapshot_date = Column(Date, nullable=False, index=True)
    snapshot_block = Column(Integer, nullable=False)

    # Counts
    delegator_count = Column(Integer, default=0)
    active_avs_count = Column(Integer, default=0)
    active_operator_set_count = Column(Integer, default=0)

    # Commission (PI only, cached)
    pi_split_bips = Column(Integer)

    # Performance
    slash_event_count_to_date = Column(Integer, default=0)

    # Operational
    operational_days = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)

    __table_args__ = (
        Index("idx_snapshot_operator_date", "operator_id", "snapshot_date"),
        Index("idx_snapshot_date", "snapshot_date"),
        UniqueConstraint(
            "operator_id",
            "snapshot_date",
            name="operator_daily_snapshots_operator_date_key",
        ),
    )


class OperatorStrategyDailySnapshot(Base):
    __tablename__ = "operator_strategy_daily_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id = Column(
        String,
        ForeignKey("strategies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    snapshot_date = Column(Date, nullable=False, index=True)
    snapshot_block = Column(Integer, nullable=False)

    # TVS
    max_magnitude = Column(Numeric, nullable=False)

    # Allocation
    encumbered_magnitude = Column(Numeric, default=0)
    utilization_rate = Column(Numeric, default=0)

    __table_args__ = (
        Index("idx_strategy_snapshot_operator_date", "operator_id", "snapshot_date"),
        Index("idx_strategy_snapshot_strategy_date", "strategy_id", "snapshot_date"),
        Index("idx_strategy_snapshot_magnitude", "max_magnitude"),
        UniqueConstraint(
            "operator_id",
            "strategy_id",
            "snapshot_date",
            name="operator_strategy_daily_snapshots_operator_strategy_date_key",
        ),
    )


class OperatorAVSRelationshipSnapshot(Base):
    """Daily snapshots of operator-AVS relationships"""

    __tablename__ = "operator_avs_relationship_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    avs_id = Column(
        String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    snapshot_date = Column(Date, nullable=False, index=True)
    snapshot_block = Column(Integer, nullable=False)

    # Snapshot of relationship state
    current_status = Column(String, nullable=False)  # 'REGISTERED' or 'UNREGISTERED'
    days_registered_to_date = Column(Integer, default=0)
    current_period_days = Column(Integer, default=0)
    total_registration_cycles = Column(Integer, default=0)
    active_operator_set_count = Column(Integer, default=0)
    avs_commission_bips = Column(Integer)

    __table_args__ = (
        Index("idx_avs_rel_snap_operator_date", "operator_id", "snapshot_date"),
        Index("idx_avs_rel_snap_avs_date", "avs_id", "snapshot_date"),
        Index("idx_avs_rel_snap_status", "current_status"),
        UniqueConstraint(
            "operator_id",
            "avs_id",
            "snapshot_date",
            name="operator_avs_relationship_snapshots_unique",
        ),
    )


class OperatorDelegatorSharesSnapshot(Base):
    """Daily snapshots of delegator shares per operator-strategy"""

    __tablename__ = "operator_delegator_shares_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id = Column(
        String,
        ForeignKey("strategies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    snapshot_date = Column(Date, nullable=False, index=True)
    snapshot_block = Column(Integer, nullable=False)

    # Snapshot of shares
    shares = Column(Numeric, nullable=False, default=0)
    is_delegated = Column(Boolean, nullable=False, default=True)

    __table_args__ = (
        Index(
            "idx_delegator_shares_snap_operator_date", "operator_id", "snapshot_date"
        ),
        Index("idx_delegator_shares_snap_staker_date", "staker_id", "snapshot_date"),
        Index(
            "idx_delegator_shares_snap_strategy_date", "strategy_id", "snapshot_date"
        ),
        Index("idx_delegator_shares_snap_delegated", "is_delegated"),
        UniqueConstraint(
            "operator_id",
            "staker_id",
            "strategy_id",
            "snapshot_date",
            name="operator_delegator_shares_snapshots_unique",
        ),
    )


class OperatorCommissionRatesSnapshot(Base):
    """Daily snapshots of commission rates"""

    __tablename__ = "operator_commission_rates_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    commission_type = Column(
        String, nullable=False, index=True
    )  # 'PI', 'AVS', 'OPERATOR_SET'
    avs_id = Column(String, ForeignKey("avs.id", ondelete="SET NULL"))
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="SET NULL")
    )
    snapshot_date = Column(Date, nullable=False, index=True)
    snapshot_block = Column(Integer, nullable=False)

    # Snapshot of commission
    current_bips = Column(Integer, nullable=False)

    __table_args__ = (
        Index("idx_commission_snap_operator_date", "operator_id", "snapshot_date"),
        Index("idx_commission_snap_type", "commission_type"),
        Index("idx_commission_snap_avs", "avs_id", "snapshot_date"),
        UniqueConstraint(
            "operator_id",
            "commission_type",
            "snapshot_date",
            # Note: Including NULLable columns in unique constraint
            # This allows multiple NULL values but enforces uniqueness when not NULL
            name="operator_commission_rates_snapshots_unique",
        ),
    )


class OperatorAllocationSnapshot(Base):
    """Daily snapshots of operator allocations per operator-set-strategy"""

    __tablename__ = "operator_allocation_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    operator_set_id = Column(
        String,
        ForeignKey("operator_sets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id = Column(
        String,
        ForeignKey("strategies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    snapshot_date = Column(Date, nullable=False, index=True)
    snapshot_block = Column(Integer, nullable=False)

    # Allocation magnitude at snapshot time
    magnitude = Column(Numeric, nullable=False, default=0)

    __table_args__ = (
        Index("idx_allocation_snap_operator_date", "operator_id", "snapshot_date"),
        Index(
            "idx_allocation_snap_operator_set_date", "operator_set_id", "snapshot_date"
        ),
        Index("idx_allocation_snap_strategy_date", "strategy_id", "snapshot_date"),
        UniqueConstraint(
            "operator_id",
            "operator_set_id",
            "strategy_id",
            "snapshot_date",
            name="operator_allocation_snapshots_unique",
        ),
    )


class NetworkDailyAggregates(Base):
    """Daily network-wide aggregate statistics for percentile calculations"""

    __tablename__ = "network_daily_aggregates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_date = Column(Date, nullable=False, unique=True, index=True)
    snapshot_block = Column(Integer, nullable=False)

    # Operator counts
    total_operators = Column(Integer, default=0)
    active_operators = Column(Integer, default=0)

    # TVS aggregates
    total_tvs = Column(Numeric, default=0)
    mean_tvs = Column(Numeric, default=0)
    median_tvs = Column(Numeric, default=0)
    p25_tvs = Column(Numeric, default=0)
    p75_tvs = Column(Numeric, default=0)
    p90_tvs = Column(Numeric, default=0)

    # Delegator aggregates
    total_delegators = Column(Integer, default=0)
    mean_delegators_per_operator = Column(Numeric, default=0)
    median_delegators_per_operator = Column(Numeric, default=0)

    # AVS aggregates
    mean_avs_per_operator = Column(Numeric, default=0)
    median_avs_per_operator = Column(Numeric, default=0)

    # Commission aggregates
    mean_pi_commission_bips = Column(Numeric, default=0)
    median_pi_commission_bips = Column(Numeric, default=0)

    __table_args__ = (Index("idx_network_agg_date", "snapshot_date"),)


class OperatorCommissionHistory(Base):
    __tablename__ = "operator_commission_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Commission Target
    commission_type = Column(String, nullable=False, index=True)
    avs_id = Column(String, ForeignKey("avs.id", ondelete="SET NULL"))
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="SET NULL")
    )

    # Change Details
    old_bips = Column(Integer, nullable=False)
    new_bips = Column(Integer, nullable=False)
    change_delta = Column(Integer, nullable=False)

    # Timestamps
    changed_at = Column(DateTime, nullable=False, index=True)
    activated_at = Column(DateTime, nullable=False)
    activation_delay_seconds = Column(Integer)

    # Event Context
    event_id = Column(String)
    caller = Column(String)
    block_number = Column(Integer)

    __table_args__ = (
        Index("idx_commission_hist_operator", "operator_id", "changed_at"),
        Index("idx_commission_hist_type", "commission_type"),
    )


class OperatorAnalytics(Base):
    __tablename__ = "operator_analytics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(
        String,
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    date = Column(Date, nullable=False, index=True)

    # Risk Assessment Scores
    risk_score = Column(Numeric, nullable=False)
    confidence_score = Column(Numeric, nullable=False)
    risk_level = Column(String(10), nullable=False, index=True)

    # Component Scores
    performance_score = Column(Numeric)
    economic_score = Column(Numeric)
    network_position_score = Column(Numeric)

    # Calculated Metrics
    delegation_hhi = Column(Numeric)
    delegation_volatility_7d = Column(Numeric)
    delegation_volatility_30d = Column(Numeric)
    delegation_volatility_90d = Column(Numeric)
    growth_rate_30d = Column(Numeric)
    growth_consistency_score = Column(Numeric)
    size_percentile = Column(Numeric)
    delegator_distribution_cv = Column(Numeric)

    # Snapshot Metrics
    snapshot_total_delegated_shares = Column(Numeric, default=0)
    snapshot_delegator_count = Column(Integer, default=0)
    snapshot_avs_count = Column(Integer, default=0)
    snapshot_commission_bips = Column(Numeric)

    # Operational Metrics
    slashing_event_count = Column(Integer, default=0)
    lifetime_slashing_amount = Column(Numeric, default=0)
    operational_days = Column(Integer)

    # Status Flags
    is_active = Column(Boolean, default=True)
    has_sufficient_data = Column(Boolean, default=False)

    # Metadata
    calculated_at = Column(DateTime, default=datetime.now)
    calculation_duration_ms = Column(Integer)

    __table_args__ = (
        Index("idx_operator_analytics_date_risk", "date", "risk_score"),
        UniqueConstraint(
            "operator_id", "date", name="uq_operator_analytics_operator_date"
        ),
    )


class VolatilityMetrics(Base):
    __tablename__ = "volatility_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_type = Column(String, nullable=False, index=True)
    entity_id = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    metric_type = Column(String, nullable=False, index=True)

    volatility_7d = Column(Numeric)
    volatility_30d = Column(Numeric)
    volatility_90d = Column(Numeric)

    mean_value = Column(Numeric)
    coefficient_of_variation = Column(Numeric)
    trend_direction = Column(Numeric)
    trend_strength = Column(Numeric)

    data_points_count = Column(Integer)
    confidence_score = Column(Numeric)

    operator_id = Column(String, ForeignKey("operators.id", ondelete="CASCADE"))

    __table_args__ = (
        UniqueConstraint(
            "entity_type",
            "entity_id",
            "date",
            "metric_type",
            name="uix_volatility_metrics",
        ),
        Index("idx_volatility_entity", "entity_type", "entity_id", "date"),
    )


class ConcentrationMetrics(Base):
    __tablename__ = "concentration_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_type = Column(String, nullable=False, index=True)
    entity_id = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    concentration_type = Column(String, nullable=False, index=True)

    hhi_value = Column(Numeric)
    gini_coefficient = Column(Numeric)
    coefficient_of_variation = Column(Numeric)

    top_1_percentage = Column(Numeric)
    top_5_percentage = Column(Numeric)
    top_10_percentage = Column(Numeric)

    total_entities = Column(Integer)
    effective_entities = Column(Numeric)
    total_amount = Column(Numeric)

    operator_id = Column(String, ForeignKey("operators.id", ondelete="CASCADE"))

    __table_args__ = (
        UniqueConstraint(
            "entity_type",
            "entity_id",
            "date",
            "concentration_type",
            name="uix_concentration_metrics",
        ),
        Index("idx_concentration_entity", "entity_type", "entity_id", "date"),
    )


# MINIMAL FOREIGN KEY TABLES
class Operator(Base, TimestampMixin):
    __tablename__ = "operators"

    id = Column(String, primary_key=True)
    address = Column(String, nullable=False, unique=True)


class AVS(Base, TimestampMixin):
    __tablename__ = "avs"

    id = Column(String, primary_key=True)
    address = Column(String, nullable=False, unique=True)


class Staker(Base, TimestampMixin):
    __tablename__ = "stakers"

    id = Column(String, primary_key=True)
    address = Column(String, nullable=False, unique=True)


class Strategy(Base, TimestampMixin):
    __tablename__ = "strategies"

    id = Column(String, primary_key=True)
    address = Column(String, nullable=False, unique=True)


class OperatorSet(Base, TimestampMixin):
    __tablename__ = "operator_sets"

    id = Column(String, primary_key=True)
    avs_id = Column(
        String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    operator_set_id = Column(Integer, nullable=False)

    __table_args__ = (Index("idx_operator_set_avs", "avs_id"),)


# ========================================
# CACHE TABLE SCHEMA (add to models.py)
# ========================================


class SlashingEventsCache(Base, TimestampMixin):
    """Temporary cache of slashing events from events DB"""

    __tablename__ = "slashing_events_cache"

    operator_id = Column(String, nullable=False, primary_key=True)
    block_number = Column(Integer, nullable=False, primary_key=True)
    transaction_hash = Column(String, nullable=False, primary_key=True)

    operator_set_id = Column(String, nullable=False)
    slashed_at = Column(DateTime, nullable=False)
    description = Column(String)
    strategies = Column(ARRAY(String), nullable=False)
    wad_slashed = Column(ARRAY(Numeric), nullable=False)

    __table_args__ = (Index("idx_slashing_cache_operator", "operator_id"),)
