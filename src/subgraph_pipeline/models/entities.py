# models/entities.py
from sqlalchemy import Column, String, BigInteger, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from .base import Base, TimestampMixin
import enum


# Enums (collected from the schema)
class ShareEventType(enum.Enum):
    INCREASED = "INCREASED"
    DECREASED = "DECREASED"


class DelegationType(enum.Enum):
    DELEGATED = "DELEGATED"
    UNDELEGATED = "UNDELEGATED"
    FORCE_UNDELEGATED = "FORCE_UNDELEGATED"


class WithdrawalEventType(enum.Enum):
    QUEUED = "QUEUED"
    COMPLETED = "COMPLETED"


class AVSRegistrationStatus(enum.Enum):
    REGISTERED = "REGISTERED"
    UNREGISTERED = "UNREGISTERED"


class StrategyOperatorSetEventType(enum.Enum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"


class RewardsSubmissionType(enum.Enum):
    AVS_REWARDS = "AVS_REWARDS"
    REWARDS_FOR_ALL = "REWARDS_FOR_ALL"
    REWARDS_FOR_ALL_EARNERS = "REWARDS_FOR_ALL_EARNERS"
    OPERATOR_DIRECTED_AVS = "OPERATOR_DIRECTED_AVS"
    OPERATOR_DIRECTED_OPERATOR_SET = "OPERATOR_DIRECTED_OPERATOR_SET"


class StrategyWhitelistEventType(enum.Enum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"


class PodSharesUpdateType(enum.Enum):
    SHARES_UPDATED = "SHARES_UPDATED"
    NEW_TOTAL_SHARES = "NEW_TOTAL_SHARES"


# Operator Table
# Purpose: Represents operators (addresses) in the system. Serves as a lookup entity for relating events.
# Relationships: One-to-many with various event tables (e.g., registration_events, share_events). No direct parent; referenced by events.
class Operator(Base, TimestampMixin):
    __tablename__ = "operators"
    id = Column(String, primary_key=True)  # operator address as string (hex)
    address = Column(String, nullable=False)

    registration_events = relationship("OperatorRegistered", back_populates="operator")
    share_events = relationship("OperatorShareEvent", back_populates="operator")
    slashing_events = relationship("OperatorSlashed", back_populates="operator")
    avs_registration_events = relationship(
        "OperatorAVSRegistrationStatusUpdated", back_populates="operator"
    )
    operator_set_join_events = relationship(
        "OperatorAddedToOperatorSet", back_populates="operator"
    )
    operator_set_leave_events = relationship(
        "OperatorRemovedFromOperatorSet", back_populates="operator"
    )
    allocation_events = relationship("AllocationEvent", back_populates="operator")
    metadata_update_events = relationship(
        "OperatorMetadataUpdate", back_populates="operator"
    )
    delegation_approver_updates = relationship(
        "DelegationApproverUpdated", back_populates="operator"
    )


# Staker Table
# Purpose: Represents stakers (addresses) in the system. Lookup for delegation and deposit-related events.
# Relationships: One-to-many with delegation, deposit, withdrawal, etc., events. Referenced by EigenPod as owner.
class Staker(Base, TimestampMixin):
    __tablename__ = "stakers"
    id = Column(String, primary_key=True)  # staker address as string (hex)
    address = Column(String, nullable=False)

    eigen_pods = relationship("EigenPod", back_populates="owner")
    delegation_events = relationship("StakerDelegationEvent", back_populates="staker")
    share_events = relationship("OperatorShareEvent", back_populates="staker")
    deposit_events = relationship("Deposit", back_populates="staker")
    withdrawal_events = relationship("WithdrawalEvent", back_populates="staker")
    pod_deployment_events = relationship("PodDeployed", back_populates="owner")
    beacon_chain_deposit_events = relationship(
        "BeaconChainDeposit", back_populates="pod_owner"
    )
    beacon_chain_withdrawal_events = relationship(
        "BeaconChainWithdrawal", back_populates="pod_owner"
    )
    pod_shares_update_events = relationship(
        "PodSharesUpdate", back_populates="pod_owner"
    )
    beacon_chain_slashing_events = relationship(
        "BeaconChainSlashingEvent", back_populates="staker"
    )
    force_undelegation_events = relationship(
        "StakerForceUndelegated", back_populates="staker"
    )
    deposit_scaling_events = relationship(
        "DepositScalingFactorUpdated", back_populates="staker"
    )


# AVS Table
# Purpose: Represents AVS (Actively Validated Services) addresses. Lookup for operator registrations and rewards.
# Relationships: One-to-many with operator registrations, rewards submissions, metadata updates, etc. Referenced by OperatorSet.
class AVS(Base, TimestampMixin):
    __tablename__ = "avs"
    id = Column(String, primary_key=True)  # avs address as string (hex)
    address = Column(String, nullable=False)
    operator_sets = relationship("OperatorSet", back_populates="avs")

    operator_registration_events = relationship(
        "OperatorAVSRegistrationStatusUpdated", back_populates="avs"
    )
    rewards_submission_events = relationship("RewardsSubmission", back_populates="avs")
    operator_directed_rewards_events = relationship(
        "OperatorDirectedAVSRewardsSubmission", back_populates="avs"
    )
    metadata_update_events = relationship("AVSMetadataUpdate", back_populates="avs")
    operator_set_creation_events = relationship(
        "OperatorSetCreated", back_populates="avs"
    )
    registrar_set_events = relationship("AVSRegistrarSet", back_populates="avs")


# Strategy Table
# Purpose: Represents strategy contract addresses. Lookup for deposits, shares, and allocations.
# Relationships: One-to-many with deposits, shares, allocations, whitelists, etc.
class Strategy(Base, TimestampMixin):
    __tablename__ = "strategies"
    id = Column(String, primary_key=True)  # strategy address as string (hex)
    address = Column(String, nullable=False)

    deposit_events = relationship(
        "Deposit", back_populates="strategy"
    )  # TODO: Look into a withdrawal event relationship here as well
    share_events = relationship("OperatorShareEvent", back_populates="strategy")
    allocation_events = relationship("AllocationEvent", back_populates="strategy")
    whitelist_events = relationship("StrategyWhitelistEvent", back_populates="strategy")
    strategy_operator_set_events = relationship(
        "StrategyOperatorSetEvent", back_populates="strategy"
    )


# OperatorSet Table
# Purpose: Represents operator sets identified by AVS and operatorSetId composite. Lookup for allocations and memberships.
# Relationships: Foreign key to AVS; one-to-many with creation, join/leave, allocation, slashing events.
class OperatorSet(Base, TimestampMixin):
    __tablename__ = "operator_sets"
    id = Column(String, primary_key=True)  # avs-operatorSetId composite as string
    avs_id = Column(String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False)
    operator_set_id = Column(BigInteger, nullable=False)

    avs = relationship(
        "AVS", back_populates="operator_sets"
    )  # Note: adjusted for relationships
    creation_event = relationship("OperatorSetCreated", back_populates="operator_set")
    member_join_events = relationship(
        "OperatorAddedToOperatorSet", back_populates="operator_set"
    )
    member_leave_events = relationship(
        "OperatorRemovedFromOperatorSet", back_populates="operator_set"
    )
    allocation_events = relationship("AllocationEvent", back_populates="operator_set")
    slashing_events = relationship("OperatorSlashed", back_populates="operator_set")
    strategy_events = relationship(
        "StrategyOperatorSetEvent", back_populates="operator_set"
    )
    redistribution_events = relationship(
        "RedistributionAddressSet", back_populates="operator_set"
    )
    operator_directed_rewards_events = relationship(
        "OperatorDirectedOperatorSetRewardsSubmission", back_populates="operator_set"
    )


# EigenPod Table
# Purpose: Represents EigenPods (addresses) owned by stakers. Lookup for beacon chain interactions.
# Relationships: Foreign key to Staker (owner); one-to-many with deployments, deposits, updates, withdrawals.
class EigenPod(Base, TimestampMixin):
    __tablename__ = "eigen_pods"
    id = Column(String, primary_key=True)  # pod address as string (hex)
    address = Column(String, nullable=False)
    owner_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )

    owner = relationship("Staker", back_populates="eigen_pods")
    deployment_event = relationship("PodDeployed", back_populates="pod")
    beacon_chain_deposit_events = relationship(
        "BeaconChainDeposit", back_populates="pod"
    )
    share_update_events = relationship("PodSharesUpdate", back_populates="pod")
    beacon_chain_withdrawal_events = relationship(
        "BeaconChainWithdrawal", back_populates="pod"
    )
