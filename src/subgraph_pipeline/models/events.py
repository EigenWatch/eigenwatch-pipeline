# models/events.py
from sqlalchemy import (
    Boolean,
    Column,
    String,
    BigInteger,
    ForeignKey,
    Enum as SQLEnum,
    Numeric,
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from .base import Base, TimestampMixin
from .entities import (
    ShareEventType,
    DelegationType,
    WithdrawalEventType,
    AVSRegistrationStatus,
    StrategyOperatorSetEventType,
    RewardsSubmissionType,
    StrategyWhitelistEventType,
    PodSharesUpdateType,
)


class BaseEvent(Base, TimestampMixin):
    __abstract__ = True

    id = Column(String, primary_key=True)  # Usually txHash-logIndex or custom
    transaction_hash = Column(String, nullable=False)
    log_index = Column(BigInteger, nullable=False)
    block_number = Column(BigInteger, nullable=False)
    block_timestamp = Column(BigInteger, nullable=False)  # Unix timestamp
    contract_address = Column(String, nullable=False)

    # ✅ Full raw payload for audit / schema evolution / re-processing
    raw_data = Column(JSONB, nullable=False)


# OperatorRegistered Event
# Purpose: Captures operator registration events with delegation approver.
# Relationships: Foreign key to Operator (cascade delete).
class OperatorRegistered(BaseEvent):
    __tablename__ = "operator_registered_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    delegation_approver = Column(String, nullable=False)

    operator = relationship("Operator", back_populates="registration_events")


# DelegationApproverUpdated Event
# Purpose: Records updates to an operator's delegation approver.
# Relationships: Foreign key to Operator.
class DelegationApproverUpdated(BaseEvent):
    __tablename__ = "delegation_approver_updated_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    new_delegation_approver = Column(String, nullable=False)

    operator = relationship("Operator", back_populates="delegation_approver_updates")


# OperatorMetadataUpdate Event
# Purpose: Tracks metadata URI updates for operators.
# Relationships: Foreign key to Operator.
class OperatorMetadataUpdate(BaseEvent):
    __tablename__ = "operator_metadata_update_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    metadata_uri = Column(String, nullable=False)

    operator = relationship("Operator", back_populates="metadata_update_events")


# OperatorShareEvent Event
# Purpose: Records share increases/decreases for operators and stakers in strategies.
# Relationships: Foreign keys to Operator, Staker, Strategy.
class OperatorShareEvent(BaseEvent):
    __tablename__ = "operator_share_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    shares = Column(Numeric, nullable=False)
    event_type = Column(SQLEnum(ShareEventType), nullable=False)

    operator = relationship("Operator", back_populates="share_events")
    staker = relationship("Staker", back_populates="share_events")
    strategy = relationship("Strategy", back_populates="share_events")


# StakerDelegationEvent Event
# Purpose: Captures delegation actions (delegate/undelegate) by stakers to operators.
# Relationships: Foreign keys to Staker, Operator.
class StakerDelegationEvent(BaseEvent):
    __tablename__ = "staker_delegation_events"
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    delegation_type = Column(SQLEnum(DelegationType), nullable=False)

    staker = relationship("Staker", back_populates="delegation_events")
    operator = relationship(
        "Operator"
    )  # No back_populate as not in Operator relationships


# StakerForceUndelegated Event
# Purpose: Records forced undelegations of stakers from operators.
# Relationships: Foreign keys to Staker, Operator.
class StakerForceUndelegated(BaseEvent):
    __tablename__ = "staker_force_undelegated_events"
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )

    staker = relationship("Staker", back_populates="force_undelegation_events")
    operator = relationship("Operator")


# DepositScalingFactorUpdated Event
# Purpose: Tracks updates to deposit scaling factors for stakers in strategies.
# Relationships: Foreign keys to Staker, Strategy.
class DepositScalingFactorUpdated(BaseEvent):
    __tablename__ = "deposit_scaling_factor_updated_events"
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    new_deposit_scaling_factor = Column(Numeric, nullable=False)

    staker = relationship("Staker", back_populates="deposit_scaling_events")
    strategy = relationship("Strategy")


# WithdrawalEvent Event
# Purpose: Captures withdrawal queuing or completion events.
# Relationships: Foreign keys to Staker, Operator (delegatedTo).
class WithdrawalEvent(BaseEvent):
    __tablename__ = "withdrawal_events"
    withdrawal_root = Column(String, nullable=False)
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    delegated_to_id = Column(String, ForeignKey("operators.id", ondelete="CASCADE"))
    withdrawer = Column(String, nullable=False)
    nonce = Column(BigInteger, nullable=False)
    start_block = Column(BigInteger)
    strategies = Column(
        ARRAY(String), nullable=False
    )  # Array of strategy addresses (as strings)
    shares = Column(ARRAY(Numeric), nullable=False)
    event_type = Column(SQLEnum(WithdrawalEventType), nullable=False)

    staker = relationship("Staker", back_populates="withdrawal_events")
    delegated_to = relationship("Operator")


# OperatorSharesSlashed Event
# Purpose: Records slashing of operator shares in strategies.
# Relationships: Foreign keys to Operator, Strategy.
class OperatorSharesSlashed(BaseEvent):
    __tablename__ = "operator_shares_slashed_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    total_slashed_shares = Column(Numeric, nullable=False)

    operator = relationship("Operator")
    strategy = relationship("Strategy")


# AllocationDelaySet Event
# Purpose: Sets allocation delays for operators.
# Relationships: Foreign key to Operator.
class AllocationDelaySet(BaseEvent):
    __tablename__ = "allocation_delay_set_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    delay = Column(BigInteger, nullable=False)
    effect_block = Column(BigInteger, nullable=False)

    operator = relationship("Operator")


# AllocationEvent Event
# Purpose: Records allocation changes for operators in operator sets and strategies.
# Relationships: Foreign keys to Operator, OperatorSet, Strategy.
class AllocationEvent(BaseEvent):
    __tablename__ = "allocation_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    magnitude = Column(Numeric, nullable=False)
    effect_block = Column(BigInteger, nullable=False)

    operator = relationship("Operator", back_populates="allocation_events")
    operator_set = relationship("OperatorSet", back_populates="allocation_events")
    strategy = relationship("Strategy", back_populates="allocation_events")


# EncumberedMagnitudeUpdated Event
# Purpose: Updates encumbered magnitudes for operators in strategies.
# Relationships: Foreign keys to Operator, Strategy.
class EncumberedMagnitudeUpdated(BaseEvent):
    __tablename__ = "encumbered_magnitude_updated_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    encumbered_magnitude = Column(Numeric, nullable=False)

    operator = relationship("Operator")
    strategy = relationship("Strategy")


# MaxMagnitudeUpdated Event
# Purpose: Updates max magnitudes for operators in strategies.
# Relationships: Foreign keys to Operator, Strategy.
class MaxMagnitudeUpdated(BaseEvent):
    __tablename__ = "max_magnitude_updated_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    max_magnitude = Column(Numeric, nullable=False)

    operator = relationship("Operator")
    strategy = relationship("Strategy")


# OperatorSlashed Event
# Purpose: Records slashing eventsfor operators in operator sets.
# Relationships: Foreign keys to Operator, OperatorSet.
class OperatorSlashed(BaseEvent):
    __tablename__ = "operator_slashed_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )
    strategies = Column(ARRAY(String), nullable=False)
    wad_slashed = Column(ARRAY(Numeric), nullable=False)
    description = Column(String, nullable=False)

    operator = relationship("Operator", back_populates="slashing_events")
    operator_set = relationship("OperatorSet", back_populates="slashing_events")


# AVSRegistrarSet Event
# Purpose: Sets registrars for AVS.
# Relationships: Foreign key to AVS.
class AVSRegistrarSet(BaseEvent):
    __tablename__ = "avs_registrar_set_events"
    avs_id = Column(String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False)
    registrar = Column(String, nullable=False)

    avs = relationship("AVS", back_populates="registrar_set_events")


# AVSMetadataUpdate Event
# Purpose: Tracks metadata URI updates for AVS.
# Relationships: Foreign key to AVS.
class AVSMetadataUpdate(BaseEvent):
    __tablename__ = "avs_metadata_update_events"
    avs_id = Column(String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False)
    metadata_uri = Column(String, nullable=False)

    avs = relationship("AVS", back_populates="metadata_update_events")


# OperatorSetCreated Event
# Purpose: Records creation of new operator sets for AVS.
# Relationships: Foreign keys to OperatorSet, AVS.
class OperatorSetCreated(BaseEvent):
    __tablename__ = "operator_set_created_events"
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )
    avs_id = Column(String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False)
    operator_set_id_num = Column(
        BigInteger, nullable=False
    )  # Renamed to avoid conflict

    operator_set = relationship("OperatorSet", back_populates="creation_event")
    avs = relationship("AVS", back_populates="operator_set_creation_events")


# OperatorAddedToOperatorSet Event
# Purpose: Adds operators to operator sets.
# Relationships: Foreign keys to Operator, OperatorSet.
class OperatorAddedToOperatorSet(BaseEvent):
    __tablename__ = "operator_added_to_operator_set_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )

    operator = relationship("Operator", back_populates="operator_set_join_events")
    operator_set = relationship("OperatorSet", back_populates="member_join_events")


# OperatorRemovedFromOperatorSet Event
# Purpose: Removes operators from operator sets.
# Relationships: Foreign keys to Operator, OperatorSet.
class OperatorRemovedFromOperatorSet(BaseEvent):
    __tablename__ = "operator_removed_from_operator_set_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )

    operator = relationship("Operator", back_populates="operator_set_leave_events")
    operator_set = relationship("OperatorSet", back_populates="member_leave_events")


# RedistributionAddressSet Event
# Purpose: Sets redistribution addresses for operator sets.
# Relationships: Foreign key to OperatorSet.
class RedistributionAddressSet(BaseEvent):
    __tablename__ = "redistribution_address_set_events"
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )
    redistribution_recipient = Column(String, nullable=False)

    operator_set = relationship("OperatorSet", back_populates="redistribution_events")


# StrategyOperatorSetEvent Event
# Purpose: Adds/removes strategies from operator sets.
# Relationships: Foreign keys to OperatorSet, Strategy.
class StrategyOperatorSetEvent(BaseEvent):
    __tablename__ = "strategy_operator_set_events"
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    event_type = Column(SQLEnum(StrategyOperatorSetEventType), nullable=False)

    operator_set = relationship("OperatorSet", back_populates="strategy_events")
    strategy = relationship("Strategy", back_populates="strategy_operator_set_events")


# RewardsSubmission Event
# Purpose: Submits rewards for AVS or other types.
# Relationships: Foreign key to AVS (optional).
class RewardsSubmission(BaseEvent):
    __tablename__ = "rewards_submission_events"
    avs_id = Column(String, ForeignKey("avs.id", ondelete="CASCADE"))
    submitter = Column(String, nullable=False)
    submission_nonce = Column(BigInteger, nullable=False)
    rewards_submission_hash = Column(String, nullable=False)
    submission_type = Column(SQLEnum(RewardsSubmissionType), nullable=False)
    strategies_and_multipliers = Column(JSONB, nullable=False)
    token = Column(String, nullable=False)
    amount = Column(Numeric, nullable=False)
    start_timestamp = Column(BigInteger, nullable=False)
    duration = Column(BigInteger, nullable=False)

    avs = relationship("AVS", back_populates="rewards_submission_events")


# OperatorDirectedAVSRewardsSubmission Event
# Purpose: Submits operator-directed rewards for AVS.
# Relationships: Foreign key to AVS.
class OperatorDirectedAVSRewardsSubmission(BaseEvent):
    __tablename__ = "operator_directed_avs_rewards_submission_events"
    caller = Column(String, nullable=False)
    avs_id = Column(String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False)
    operator_directed_rewards_submission_hash = Column(String, nullable=False)
    submission_nonce = Column(BigInteger, nullable=False)
    strategies_and_multipliers = Column(JSONB, nullable=False)
    token = Column(String, nullable=False)
    operator_rewards = Column(JSONB, nullable=False)
    start_timestamp = Column(BigInteger, nullable=False)
    duration = Column(BigInteger, nullable=False)
    description = Column(String, nullable=False)

    avs = relationship("AVS", back_populates="operator_directed_rewards_events")


# OperatorDirectedOperatorSetRewardsSubmission Event
# Purpose: Submits operator-directed rewards for operator sets.
# Relationships: Foreign key to OperatorSet.
class OperatorDirectedOperatorSetRewardsSubmission(BaseEvent):
    __tablename__ = "operator_directed_operator_set_rewards_submission_events"
    caller = Column(String, nullable=False)
    operator_directed_rewards_submission_hash = Column(String, nullable=False)
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )
    submission_nonce = Column(BigInteger, nullable=False)
    strategies_and_multipliers = Column(JSONB, nullable=False)
    token = Column(String, nullable=False)
    operator_rewards = Column(JSONB, nullable=False)
    start_timestamp = Column(BigInteger, nullable=False)
    duration = Column(BigInteger, nullable=False)
    description = Column(String, nullable=False)

    operator_set = relationship(
        "OperatorSet", back_populates="operator_directed_rewards_events"
    )


# RewardsUpdaterSet Event
# Purpose: Sets rewards updaters.
# Relationships: No entity references.
class RewardsUpdaterSet(BaseEvent):
    __tablename__ = "rewards_updater_set_events"
    old_rewards_updater = Column(String, nullable=False)
    new_rewards_updater = Column(String, nullable=False)


# RewardsForAllSubmitterSet Event
# Purpose: Toggles rewards for all submitters.
# Relationships: No entity references.
class RewardsForAllSubmitterSet(BaseEvent):
    __tablename__ = "rewards_for_all_submitter_set_events"
    rewards_for_all_submitter = Column(String, nullable=False)
    old_value = Column(Boolean, nullable=False)
    new_value = Column(Boolean, nullable=False)


# ActivationDelaySet Event
# Purpose: Sets activation delays.
# Relationships: No entity references.
class ActivationDelaySet(BaseEvent):
    __tablename__ = "activation_delay_set_events"
    old_activation_delay = Column(BigInteger, nullable=False)
    new_activation_delay = Column(BigInteger, nullable=False)


# DefaultOperatorSplitBipsSet Event
# Purpose: Sets default operator split basis points.
# Relationships: No entity references.
class DefaultOperatorSplitBipsSet(BaseEvent):
    __tablename__ = "default_operator_split_bips_set_events"
    old_default_operator_split_bips = Column(BigInteger, nullable=False)
    new_default_operator_split_bips = Column(BigInteger, nullable=False)


# OperatorAVSSplitBipsSet Event
# Purpose: Sets operator AVS split basis points.
# Relationships: Foreign keys to Operator, AVS.
class OperatorAVSSplitBipsSet(BaseEvent):
    __tablename__ = "operator_avs_split_bips_set_events"
    caller = Column(String, nullable=False)
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    avs_id = Column(String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False)
    activated_at = Column(BigInteger, nullable=False)
    old_operator_avs_split_bips = Column(Numeric, nullable=False)
    new_operator_avs_split_bips = Column(Numeric, nullable=False)

    operator = relationship("Operator")
    avs = relationship("AVS")


# OperatorPISplitBipsSet Event
# Purpose: Sets operator PI split basis points.
# Relationships: Foreign key to Operator.
class OperatorPISplitBipsSet(BaseEvent):
    __tablename__ = "operator_pi_split_bips_set_events"
    caller = Column(String, nullable=False)
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    activated_at = Column(BigInteger, nullable=False)
    old_operator_pi_split_bips = Column(Numeric, nullable=False)
    new_operator_pi_split_bips = Column(Numeric, nullable=False)

    operator = relationship("Operator")


# OperatorSetSplitBipsSet Event
# Purpose: Sets operator set split basis points.
# Relationships: Foreign keys to Operator, OperatorSet.
class OperatorSetSplitBipsSet(BaseEvent):
    __tablename__ = "operator_set_split_bips_set_events"
    caller = Column(String, nullable=False)
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )
    activated_at = Column(BigInteger, nullable=False)
    old_operator_set_split_bips = Column(Numeric, nullable=False)
    new_operator_set_split_bips = Column(Numeric, nullable=False)

    operator = relationship("Operator")
    operator_set = relationship("OperatorSet")


# ClaimerForSet Event
# Purpose: Sets claimers for earners.
# Relationships: No entity references (addresses are Bytes).
class ClaimerForSet(BaseEvent):
    __tablename__ = "claimer_for_set_events"
    earner = Column(String, nullable=False)
    old_claimer = Column(String, nullable=False)
    claimer = Column(String, nullable=False)


# DistributionRootSubmitted Event
# Purpose: Submits distribution roots.
# Relationships: No entity references.
class DistributionRootSubmitted(BaseEvent):
    __tablename__ = "distribution_root_submitted_events"
    root_index = Column(Numeric, nullable=False)
    root = Column(String, nullable=False)
    rewards_calculation_end_timestamp = Column(BigInteger, nullable=False)
    activated_at = Column(BigInteger, nullable=False)


# DistributionRootDisabled Event
# Purpose: Disables distribution roots.
# Relationships: No entity references.
class DistributionRootDisabled(BaseEvent):
    __tablename__ = "distribution_root_disabled_events"
    root_index = Column(Numeric, nullable=False)


# RewardsClaimed Event
# Purpose: Records claimed rewards.
# Relationships: No entity references.
class RewardsClaimed(BaseEvent):
    __tablename__ = "rewards_claimed_events"
    root = Column(String, nullable=False)
    earner = Column(String, nullable=False)
    claimer = Column(String, nullable=False)
    recipient = Column(String, nullable=False)
    token = Column(String, nullable=False)
    claimed_amount = Column(Numeric, nullable=False)


# Deposit Event
# Purpose: Captures deposits into strategies by stakers.
# Relationships: Foreign keys to Staker, Strategy.
class Deposit(BaseEvent):
    __tablename__ = "deposit_events"
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    shares = Column(Numeric, nullable=False)

    staker = relationship("Staker", back_populates="deposit_events")
    strategy = relationship("Strategy", back_populates="deposit_events")


# StrategyWhitelisterChanged Event
# Purpose: Changes strategy whitelisters.
# Relationships: No entity references.
class StrategyWhitelisterChanged(BaseEvent):
    __tablename__ = "strategy_whitelister_changed_events"
    previous_address = Column(String, nullable=False)
    new_address = Column(String, nullable=False)


# StrategyWhitelistEvent Event
# Purpose: Adds/removes strategies from whitelists.
# Relationships: Foreign key to Strategy.
class StrategyWhitelistEvent(BaseEvent):
    __tablename__ = "strategy_whitelist_events"
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    event_type = Column(SQLEnum(StrategyWhitelistEventType), nullable=False)

    strategy = relationship("Strategy", back_populates="whitelist_events")


# BurnOrRedistributableSharesIncreased Event
# Purpose: Increases burn or redistributable shares for operator sets.
# Relationships: Foreign keys to OperatorSet, Strategy.
class BurnOrRedistributableSharesIncreased(BaseEvent):
    __tablename__ = "burn_or_redistributable_shares_increased_events"
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )
    slash_id = Column(BigInteger, nullable=False)
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    shares = Column(Numeric, nullable=False)

    operator_set = relationship("OperatorSet")
    strategy = relationship("Strategy")


# BurnOrRedistributableSharesDecreased Event
# Purpose: Decreases burn or redistributable shares for operator sets.
# Relationships: Foreign keys to OperatorSet, Strategy.
class BurnOrRedistributableSharesDecreased(BaseEvent):
    __tablename__ = "burn_or_redistributable_shares_decreased_events"
    operator_set_id = Column(
        String, ForeignKey("operator_sets.id", ondelete="CASCADE"), nullable=False
    )
    slash_id = Column(BigInteger, nullable=False)
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    shares = Column(Numeric, nullable=False)

    operator_set = relationship("OperatorSet")
    strategy = relationship("Strategy")


# BurnableSharesDecreased Event
# Purpose: Decreases burnable shares for strategies.
# Relationships: Foreign key to Strategy.
class BurnableSharesDecreased(BaseEvent):
    __tablename__ = "burnable_shares_decreased_events"
    strategy_id = Column(
        String, ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    shares = Column(BigInteger, nullable=False)

    strategy = relationship("Strategy")


# OperatorAVSRegistrationStatusUpdated Event
# Purpose: Updates registration status of operators in AVS.
# Relationships: Foreign keys to Operator, AVS.
class OperatorAVSRegistrationStatusUpdated(BaseEvent):
    __tablename__ = "operator_avs_registration_status_updated_events"
    operator_id = Column(
        String, ForeignKey("operators.id", ondelete="CASCADE"), nullable=False
    )
    avs_id = Column(String, ForeignKey("avs.id", ondelete="CASCADE"), nullable=False)
    status = Column(SQLEnum(AVSRegistrationStatus), nullable=False)

    operator = relationship("Operator", back_populates="avs_registration_events")
    avs = relationship("AVS", back_populates="operator_registration_events")


# PodDeployed Event
# Purpose: Deploys EigenPods for stakers.
# Relationships: Foreign keys to EigenPod, Staker.
class PodDeployed(BaseEvent):
    __tablename__ = "pod_deployed_events"
    pod_id = Column(
        String, ForeignKey("eigen_pods.id", ondelete="CASCADE"), nullable=False
    )
    owner_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )

    pod = relationship("EigenPod", back_populates="deployment_event")
    owner = relationship("Staker", back_populates="pod_deployment_events")


# BeaconChainDeposit Event
# Purpose: Deposits on beacon chain for pods.
# Relationships: Foreign keys to EigenPod, Staker.
class BeaconChainDeposit(BaseEvent):
    __tablename__ = "beacon_chain_deposit_events"
    pod_id = Column(String, ForeignKey("eigen_pods.id", ondelete="CASCADE"))
    pod_owner_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    amount = Column(Numeric, nullable=False)

    pod = relationship("EigenPod", back_populates="beacon_chain_deposit_events")
    pod_owner = relationship("Staker", back_populates="beacon_chain_deposit_events")


# PodSharesUpdate Event
# Purpose: Updates shares for pods.
# Relationships: Foreign keys to EigenPod, Staker.
class PodSharesUpdate(BaseEvent):
    __tablename__ = "pod_shares_update_events"
    pod_id = Column(String, ForeignKey("eigen_pods.id", ondelete="CASCADE"))
    pod_owner_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    shares_delta = Column(Numeric, nullable=False)
    new_total_shares = Column(Numeric)
    update_type = Column(SQLEnum(PodSharesUpdateType), nullable=False)

    pod = relationship("EigenPod", back_populates="share_update_events")
    pod_owner = relationship("Staker", back_populates="pod_shares_update_events")


# BeaconChainWithdrawal Event
# Purpose: Withdrawals on beacon chain for pods.
# Relationships: Foreign keys to EigenPod, Staker.
class BeaconChainWithdrawal(BaseEvent):
    __tablename__ = "beacon_chain_withdrawal_events"
    pod_id = Column(String, ForeignKey("eigen_pods.id", ondelete="CASCADE"))
    pod_owner_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    shares = Column(Numeric, nullable=False)
    nonce = Column(BigInteger, nullable=False)
    delegated_address = Column(String, nullable=False)
    withdrawer = Column(String, nullable=False)
    withdrawal_root = Column(String, nullable=False)

    pod = relationship("EigenPod", back_populates="beacon_chain_withdrawal_events")
    pod_owner = relationship("Staker", back_populates="beacon_chain_withdrawal_events")


# BeaconChainETHWithdrawalCompleted Event
# Purpose: Completes ETH withdrawals on beacon chain.
# Relationships: Foreign key to Staker.
class BeaconChainETHWithdrawalCompleted(BaseEvent):
    __tablename__ = "beacon_chain_eth_withdrawal_completed_events"
    pod_owner_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    shares = Column(Numeric, nullable=False)
    nonce = Column(BigInteger, nullable=False)
    delegated_address = Column(String, nullable=False)
    withdrawer = Column(String, nullable=False)
    withdrawal_root = Column(String, nullable=False)

    pod_owner = relationship("Staker")


# BeaconChainSlashingEvent Event
# Purpose: Slashing events on beacon chain for stakers.
# Relationships: Foreign key to Staker.
class BeaconChainSlashingEvent(BaseEvent):
    __tablename__ = "beacon_chain_slashing_events"
    staker_id = Column(
        String, ForeignKey("stakers.id", ondelete="CASCADE"), nullable=False
    )
    prev_beacon_chain_slashing_factor = Column(Numeric, nullable=False)
    new_beacon_chain_slashing_factor = Column(Numeric, nullable=False)

    staker = relationship("Staker", back_populates="beacon_chain_slashing_events")


# BurnableETHSharesIncreased Event
# Purpose: Increases burnable ETH shares.
# Relationships: No entity references.
class BurnableETHSharesIncreased(BaseEvent):
    __tablename__ = "burnable_eth_shares_increased_events"
    shares = Column(Numeric, nullable=False)


# PectraForkTimestampSet Event
# Purpose: Sets Pectra fork timestamps.
# Relationships: No entity references.
class PectraForkTimestampSet(BaseEvent):
    __tablename__ = "pectra_fork_timestamp_set_events"
    new_pectra_fork_timestamp = Column(Numeric, nullable=False)


# ProofTimestampSetterSet Event
# Purpose: Sets proof timestamp setters.
# Relationships: No entity references.
class ProofTimestampSetterSet(BaseEvent):
    __tablename__ = "proof_timestamp_setter_set_events"
    new_proof_timestamp_setter = Column(String, nullable=False)
