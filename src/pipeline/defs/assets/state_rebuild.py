# analytics_pipeline/assets/state_rebuild.py
"""
State Rebuild Assets - Reconstruct operator state from events
"""

from dagster import asset, OpExecutionContext, Output, MetadataValue, AssetIn
from datetime import datetime, timezone
from typing import Set
from ..resources import DatabaseResource, ConfigResource
from services.state_reconstructor import (
    StrategyStateReconstructor,
    AllocationReconstructor,
    AVSRelationshipReconstructor,
    CommissionRateReconstructor,
    DelegatorReconstructor,
    SlashingReconstructor,
    CurrentStateAggregator,
)


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds operator_strategy_state for all affected operators",
    compute_kind="sql",
)
def operator_strategy_state_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> Output[int]:
    """
    Rebuild strategy-level state (TVS, encumbered magnitude, utilization)
    for each affected operator.
    """
    if not changed_operators:
        context.log.info("No operators to process")
        return Output(0, metadata={"operators_processed": 0})

    start_time = datetime.now(timezone.utc)
    reconstructor = StrategyStateReconstructor(db, context.log)

    processed_count = 0
    total_strategies_updated = 0

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"Processing operator {idx}/{len(changed_operators)}: {operator_id}"
            )

        strategies_updated = reconstructor.rebuild_for_operator(operator_id)
        total_strategies_updated += strategies_updated
        processed_count += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    context.log.info(
        f"Rebuilt strategy state for {processed_count} operators "
        f"({total_strategies_updated} strategy records) in {duration:.2f}s"
    )

    return Output(
        processed_count,
        metadata={
            "operators_processed": processed_count,
            "strategies_updated": total_strategies_updated,
            "duration_seconds": duration,
            "avg_time_per_operator_ms": (
                (duration * 1000 / processed_count) if processed_count else 0
            ),
        },
    )


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds operator_allocations for all affected operators",
    compute_kind="sql",
)
def operator_allocations_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> Output[int]:
    """
    Rebuild current and pending allocations for each affected operator.
    """
    if not changed_operators:
        return Output(0, metadata={"operators_processed": 0})

    start_time = datetime.now(timezone.utc)
    reconstructor = AllocationReconstructor(db, context.log)

    processed_count = 0
    total_allocations = 0

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"Processing allocations {idx}/{len(changed_operators)}: {operator_id}"
            )

        allocations = reconstructor.rebuild_for_operator(operator_id)
        total_allocations += allocations
        processed_count += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    return Output(
        processed_count,
        metadata={
            "operators_processed": processed_count,
            "allocations_updated": total_allocations,
            "duration_seconds": duration,
        },
    )


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds AVS registration relationships and history",
    compute_kind="sql",
)
def operator_avs_relationships_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> Output[int]:
    """
    Rebuild AVS registration history and current relationship status.
    """
    if not changed_operators:
        return Output(0, metadata={"operators_processed": 0})

    start_time = datetime.now(timezone.utc)
    reconstructor = AVSRelationshipReconstructor(db, context.log)

    processed_count = 0
    total_relationships = 0

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"Processing AVS relationships {idx}/{len(changed_operators)}: {operator_id}"
            )

        relationships = reconstructor.rebuild_for_operator(operator_id)
        total_relationships += relationships
        processed_count += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    return Output(
        processed_count,
        metadata={
            "operators_processed": processed_count,
            "relationships_updated": total_relationships,
            "duration_seconds": duration,
        },
    )


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds commission rates (AVS, PI, Operator Set)",
    compute_kind="sql",
)
def operator_commission_rates_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> Output[int]:
    """
    Rebuild commission rate state (current and upcoming).
    """
    if not changed_operators:
        return Output(0, metadata={"operators_processed": 0})

    start_time = datetime.now(timezone.utc)
    reconstructor = CommissionRateReconstructor(db, context.log)

    processed_count = 0
    total_rates = 0

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"Processing commissions {idx}/{len(changed_operators)}: {operator_id}"
            )

        rates = reconstructor.rebuild_for_operator(operator_id)
        total_rates += rates
        processed_count += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    return Output(
        processed_count,
        metadata={
            "operators_processed": processed_count,
            "commission_rates_updated": total_rates,
            "duration_seconds": duration,
        },
    )


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds delegator lists and shares",
    compute_kind="sql",
)
def operator_delegators_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> Output[int]:
    """
    Rebuild delegator state and per-strategy shares.
    """
    if not changed_operators:
        return Output(0, metadata={"operators_processed": 0})

    start_time = datetime.now(timezone.utc)
    reconstructor = DelegatorReconstructor(db, context.log)

    processed_count = 0
    total_delegators = 0

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"Processing delegators {idx}/{len(changed_operators)}: {operator_id}"
            )

        delegators = reconstructor.rebuild_for_operator(operator_id)
        total_delegators += delegators
        processed_count += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    return Output(
        processed_count,
        metadata={
            "operators_processed": processed_count,
            "delegators_updated": total_delegators,
            "duration_seconds": duration,
        },
    )


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds slashing incidents and amounts",
    compute_kind="sql",
)
def operator_slashing_incidents_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> Output[int]:
    """
    Rebuild slashing incident records and per-strategy slashed amounts.
    """
    if not changed_operators:
        return Output(0, metadata={"operators_processed": 0})

    start_time = datetime.now(timezone.utc)
    reconstructor = SlashingReconstructor(db, context.log)

    processed_count = 0
    total_incidents = 0

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"Processing slashing {idx}/{len(changed_operators)}: {operator_id}"
            )

        incidents = reconstructor.rebuild_for_operator(operator_id)
        total_incidents += incidents
        processed_count += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    return Output(
        processed_count,
        metadata={
            "operators_processed": processed_count,
            "incidents_processed": total_incidents,
            "duration_seconds": duration,
        },
    )


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "strategy_state": AssetIn("operator_strategy_state_asset"),
        "allocations": AssetIn("operator_allocations_asset"),
        "avs_relationships": AssetIn("operator_avs_relationships_asset"),
        "commission_rates": AssetIn("operator_commission_rates_asset"),
        "delegators": AssetIn("operator_delegators_asset"),
        "slashing": AssetIn("operator_slashing_incidents_asset"),
    },
    description="Aggregates all state into operator_current_state table",
    compute_kind="sql",
)
def operator_current_state_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
    strategy_state: int,
    allocations: int,
    avs_relationships: int,
    commission_rates: int,
    delegators: int,
    slashing: int,
) -> Output[int]:
    """
    Final aggregation step - update operator_current_state with counts and metadata
    from all the derived tables.
    """
    if not changed_operators:
        return Output(0, metadata={"operators_processed": 0})

    start_time = datetime.now(timezone.utc)
    aggregator = CurrentStateAggregator(db, context.log)

    processed_count = 0

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"Aggregating state {idx}/{len(changed_operators)}: {operator_id}"
            )

        aggregator.aggregate_for_operator(operator_id)
        processed_count += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    # Update checkpoint
    current_time = datetime.now(timezone.utc)
    db.execute_update(
        config.get_update_checkpoint_query(),
        {
            "pipeline_name": config.checkpoint_key,
            "last_processed_at": current_time,
            "last_processed_block": 0,  # We use timestamp-based cursoring
            "operators_processed_count": processed_count,
            "total_events_processed": 0,  # Could track this if needed
            "run_duration_seconds": duration,
            "run_metadata": {
                "strategy_state_updates": strategy_state,
                "allocations_updates": allocations,
                "avs_relationships_updates": avs_relationships,
                "commission_rates_updates": commission_rates,
                "delegators_updates": delegators,
                "slashing_updates": slashing,
            },
        },
        db="analytics",
    )

    context.log.info(f"Updated checkpoint: {current_time}")

    return Output(
        processed_count,
        metadata={
            "operators_processed": processed_count,
            "duration_seconds": duration,
            "checkpoint_updated": str(current_time),
        },
    )
