# analytics_pipeline/assets/state_rebuild.py
"""
State Rebuild Assets - Reconstruct operator state from events
"""

from dagster import asset, OpExecutionContext, AssetIn
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


def process_operators(
    context: OpExecutionContext,
    changed_operators: Set[str],
    reconstructor,
    log_prefix: str,
    config: ConfigResource,
) -> int:
    """
    Shared helper to process operators with a 2-step reconstructor:
      1. fetch_state_for_operator → returns list/dict of state rows
      2. insert_state_rows → inserts OR updates those rows

    This structure is generic and reusable across all reconstructor types.
    """
    if not changed_operators:
        context.log.info(f"No operators to process for {log_prefix}")
        return 0

    start_time = datetime.now(timezone.utc)
    processed_count = 0
    total_rows_fetched = 0
    total_rows_inserted = 0

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"{log_prefix} {idx}/{len(changed_operators)}: {operator_id}"
            )

        # --- STEP 1: Fetch reconstructed state ---
        try:
            state_rows = reconstructor.fetch_state_for_operator(operator_id)
        except Exception as exc:
            context.log.error(
                f"{log_prefix}: Failed while fetching for {operator_id}: {exc}"
            )
            continue

        rows_fetched = len(state_rows) if state_rows else 0
        total_rows_fetched += rows_fetched

        if rows_fetched == 0:
            context.log.debug(f"{log_prefix}: No state rows for {operator_id}")
            processed_count += 1
            continue

        # --- STEP 2: Insert / update state ---
        try:
            inserted_count = reconstructor.insert_state_rows(operator_id, state_rows)
            total_rows_inserted += inserted_count
        except Exception as exc:
            context.log.error(
                f"{log_prefix}: Failed while inserting for {operator_id}: {exc}"
            )
            continue

        processed_count += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    context.log.info(
        f"{log_prefix}: Processed {processed_count} operators, "
        f"rows fetched: {total_rows_fetched}, "
        f"rows inserted/updated: {total_rows_inserted}, "
        f"duration: {duration:.2f}s"
    )

    return processed_count


# -----------------------------
# Operator state rebuild assets
# -----------------------------


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
) -> int:
    reconstructor = StrategyStateReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Rebuilding strategy state", config
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
) -> int:
    reconstructor = AllocationReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Rebuilding allocations", config
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
) -> int:
    reconstructor = AVSRelationshipReconstructor(db, context.log)
    return process_operators(
        context,
        changed_operators,
        reconstructor,
        "Rebuilding AVS relationships",
        config,
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
) -> int:
    reconstructor = CommissionRateReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Rebuilding commission rates", config
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
) -> int:
    reconstructor = DelegatorReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Rebuilding delegators", config
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
) -> int:
    reconstructor = SlashingReconstructor(db, context.log)
    return process_operators(
        context,
        changed_operators,
        reconstructor,
        "Rebuilding slashing incidents",
        config,
    )


# -----------------------------
# Aggregator asset
# -----------------------------


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
) -> int:
    if not changed_operators:
        context.log.info("No operators to aggregate")
        return 0

    start_time = datetime.now(timezone.utc)
    aggregator = CurrentStateAggregator(db, context.log)

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"Aggregating state {idx}/{len(changed_operators)}: {operator_id}"
            )
        aggregator.aggregate_for_operator(operator_id)

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    current_time = datetime.now(timezone.utc)

    # Update checkpoint
    db.execute_update(
        config.get_update_checkpoint_query(),
        {
            "pipeline_name": config.checkpoint_key,
            "last_processed_at": current_time,
            "last_processed_block": 0,
            "operators_processed_count": len(changed_operators),
            "total_events_processed": 0,
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

    context.log.info(f"Checkpoint updated at {current_time}")
    return len(changed_operators)
