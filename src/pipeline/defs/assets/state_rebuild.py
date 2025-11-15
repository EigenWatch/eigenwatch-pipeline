# defs/assets/state_rebuild.py
"""
State Rebuild Assets - Reconstruct operator state from events
"""

import json
from dagster import asset, OpExecutionContext, AssetIn
from datetime import datetime, timezone
from typing import Set

from services.processors.process_operators import process_operators
from services.reconstructors.allocation_state import AllocationReconstructor
from services.reconstructors.strategy_state import StrategyStateReconstructor
from services.reconstructors.avs_relationship_history import (
    AVSRelationshipHistoryReconstructor,
)
from services.reconstructors.avs_relationship_current import (
    AVSRelationshipCurrentReconstructor,
)
from services.reconstructors.commission_pi import CommissionPIReconstructor
from services.reconstructors.commission_avs import CommissionAVSReconstructor
from services.reconstructors.commission_operator_set import (
    CommissionOperatorSetReconstructor,
)
from services.reconstructors.delegator_history import DelegatorHistoryReconstructor
from services.reconstructors.delegator_current import DelegatorCurrentReconstructor
from services.reconstructors.delegator_shares import DelegatorSharesReconstructor
from services.reconstructors.slashing_events_cache import (
    SlashingEventsCacheReconstructor,
)
from services.reconstructors.slashing_incidents import SlashingIncidentsReconstructor
from services.reconstructors.slashing_amounts import SlashingAmountsReconstructor

from ..resources import DatabaseResource, ConfigResource

# -----------------------------
# Strategy State
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


# -----------------------------
# Allocations
# -----------------------------


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


# -----------------------------
# AVS Relationships (2-step)
# -----------------------------


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds AVS registration history (step 1 of 2)",
    compute_kind="sql",
)
def operator_avs_history_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> int:
    reconstructor = AVSRelationshipHistoryReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building AVS history", config
    )


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "avs_history": AssetIn("operator_avs_history_asset"),
    },
    description="Rebuilds current AVS relationships from history (step 2 of 2)",
    compute_kind="sql",
)
def operator_avs_relationships_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
    avs_history: int,
) -> int:
    reconstructor = AVSRelationshipCurrentReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building AVS relationships", config
    )


# -----------------------------
# Commission Rates (3 separate assets)
# -----------------------------


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds PI commission rates",
    compute_kind="sql",
)
def operator_commission_pi_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> int:
    reconstructor = CommissionPIReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building PI commissions", config
    )


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds AVS commission rates",
    compute_kind="sql",
)
def operator_commission_avs_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> int:
    reconstructor = CommissionAVSReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building AVS commissions", config
    )


# TODO: Verify that there is no issues with this asset. Currently has no data in db to test
@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds Operator Set commission rates",
    compute_kind="sql",
)
def operator_commission_operator_set_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> int:
    reconstructor = CommissionOperatorSetReconstructor(db, context.log)
    return process_operators(
        context,
        changed_operators,
        reconstructor,
        "Building Operator Set commissions",
        config,
    )


# -----------------------------
# Delegators (3-step)
# -----------------------------


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds delegator history (step 1 of 3)",
    compute_kind="sql",
)
def operator_delegator_history_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> int:
    reconstructor = DelegatorHistoryReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building delegator history", config
    )


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "delegator_history": AssetIn("operator_delegator_history_asset"),
    },
    description="Rebuilds current delegator state from history (step 2 of 3)",
    compute_kind="sql",
)
def operator_delegators_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
    delegator_history: int,
) -> int:
    reconstructor = DelegatorCurrentReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building delegator state", config
    )


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "delegators": AssetIn("operator_delegators_asset"),
    },
    description="Rebuilds delegator shares (step 3 of 3)",
    compute_kind="sql",
)
def operator_delegator_shares_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
    delegators: int,
) -> int:
    reconstructor = DelegatorSharesReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building delegator shares", config
    )


# -----------------------------
# Slashing (3-step)
# -----------------------------


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Cache slashing events from events DB (step 1 of 3)",
    compute_kind="sql",
)
def operator_slashing_events_cache_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> int:
    reconstructor = SlashingEventsCacheReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Caching slashing events", config
    )


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "slashing_cache": AssetIn("operator_slashing_events_cache_asset"),
    },
    description="Rebuild slashing incidents from cache (step 2 of 3)",
    compute_kind="sql",
)
def operator_slashing_incidents_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
    slashing_cache: int,
) -> int:
    reconstructor = SlashingIncidentsReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building slashing incidents", config
    )


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "slashing_incidents": AssetIn("operator_slashing_incidents_asset"),
    },
    description="Rebuild slashing amounts per strategy (step 3 of 3)",
    compute_kind="sql",
)
def operator_slashing_amounts_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
    slashing_incidents: int,
) -> int:
    reconstructor = SlashingAmountsReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building slashing amounts", config
    )


# -----------------------------
# Aggregator asset (updated dependencies)
# -----------------------------


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "strategy_state": AssetIn("operator_strategy_state_asset"),
        "allocations": AssetIn("operator_allocations_asset"),
        "avs_relationships": AssetIn("operator_avs_relationships_asset"),
        "commission_pi": AssetIn("operator_commission_pi_asset"),
        "commission_avs": AssetIn("operator_commission_avs_asset"),
        "commission_operator_set": AssetIn("operator_commission_operator_set_asset"),
        "delegators": AssetIn("operator_delegators_asset"),
        "delegator_shares": AssetIn("operator_delegator_shares_asset"),
        "slashing_incidents": AssetIn("operator_slashing_incidents_asset"),
        "slashing_amounts": AssetIn("operator_slashing_amounts_asset"),
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
    commission_pi: int,
    commission_avs: int,
    commission_operator_set: int,
    delegators: int,
    delegator_shares: int,
    slashing_incidents: int,
    slashing_amounts: int,
) -> int:
    if not changed_operators:
        context.log.info("No operators to aggregate")
        return 0

    start_time = datetime.now(timezone.utc)

    aggregate_query = """
        WITH operator_info AS (
            SELECT 
                id as operator_id,
                address as operator_address
            FROM operators
            WHERE id = :operator_id
        ),
        first_activity AS (
            SELECT 
                MIN(event_time) as first_activity_at,
                MIN(event_block) as first_activity_block,
                (ARRAY_AGG(event_type ORDER BY event_time))[1] as first_activity_type
            FROM (
                SELECT allocated_at as event_time, allocated_at_block as event_block, 'ALLOCATION' as event_type
                FROM operator_allocations WHERE operator_id = :operator_id
                UNION ALL
                SELECT status_changed_at, status_changed_block, 'AVS_REGISTRATION'
                FROM operator_avs_registration_history WHERE operator_id = :operator_id
                UNION ALL
                SELECT delegated_at, NULL, 'DELEGATION'
                FROM operator_delegators WHERE operator_id = :operator_id AND delegated_at IS NOT NULL
                UNION ALL
                SELECT slashed_at, slashed_at_block, 'SLASHING'
                FROM operator_slashing_incidents WHERE operator_id = :operator_id
            ) all_events
        ),
        counts AS (
            SELECT
                COUNT(DISTINCT avs_id) FILTER (WHERE current_status = 'REGISTERED') as active_avs_count,
                COUNT(DISTINCT avs_id) as registered_avs_count
            FROM operator_avs_relationships
            WHERE operator_id = :operator_id
        ),
        operator_set_count AS (
            SELECT COUNT(DISTINCT operator_set_id) as active_operator_set_count
            FROM operator_allocations
            WHERE operator_id = :operator_id
        ),
        delegator_counts AS (
            SELECT 
                COUNT(*) as total_delegators,
                COUNT(*) FILTER (WHERE is_delegated = TRUE) as active_delegators
            FROM operator_delegators
            WHERE operator_id = :operator_id
        ),
        slashing_info AS (
            SELECT 
                COUNT(*) as total_slash_events,
                MAX(slashed_at) as last_slashed_at
            FROM operator_slashing_incidents
            WHERE operator_id = :operator_id
        ),
        activity_info AS (
            SELECT MAX(allocated_at) as last_allocation_at
            FROM operator_allocations
            WHERE operator_id = :operator_id
        )
        INSERT INTO operator_state (
            operator_id, operator_address,
            first_activity_at, first_activity_block, first_activity_type,
            current_delegation_approver,
            active_avs_count, registered_avs_count, active_operator_set_count,
            total_delegators, active_delegators,
            total_slash_events, last_slashed_at,
            last_allocation_at, last_activity_at,
            is_active, updated_at
        )
        SELECT 
            oi.operator_id,
            oi.operator_address,
            COALESCE(fa.first_activity_at, NOW()),
            COALESCE(fa.first_activity_block, 0),
            COALESCE(fa.first_activity_type, 'UNKNOWN'),
            '0x0000000000000000000000000000000000000000',
            c.active_avs_count,
            c.registered_avs_count,
            osc.active_operator_set_count,
            dc.total_delegators,
            dc.active_delegators,
            si.total_slash_events,
            si.last_slashed_at,
            ai.last_allocation_at,
            NOW() as last_activity_at,
            TRUE as is_active,
            NOW() as updated_at
        FROM operator_info oi
        CROSS JOIN first_activity fa
        CROSS JOIN counts c
        CROSS JOIN operator_set_count osc
        CROSS JOIN delegator_counts dc
        CROSS JOIN slashing_info si
        CROSS JOIN activity_info ai
        ON CONFLICT (operator_id) DO UPDATE SET
            first_activity_at = EXCLUDED.first_activity_at,
            first_activity_block = EXCLUDED.first_activity_block,
            first_activity_type = EXCLUDED.first_activity_type,
            active_avs_count = EXCLUDED.active_avs_count,
            registered_avs_count = EXCLUDED.registered_avs_count,
            active_operator_set_count = EXCLUDED.active_operator_set_count,
            total_delegators = EXCLUDED.total_delegators,
            active_delegators = EXCLUDED.active_delegators,
            total_slash_events = EXCLUDED.total_slash_events,
            last_slashed_at = EXCLUDED.last_slashed_at,
            last_allocation_at = EXCLUDED.last_allocation_at,
            last_activity_at = EXCLUDED.last_activity_at,
            updated_at = EXCLUDED.updated_at
        """

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"Aggregating state {idx}/{len(changed_operators)}: {operator_id}"
            )
        db.execute_update(aggregate_query, {"operator_id": operator_id}, db="analytics")

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    current_time = datetime.now(timezone.utc)

    db.execute_update(
        config.get_update_checkpoint_query(),
        {
            "pipeline_name": config.checkpoint_key,
            "last_processed_at": current_time,
            "last_processed_block": 0,
            "operators_processed_count": len(changed_operators),
            "total_events_processed": 0,
            "run_duration_seconds": duration,
            "run_metadata": json.dumps(
                {
                    "strategy_state_updates": strategy_state,
                    "allocations_updates": allocations,
                    "avs_relationships_updates": avs_relationships,
                    "commission_pi_updates": commission_pi,
                    "commission_avs_updates": commission_avs,
                    "commission_operator_set_updates": commission_operator_set,
                    "delegators_updates": delegators,
                    "delegator_shares_updates": delegator_shares,
                    "slashing_incidents_updates": slashing_incidents,
                    "slashing_amounts_updates": slashing_amounts,
                }
            ),
        },
        db="analytics",
    )

    context.log.info(f"Checkpoint updated at {current_time}")
    return len(changed_operators)
