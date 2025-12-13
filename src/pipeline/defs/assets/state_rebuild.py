# defs/assets/state_rebuild.py - COMPLETE FIXED VERSION
"""
State Rebuild Assets - Reconstruct operator state from events
"""

import json
from dagster import asset, OpExecutionContext, AssetIn
from datetime import datetime, timezone
from typing import Set

from pipeline.services.processors.process_operators import process_operators
from pipeline.services.reconstructors.allocation_state import AllocationReconstructor
from pipeline.services.reconstructors.commission_history import (
    CommissionHistoryReconstructor,
)
from pipeline.services.reconstructors.delegation_approver import (
    DelegationApproverHistoryReconstructor,
)
from pipeline.services.reconstructors.metadata_history import (
    OperatorMetadataHistoryReconstructor,
)
from pipeline.services.reconstructors.strategy_state import StrategyStateReconstructor
from pipeline.services.reconstructors.avs_relationship_history import (
    AVSRelationshipHistoryReconstructor,
)
from pipeline.services.reconstructors.avs_relationship_current import (
    AVSRelationshipCurrentReconstructor,
)
from pipeline.services.reconstructors.commission_pi import CommissionPIReconstructor
from pipeline.services.reconstructors.commission_avs import CommissionAVSReconstructor
from pipeline.services.reconstructors.commission_operator_set import (
    CommissionOperatorSetReconstructor,
)
from pipeline.services.reconstructors.delegator_history import (
    DelegatorHistoryReconstructor,
)
from pipeline.services.reconstructors.delegator_current import (
    DelegatorCurrentReconstructor,
)
from pipeline.services.reconstructors.delegator_shares import (
    DelegatorSharesReconstructor,
)
from pipeline.services.reconstructors.slashing_events_cache import (
    SlashingEventsCacheReconstructor,
)
from pipeline.services.reconstructors.slashing_incidents import (
    SlashingIncidentsReconstructor,
)
from pipeline.services.reconstructors.slashing_amounts import (
    SlashingAmountsReconstructor,
)
from pipeline.services.reconstructors.registration import (
    OperatorRegistrationReconstructor,
)
from pipeline.services.reconstructors.metadata import (
    OperatorMetadataReconstructor,
)
from pipeline.services.reconstructors.avs_allocation_summary import (
    AVSAllocationSummaryReconstructor,
)

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
# Registration (2-step)
# -----------------------------


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds operator registration information (step 1 of 2)",
    compute_kind="sql",
)
def operator_registration_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> int:
    reconstructor = OperatorRegistrationReconstructor(db, context.log)
    return process_operators(
        context,
        changed_operators,
        reconstructor,
        "Building operator registration",
        config,
    )


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "registration": AssetIn("operator_registration_asset"),
    },
    description="Rebuilds delegation approver change history (step 2 of 2)",
    compute_kind="sql",
)
def operator_delegation_approver_history_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
    registration: int,
) -> int:
    reconstructor = DelegationApproverHistoryReconstructor(db, context.log)
    return process_operators(
        context,
        changed_operators,
        reconstructor,
        "Building delegation approver history",
        config,
    )


# -----------------------------
# Metadata (2-step)
# -----------------------------


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds operator metadata history (step 1 of 2)",
    compute_kind="sql",
)
def operator_metadata_history_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> int:
    reconstructor = OperatorMetadataHistoryReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building metadata history", config
    )


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "metadata_history": AssetIn("operator_metadata_history_asset"),
    },
    description="Rebuilds current operator metadata from history (step 2 of 2)",
    compute_kind="sql",
)
def operator_metadata_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
    metadata_history: int,
) -> int:
    reconstructor = OperatorMetadataReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building current metadata", config
    )


# -----------------------------
# AVS Allocation Summary
# -----------------------------


@asset(
    ins={
        "changed_operators": AssetIn("changed_operators_since_last_run"),
        "allocations": AssetIn("operator_allocations_asset"),
    },
    description="Builds AVS allocation summary per operator-avs-strategy",
    compute_kind="sql",
)
def operator_avs_allocation_summary_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
    allocations: int,
) -> int:
    reconstructor = AVSAllocationSummaryReconstructor(db, context.log)
    return process_operators(
        context,
        changed_operators,
        reconstructor,
        "Building AVS allocation summary",
        config,
    )


@asset(
    ins={"changed_operators": AssetIn("changed_operators_since_last_run")},
    description="Rebuilds commission change history for all commission types",
    compute_kind="sql",
)
def operator_commission_history_asset(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    changed_operators: Set[str],
) -> int:
    reconstructor = CommissionHistoryReconstructor(db, context.log)
    return process_operators(
        context, changed_operators, reconstructor, "Building commission history", config
    )


# -----------------------------
# Aggregator asset (COMPLETE FIXED VERSION)
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
        "registration": AssetIn("operator_registration_asset"),
        "delegation_approver_history": AssetIn(
            "operator_delegation_approver_history_asset"
        ),
        "metadata": AssetIn("operator_metadata_asset"),
        "metadata_history": AssetIn("operator_metadata_history_asset"),
        "avs_allocation_summary": AssetIn("operator_avs_allocation_summary_asset"),
    },
    description="Aggregates all state into operator_state table",
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
    registration: int,
    delegation_approver_history: int,
    metadata: int,
    metadata_history: int,
    avs_allocation_summary: int,
) -> int:
    if not changed_operators:
        context.log.info("No operators to aggregate")
        return 0

    start_time = datetime.now(timezone.utc)

    # COMPLETE FIXED AGGREGATION QUERY (Removed metadata_fetched_at)
    aggregate_query = """
        WITH operator_info AS (
            SELECT 
                id as operator_id,
                address as operator_address
            FROM operators
            WHERE id = :operator_id
        ),
        
        -- REGISTRATION & DELEGATION APPROVER
        delegation_approver_current AS (
            SELECT 
                operator_id,
                new_delegation_approver as current_delegation_approver,
                changed_at as delegation_approver_updated_at
            FROM operator_delegation_approver_history
            WHERE operator_id = :operator_id
            ORDER BY changed_at DESC, changed_at_block DESC
            LIMIT 1
        ),
        
        -- METADATA
        metadata_info AS (
            SELECT 
                operator_id,
                metadata_uri as current_metadata_uri,
                -- metadata_json removed
                -- metadata_fetched_at removed
                last_updated_at as last_metadata_update_at
            FROM operator_metadata
            WHERE operator_id = :operator_id
        ),
        
        -- REGISTRATION INFO
        registration_info AS (
            SELECT 
                operator_id,
                registered_at,
                registration_block
            FROM operator_registration
            WHERE operator_id = :operator_id
        ),
        
        -- FIRST ACTIVITY (FIXED!)
        first_activity AS (
            SELECT 
                MIN(event_time) as first_activity_at,
                MIN(event_block) as first_activity_block,
                (ARRAY_AGG(event_type ORDER BY event_time, event_block))[1] as first_activity_type
            FROM (
                SELECT registered_at as event_time, registration_block as event_block, 'REGISTRATION' as event_type
                FROM operator_registration WHERE operator_id = :operator_id
                UNION ALL
                SELECT allocated_at, allocated_at_block, 'ALLOCATION'
                FROM operator_allocations WHERE operator_id = :operator_id
                UNION ALL
                SELECT status_changed_at, status_changed_block, 'AVS_REGISTRATION'
                FROM operator_avs_registration_history WHERE operator_id = :operator_id
                UNION ALL
                SELECT event_timestamp, event_block, 'DELEGATION'
                FROM operator_delegator_history 
                WHERE operator_id = :operator_id AND delegation_type = 'DELEGATED'
                UNION ALL
                SELECT slashed_at, slashed_at_block, 'SLASHING'
                FROM operator_slashing_incidents WHERE operator_id = :operator_id
                UNION ALL
                SELECT updated_at, updated_at_block, 'METADATA_UPDATE'
                FROM operator_metadata_history WHERE operator_id = :operator_id
            ) all_events
            WHERE event_block IS NOT NULL
        ),
        
        -- LAST ACTIVITY (FIXED!)
        last_activity AS (
            SELECT GREATEST(
                COALESCE(MAX(allocated_at), '1970-01-01'::timestamp),
                COALESCE(MAX(status_changed_at), '1970-01-01'::timestamp),
                COALESCE(MAX(event_timestamp), '1970-01-01'::timestamp),
                COALESCE(MAX(slashed_at), '1970-01-01'::timestamp),
                COALESCE(MAX(updated_at), '1970-01-01'::timestamp),
                COALESCE(MAX(changed_at), '1970-01-01'::timestamp)
            ) as last_activity_at
            FROM (
                SELECT allocated_at as allocated_at, NULL::timestamp as status_changed_at, 
                       NULL::timestamp as event_timestamp, NULL::timestamp as slashed_at,
                       NULL::timestamp as updated_at, NULL::timestamp as changed_at
                FROM operator_allocations WHERE operator_id = :operator_id
                UNION ALL
                SELECT NULL, status_changed_at, NULL, NULL, NULL, NULL
                FROM operator_avs_registration_history WHERE operator_id = :operator_id
                UNION ALL
                SELECT NULL, NULL, event_timestamp, NULL, NULL, NULL
                FROM operator_delegator_history WHERE operator_id = :operator_id
                UNION ALL
                SELECT NULL, NULL, NULL, slashed_at, NULL, NULL
                FROM operator_slashing_incidents WHERE operator_id = :operator_id
                UNION ALL
                SELECT NULL, NULL, NULL, NULL, updated_at, NULL
                FROM operator_metadata_history WHERE operator_id = :operator_id
                UNION ALL
                SELECT NULL, NULL, NULL, NULL, NULL, changed_at
                FROM operator_delegation_approver_history WHERE operator_id = :operator_id
            ) all_timestamps
        ),
        
        -- PI COMMISSION (NEW!)
        pi_commission_info AS (
            SELECT 
                current_bips as current_pi_split_bips,
                current_activated_at as pi_split_activated_at
            FROM operator_commission_rates
            WHERE operator_id = :operator_id AND commission_type = 'PI'
        ),
        
        -- FORCE UNDELEGATIONS (NEW!)
        force_undelegation_info AS (
            SELECT COUNT(*) as force_undelegation_count
            FROM operator_delegator_history
            WHERE operator_id = :operator_id AND delegation_type = 'FORCE_UNDELEGATED'
        ),
        
        -- COMMISSION CHANGES (NEW!)
        commission_change_info AS (
            SELECT MAX(changed_at) as last_commission_change_at
            FROM operator_commission_history
            WHERE operator_id = :operator_id
        ),
        
        -- AVS/OPERATOR SET COUNTS
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
        
        -- DELEGATOR COUNTS
        delegator_counts AS (
            SELECT 
                COUNT(*) as total_delegators,
                COUNT(*) FILTER (WHERE is_delegated = TRUE) as active_delegators
            FROM operator_delegators
            WHERE operator_id = :operator_id
        ),
        
        -- SLASHING INFO
        slashing_info AS (
            SELECT 
                COUNT(*) as total_slash_events,
                MAX(slashed_at) as last_slashed_at
            FROM operator_slashing_incidents
            WHERE operator_id = :operator_id
        ),
        
        -- ALLOCATION INFO
        activity_info AS (
            SELECT MAX(allocated_at) as last_allocation_at
            FROM operator_allocations
            WHERE operator_id = :operator_id
        )
        
        -- FINAL INSERT
        INSERT INTO operator_state (
            operator_id, operator_address,
            current_metadata_uri, -- metadata_fetched_at removed
            registered_at, registration_block,
            first_activity_at, first_activity_block, first_activity_type,
            current_delegation_approver, is_permissioned, delegation_approver_updated_at,
            current_pi_split_bips, pi_split_activated_at,
            active_avs_count, registered_avs_count, active_operator_set_count,
            total_delegators, active_delegators,
            total_slash_events, last_slashed_at,
            force_undelegation_count,
            last_allocation_at, last_commission_change_at, last_metadata_update_at, last_activity_at,
            operational_days,
            is_active, updated_at
        )
        SELECT 
            oi.operator_id,
            oi.operator_address,
            -- Metadata
            mi.current_metadata_uri,
            -- metadata_fetched_at removed
            -- Registration
            ri.registered_at,
            ri.registration_block,
            -- First Activity
            fa.first_activity_at,
            fa.first_activity_block,
            fa.first_activity_type,
            -- Delegation Approver
            COALESCE(dac.current_delegation_approver, '0x0000000000000000000000000000000000000000'),
            CASE 
                WHEN COALESCE(dac.current_delegation_approver, '0x0000000000000000000000000000000000000000') 
                     != '0x0000000000000000000000000000000000000000'
                THEN TRUE ELSE FALSE 
            END as is_permissioned,
            dac.delegation_approver_updated_at,
            -- PI Commission
            pic.current_pi_split_bips,
            pic.pi_split_activated_at,
            -- Counts
            c.active_avs_count,
            c.registered_avs_count,
            osc.active_operator_set_count,
            dc.total_delegators,
            dc.active_delegators,
            -- Slashing
            COALESCE(si.total_slash_events, 0),
            si.last_slashed_at,
            -- Force Undelegations
            COALESCE(fui.force_undelegation_count, 0),
            -- Activity Timestamps
            ai.last_allocation_at,
            cci.last_commission_change_at,
            mi.last_metadata_update_at,
            COALESCE(la.last_activity_at, NOW()),
            -- Operational Days
            CASE 
                WHEN fa.first_activity_at IS NOT NULL 
                THEN EXTRACT(DAY FROM NOW() - fa.first_activity_at)::INTEGER
                ELSE 0
            END as operational_days,
            -- Status
            TRUE as is_active,
            NOW() as updated_at
        FROM operator_info oi
        LEFT JOIN registration_info ri ON oi.operator_id = ri.operator_id
        LEFT JOIN metadata_info mi ON oi.operator_id = mi.operator_id
        LEFT JOIN delegation_approver_current dac ON oi.operator_id = dac.operator_id
        LEFT JOIN first_activity fa ON TRUE
        LEFT JOIN last_activity la ON TRUE
        LEFT JOIN pi_commission_info pic ON TRUE
        LEFT JOIN force_undelegation_info fui ON TRUE
        LEFT JOIN commission_change_info cci ON TRUE
        CROSS JOIN counts c
        CROSS JOIN operator_set_count osc
        CROSS JOIN delegator_counts dc
        LEFT JOIN slashing_info si ON TRUE
        LEFT JOIN activity_info ai ON TRUE
        
        ON CONFLICT (operator_id) DO UPDATE SET
            current_metadata_uri = EXCLUDED.current_metadata_uri,
            -- metadata_fetched_at removed
            registered_at = EXCLUDED.registered_at,
            registration_block = EXCLUDED.registration_block,
            first_activity_at = EXCLUDED.first_activity_at,
            first_activity_block = EXCLUDED.first_activity_block,
            first_activity_type = EXCLUDED.first_activity_type,
            current_delegation_approver = EXCLUDED.current_delegation_approver,
            is_permissioned = EXCLUDED.is_permissioned,
            delegation_approver_updated_at = EXCLUDED.delegation_approver_updated_at,
            current_pi_split_bips = EXCLUDED.current_pi_split_bips,
            pi_split_activated_at = EXCLUDED.pi_split_activated_at,
            active_avs_count = EXCLUDED.active_avs_count,
            registered_avs_count = EXCLUDED.registered_avs_count,
            active_operator_set_count = EXCLUDED.active_operator_set_count,
            total_delegators = EXCLUDED.total_delegators,
            active_delegators = EXCLUDED.active_delegators,
            total_slash_events = EXCLUDED.total_slash_events,
            last_slashed_at = EXCLUDED.last_slashed_at,
            force_undelegation_count = EXCLUDED.force_undelegation_count,
            last_allocation_at = EXCLUDED.last_allocation_at,
            last_commission_change_at = EXCLUDED.last_commission_change_at,
            last_metadata_update_at = EXCLUDED.last_metadata_update_at,
            last_activity_at = EXCLUDED.last_activity_at,
            operational_days = EXCLUDED.operational_days,
            updated_at = EXCLUDED.updated_at;
        """

    # Prepare batch parameters
    params_list = [{"operator_id": op_id} for op_id in changed_operators]

    context.log.info(f"Aggregating state for {len(params_list)} operators in batch...")

    # Execute as a single batch transaction
    db.execute_batch(aggregate_query, params_list, db="analytics")

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
                    "registration_updates": registration,
                    "delegation_approver_history_updates": delegation_approver_history,
                    "metadata_updates": metadata,
                    "metadata_history_updates": metadata_history,
                    "avs_allocation_summary_updates": avs_allocation_summary,
                }
            ),
        },
        db="analytics",
    )

    context.log.info(f"Checkpoint updated at {current_time}")
    return len(changed_operators)
