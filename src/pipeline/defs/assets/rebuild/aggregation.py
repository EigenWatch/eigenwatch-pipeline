import json
import os
from dagster import asset, OpExecutionContext, AssetIn
from datetime import datetime, timezone
from typing import Set

from pipeline.defs.resources import DatabaseResource, ConfigResource


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

    # Load SQL query from file
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "sql",
        "aggregate_operator_state.sql",
    )

    with open(sql_path, "r") as f:
        aggregate_query = f.read()

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
