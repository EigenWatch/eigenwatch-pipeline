from dagster import asset, OpExecutionContext, AssetIn
from typing import Set

from pipeline.services.processors.process_operators import process_operators
from pipeline.services.reconstructors.commission_pi import CommissionPIReconstructor
from pipeline.services.reconstructors.commission_avs import CommissionAVSReconstructor
from pipeline.services.reconstructors.commission_operator_set import (
    CommissionOperatorSetReconstructor,
)
from pipeline.services.reconstructors.commission_history import (
    CommissionHistoryReconstructor,
)
from pipeline.defs.resources import DatabaseResource, ConfigResource


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
