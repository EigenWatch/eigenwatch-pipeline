from dagster import asset, OpExecutionContext, AssetIn
from typing import Set

from pipeline.services.processors.process_operators import process_operators
from pipeline.services.reconstructors.registration import (
    OperatorRegistrationReconstructor,
)
from pipeline.services.reconstructors.delegation_approver import (
    DelegationApproverHistoryReconstructor,
)
from pipeline.defs.resources import DatabaseResource, ConfigResource


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
