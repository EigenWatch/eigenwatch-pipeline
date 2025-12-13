from dagster import asset, OpExecutionContext, AssetIn
from typing import Set

from pipeline.services.processors.process_operators import process_operators
from pipeline.services.reconstructors.delegator_history import (
    DelegatorHistoryReconstructor,
)
from pipeline.services.reconstructors.delegator_current import (
    DelegatorCurrentReconstructor,
)
from pipeline.services.reconstructors.delegator_shares import (
    DelegatorSharesReconstructor,
)
from pipeline.defs.resources import DatabaseResource, ConfigResource


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
