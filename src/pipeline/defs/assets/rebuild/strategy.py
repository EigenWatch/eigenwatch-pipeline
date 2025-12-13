from dagster import asset, OpExecutionContext, AssetIn
from typing import Set

from pipeline.services.processors.process_operators import process_operators
from pipeline.services.reconstructors.strategy_state import StrategyStateReconstructor
from pipeline.defs.resources import DatabaseResource, ConfigResource


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
