from dagster import asset, OpExecutionContext, AssetIn
from typing import Set

from pipeline.services.processors.process_operators import process_operators
from pipeline.services.reconstructors.allocation_state import AllocationReconstructor
from pipeline.services.reconstructors.avs_allocation_summary import (
    AVSAllocationSummaryReconstructor,
)
from pipeline.defs.resources import DatabaseResource, ConfigResource


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
