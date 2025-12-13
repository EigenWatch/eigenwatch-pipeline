from dagster import asset, OpExecutionContext, AssetIn
from typing import Set

from pipeline.services.processors.process_operators import process_operators
from pipeline.services.reconstructors.metadata_history import (
    OperatorMetadataHistoryReconstructor,
)
from pipeline.services.reconstructors.metadata import (
    OperatorMetadataReconstructor,
)
from pipeline.defs.resources import DatabaseResource, ConfigResource


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
