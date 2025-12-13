from dagster import asset, OpExecutionContext, AssetIn
from typing import Set

from pipeline.services.processors.process_operators import process_operators
from pipeline.services.reconstructors.avs_relationship_history import (
    AVSRelationshipHistoryReconstructor,
)
from pipeline.services.reconstructors.avs_relationship_current import (
    AVSRelationshipCurrentReconstructor,
)
from pipeline.defs.resources import DatabaseResource, ConfigResource


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
