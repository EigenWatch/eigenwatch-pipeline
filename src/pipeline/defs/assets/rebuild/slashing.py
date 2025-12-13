from dagster import asset, OpExecutionContext, AssetIn
from typing import Set

from pipeline.services.processors.process_operators import process_operators
from pipeline.services.reconstructors.slashing_events_cache import (
    SlashingEventsCacheReconstructor,
)
from pipeline.services.reconstructors.slashing_incidents import (
    SlashingIncidentsReconstructor,
)
from pipeline.services.reconstructors.slashing_amounts import (
    SlashingAmountsReconstructor,
)
from pipeline.defs.resources import DatabaseResource, ConfigResource


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
