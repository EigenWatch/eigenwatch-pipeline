from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)

import dagster as dg

from .assets.extraction import (
    changed_operators_since_last_run,
)

from .assets.state_rebuild import (
    operator_strategy_state_asset,
    operator_allocations_asset,
    operator_avs_relationships_asset,
    operator_commission_rates_asset,
    operator_delegators_asset,
    operator_slashing_incidents_asset,
    operator_current_state_asset,
)

from .assets.snapshots import (
    operator_daily_snapshots_asset,
    operator_strategy_daily_snapshots_asset,
)

from .assets.analytics import (
    volatility_metrics_asset,
    concentration_metrics_asset,
    operator_analytics_asset,
)

from .resources import DatabaseResource, ConfigResource


# analytics_pipeline/__init__.py
"""
Operator Analytics Pipeline - Dagster Implementation

Architecture:
- Entity-centric processing (query changed operators via entity tables)
- Full state reconstruction (not incremental)
- SQL-first approach with bulk operations
- Clear phase separation: Extract → Transform → Load → Analyze
"""


# Define asset groups
state_rebuild_assets = [
    operator_strategy_state_asset,
    operator_allocations_asset,
    operator_avs_relationships_asset,
    operator_commission_rates_asset,
    operator_delegators_asset,
    operator_slashing_incidents_asset,
    operator_current_state_asset,
]

snapshot_assets = [
    operator_daily_snapshots_asset,
    operator_strategy_daily_snapshots_asset,
]

analytics_assets = [
    volatility_metrics_asset,
    concentration_metrics_asset,
    operator_analytics_asset,
]


# Define jobs
state_update_job = define_asset_job(
    name="operator_state_update",
    selection=AssetSelection.assets(changed_operators_since_last_run)
    | AssetSelection.assets(*state_rebuild_assets),
    description="Extract changed operators and rebuild their state",
)

snapshot_job = define_asset_job(
    name="operator_snapshots",
    selection=AssetSelection.assets(*snapshot_assets),
    description="Create daily snapshots of operator state",
)

analytics_job = define_asset_job(
    name="operator_analytics",
    selection=AssetSelection.assets(*analytics_assets),
    description="Calculate risk scores and analytics metrics",
)


# Define schedules
state_update_schedule = ScheduleDefinition(
    job=state_update_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours
    description="Run state updates every 6 hours",
)

snapshot_schedule = ScheduleDefinition(
    job=snapshot_job,
    cron_schedule="5 0 * * *",  # 00:05 UTC daily
    description="Create daily snapshots at midnight",
)

analytics_schedule = ScheduleDefinition(
    job=analytics_job,
    cron_schedule="30 0 * * *",  # 00:30 UTC daily
    description="Run analytics calculations after snapshots",
)


# Define resources
resources = {
    "db": DatabaseResource(),
    "config": ConfigResource(),
}


# @dg.definitions
# def resources():
#     return dg.Definitions(
#         resources=resources,
#         jobs=[
#             state_update_job,
#             snapshot_job,
#             analytics_job,
#         ],
#         schedules=[
#             state_update_schedule,
#             snapshot_schedule,
#             analytics_schedule,
#         ],
#     )
# Definitions
defs = Definitions(
    assets=[
        changed_operators_since_last_run,
        *state_rebuild_assets,
        *snapshot_assets,
        *analytics_assets,
    ],
    jobs=[
        state_update_job,
        snapshot_job,
        analytics_job,
    ],
    schedules=[
        state_update_schedule,
        snapshot_schedule,
        analytics_schedule,
    ],
    resources=resources,
)
