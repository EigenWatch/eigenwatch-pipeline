from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)

from .assets.extraction import (
    changed_operators_since_last_run,
)

from .assets.state_rebuild import (
    operator_strategy_state_asset,
    operator_allocations_asset,
    operator_avs_history_asset,
    operator_avs_relationships_asset,
    operator_commission_pi_asset,
    operator_commission_avs_asset,
    operator_commission_operator_set_asset,
    operator_delegator_history_asset,
    operator_delegators_asset,
    operator_delegator_shares_asset,
    operator_slashing_events_cache_asset,
    operator_slashing_incidents_asset,
    operator_slashing_amounts_asset,
    operator_current_state_asset,
)

from .assets.snapshots import (
    operator_daily_snapshots_asset,
    operator_strategy_daily_snapshots_asset,
    operator_avs_relationship_snapshots_asset,
    operator_delegator_shares_snapshots_asset,
    operator_commission_rates_snapshots_asset,
    operator_allocation_snapshots_asset,
    network_daily_aggregates_asset,
)

from .assets.analytics import (
    volatility_metrics_asset,
    concentration_metrics_asset,
    operator_analytics_asset,
)

from .resources import DatabaseResource, ConfigResource


state_rebuild_assets = [
    operator_strategy_state_asset,
    operator_allocations_asset,
    operator_avs_history_asset,
    operator_avs_relationships_asset,
    operator_commission_pi_asset,
    operator_commission_avs_asset,
    operator_commission_operator_set_asset,
    operator_delegator_history_asset,
    operator_delegators_asset,
    operator_delegator_shares_asset,
    operator_slashing_events_cache_asset,
    operator_slashing_incidents_asset,
    operator_slashing_amounts_asset,
    operator_current_state_asset,
]

snapshot_assets = [
    operator_daily_snapshots_asset,
    operator_strategy_daily_snapshots_asset,
    operator_avs_relationship_snapshots_asset,
    operator_delegator_shares_snapshots_asset,
    operator_commission_rates_snapshots_asset,
    operator_allocation_snapshots_asset,
    network_daily_aggregates_asset,
]

analytics_assets = [
    volatility_metrics_asset,
    concentration_metrics_asset,
    operator_analytics_asset,
]


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


state_update_schedule = ScheduleDefinition(
    job=state_update_job,
    cron_schedule="0 */6 * * *",
    description="Run state updates every 6 hours",
)

snapshot_schedule = ScheduleDefinition(
    job=snapshot_job,
    cron_schedule="5 0 * * *",
    description="Create daily snapshots at midnight",
)

analytics_schedule = ScheduleDefinition(
    job=analytics_job,
    cron_schedule="30 0 * * *",
    description="Run analytics calculations after snapshots",
)


resources = {
    "db": DatabaseResource(),
    "config": ConfigResource(),
}


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
