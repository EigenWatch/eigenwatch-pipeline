"""
Dagster Jobs and Schedules for Subgraph Event Pipeline
Defines jobs for each event contract and schedules them to run 4x daily
"""

import dagster as dg

from subgraph_pipeline.defs.assets import (
    delegation_manager_event_assets,
    allocation_manager_event_assets,
    avs_directory_event_assets,
    eigenpod_manager_event_assets,
    rewards_coordinator_event_assets,
    strategy_manager_event_assets,
)

# ============================================================================
# Define Jobs for Each Event Group
# ============================================================================

delegation_manager_job = dg.define_asset_job(
    name="delegation_manager_events",
    selection=[asset.key for asset in delegation_manager_event_assets],
    description="Process all delegation manager events sequentially",
)

allocation_manager_job = dg.define_asset_job(
    name="allocation_manager_events",
    selection=[asset.key for asset in allocation_manager_event_assets],
    description="Process all allocation manager events sequentially",
)

avs_directory_job = dg.define_asset_job(
    name="avs_directory_events",
    selection=[asset.key for asset in avs_directory_event_assets],
    description="Process all AVS directory events sequentially",
)

rewards_coordinator_job = dg.define_asset_job(
    name="rewards_coordinator_events",
    selection=[asset.key for asset in rewards_coordinator_event_assets],
    description="Process all rewards coordinator events sequentially",
)

strategy_manager_job = dg.define_asset_job(
    name="strategy_manager_events",
    selection=[asset.key for asset in strategy_manager_event_assets],
    description="Process all strategy manager events sequentially",
)

eigenpod_manager_job = dg.define_asset_job(
    name="eigenpod_manager_events",
    selection=[asset.key for asset in eigenpod_manager_event_assets],
    description="Process all EigenPod manager events sequentially",
)

# ============================================================================
# Define Schedules (4x Daily, Staggered by 30 Minutes)
# ============================================================================
# Strategy: Run every 6 hours, stagger start times to avoid overlap
# Total cycle time: 6 hours
# Jobs staggered: ~30 minutes apart

delegation_manager_schedule = dg.ScheduleDefinition(
    job=delegation_manager_job,
    cron_schedule="0 0,6,12,18 * * *",  # 00:00, 06:00, 12:00, 18:00
    name="delegation_manager_4x_daily",
    description="Run delegation manager events 4 times daily at 6-hour intervals",
)

allocation_manager_schedule = dg.ScheduleDefinition(
    job=allocation_manager_job,
    cron_schedule="30 0,6,12,18 * * *",  # 00:30, 06:30, 12:30, 18:30
    name="allocation_manager_4x_daily",
    description="Run allocation manager events 4 times daily at 6-hour intervals",
)

avs_directory_schedule = dg.ScheduleDefinition(
    job=avs_directory_job,
    cron_schedule="0 1,7,13,19 * * *",  # 01:00, 07:00, 13:00, 19:00
    name="avs_directory_4x_daily",
    description="Run AVS directory events 4 times daily at 6-hour intervals",
)

rewards_coordinator_schedule = dg.ScheduleDefinition(
    job=rewards_coordinator_job,
    cron_schedule="30 1,7,13,19 * * *",  # 01:30, 07:30, 13:30, 19:30
    name="rewards_coordinator_4x_daily",
    description="Run rewards coordinator events 4 times daily at 6-hour intervals",
)

strategy_manager_schedule = dg.ScheduleDefinition(
    job=strategy_manager_job,
    cron_schedule="0 2,8,14,20 * * *",  # 02:00, 08:00, 14:00, 20:00
    name="strategy_manager_4x_daily",
    description="Run strategy manager events 4 times daily at 6-hour intervals",
)

eigenpod_manager_schedule = dg.ScheduleDefinition(
    job=eigenpod_manager_job,
    cron_schedule="30 2,8,14,20 * * *",  # 02:30, 08:30, 14:30, 20:30
    name="eigenpod_manager_4x_daily",
    description="Run EigenPod manager events 4 times daily at 6-hour intervals",
)
