"""
Unified Dagster Definitions for EigenWatch Pipeline
Combines analytics pipeline and subgraph event pipeline into single code location
"""

from dagster import Definitions, EnvVar

# ============================================================================
# Import Pipeline (Analytics) Components
# ============================================================================
from pipeline.defs import (
    changed_operators_since_last_run,
    state_rebuild_assets,
    snapshot_assets,
    analytics_assets,
    state_update_job,
    snapshot_job,
    analytics_job,
    state_update_schedule,
    snapshot_schedule,
    analytics_schedule,
    resources as pipeline_resources,
)

# ============================================================================
# Import Subgraph Pipeline Components
# ============================================================================
from subgraph_pipeline.defs.assets import (
    delegation_manager_event_assets,
    allocation_manager_event_assets,
    avs_directory_event_assets,
    eigenpod_manager_event_assets,
    rewards_coordinator_event_assets,
    strategy_manager_event_assets,
)

from subgraph_pipeline.defs.jobs import (
    delegation_manager_job,
    allocation_manager_job,
    avs_directory_job,
    eigenpod_manager_job,
    rewards_coordinator_job,
    strategy_manager_job,
    delegation_manager_schedule,
    allocation_manager_schedule,
    avs_directory_schedule,
    eigenpod_manager_schedule,
    rewards_coordinator_schedule,
    strategy_manager_schedule,
)

from subgraph_pipeline.defs.resources import get_subgraph_resources

# ============================================================================
# Combine All Assets
# ============================================================================
all_assets = [
    # Analytics pipeline assets
    changed_operators_since_last_run,
    *state_rebuild_assets,
    *snapshot_assets,
    *analytics_assets,
    # Subgraph event pipeline assets
    *delegation_manager_event_assets,
    *allocation_manager_event_assets,
    *avs_directory_event_assets,
    *eigenpod_manager_event_assets,
    *rewards_coordinator_event_assets,
    *strategy_manager_event_assets,
]

# ============================================================================
# Combine All Jobs
# ============================================================================
all_jobs = [
    # Analytics pipeline jobs
    state_update_job,
    snapshot_job,
    analytics_job,
    # Subgraph event pipeline jobs
    delegation_manager_job,
    allocation_manager_job,
    avs_directory_job,
    eigenpod_manager_job,
    rewards_coordinator_job,
    strategy_manager_job,
]

# ============================================================================
# Combine All Schedules
# ============================================================================
all_schedules = [
    # Analytics pipeline schedules
    state_update_schedule,
    snapshot_schedule,
    analytics_schedule,
    # Subgraph event pipeline schedules
    delegation_manager_schedule,
    allocation_manager_schedule,
    avs_directory_schedule,
    eigenpod_manager_schedule,
    rewards_coordinator_schedule,
    strategy_manager_schedule,
]

# ============================================================================
# Merge Resources
# ============================================================================
all_resources = {
    **pipeline_resources,
    **get_subgraph_resources(),
}

# ============================================================================
# Unified Definitions
# ============================================================================
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources=all_resources,
)
