"""
Subgraph Pipeline Definitions Module
Exports all assets, jobs, schedules, and resources for subgraph event processing
"""

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

__all__ = [
    # Asset groups
    "delegation_manager_event_assets",
    "allocation_manager_event_assets",
    "avs_directory_event_assets",
    "eigenpod_manager_event_assets",
    "rewards_coordinator_event_assets",
    "strategy_manager_event_assets",
    # Jobs
    "delegation_manager_job",
    "allocation_manager_job",
    "avs_directory_job",
    "eigenpod_manager_job",
    "rewards_coordinator_job",
    "strategy_manager_job",
    # Schedules
    "delegation_manager_schedule",
    "allocation_manager_schedule",
    "avs_directory_schedule",
    "eigenpod_manager_schedule",
    "rewards_coordinator_schedule",
    "strategy_manager_schedule",
    # Resources
    "get_subgraph_resources",
]
