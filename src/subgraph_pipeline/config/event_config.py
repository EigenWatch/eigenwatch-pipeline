"""
Event configuration registry for EigenWatch Subgraph.
Covers ALL events from the schema with consistent patterns.
"""

from typing import List, Dict

from subgraph_pipeline.config.config_schema import EventConfig
from subgraph_pipeline.config.allocation_manager import ALLOCATION_MANAGER_EVENT_CONFIGS
from subgraph_pipeline.config.avs_directory import AVS_DIRECTORY_EVENT_CONFIGS
from subgraph_pipeline.config.delegation_manager import DELEGATION_MANAGER_EVENT_CONFIGS
from subgraph_pipeline.config.eigenpod_manager import EIGENPOD_MANAGER_EVENT_CONFIGS
from subgraph_pipeline.config.rewards_coordinator import (
    REWARDS_COORDINATOR_EVENT_CONFIGS,
)
from subgraph_pipeline.config.strategy_manager import STRATEGY_MANAGER_EVENT_CONFIGS


# ============================================================================
# FULL REGISTRY
# ============================================================================

EVENT_CONFIGS: Dict[str, EventConfig] = {
    **STRATEGY_MANAGER_EVENT_CONFIGS,
    **REWARDS_COORDINATOR_EVENT_CONFIGS,
    **EIGENPOD_MANAGER_EVENT_CONFIGS,
    **DELEGATION_MANAGER_EVENT_CONFIGS,
    **AVS_DIRECTORY_EVENT_CONFIGS,
    **ALLOCATION_MANAGER_EVENT_CONFIGS,
}


def get_event_config(graphql_name: str) -> EventConfig:
    """Retrieve event config by GraphQL plural name."""
    if graphql_name not in EVENT_CONFIGS:
        raise ValueError(f"Unknown event type: {graphql_name}")
    return EVENT_CONFIGS[graphql_name]


def list_all_events() -> List[str]:
    """List all configured event types."""
    return list(EVENT_CONFIGS.keys())
