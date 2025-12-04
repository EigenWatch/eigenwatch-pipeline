import dagster as dg

from subgraph_pipeline.database.database_client import DatabaseClient
from subgraph_pipeline.database.entity_manager import EntityManager
from subgraph_pipeline.database.event_loader import EventLoader
from subgraph_pipeline.utils.event_transformers import EventTransformer
from subgraph_pipeline.utils.query_builder import SubgraphQueryBuilder
from subgraph_pipeline.utils.subgraph_client import SubgraphClient

from subgraph_pipeline.defs.assets import (
    delegation_manager_event_assets,
    allocation_manager_event_assets,
    avs_directory_event_assets,
    eigenpod_manager_event_assets,
    rewards_coordinator_event_assets,
    strategy_manager_event_assets,
)

# -----------------------------
# Define jobs for each event group
# -----------------------------
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

# -----------------------------
# Define schedules for each job
# -----------------------------

# 3 hours total = 180 minutes, 6 jobs → stagger ~30 minutes apart

delegation_manager_schedule = dg.ScheduleDefinition(
    job=delegation_manager_job,
    cron_schedule="0 0,6,12,18 * * *",  # start at 00:00, 06:00, 12:00, 18:00
    name="delegation_manager_4x_daily",
    description="Run delegation manager events 4 times daily at 6-hour intervals",
)

allocation_manager_schedule = dg.ScheduleDefinition(
    job=allocation_manager_job,
    cron_schedule="30 0,6,12,18 * * *",  # start at 00:30, 06:30, 12:30, 18:30
    name="allocation_manager_4x_daily",
    description="Run allocation manager events 4 times daily at 6-hour intervals",
)

avs_directory_schedule = dg.ScheduleDefinition(
    job=avs_directory_job,
    cron_schedule="0 1,7,13,19 * * *",  # start at 01:00, 07:00, 13:00, 19:00
    name="avs_directory_4x_daily",
    description="Run AVS directory events 4 times daily at 6-hour intervals",
)

rewards_coordinator_schedule = dg.ScheduleDefinition(
    job=rewards_coordinator_job,
    cron_schedule="30 1,7,13,19 * * *",  # start at 01:30, 07:30, 13:30, 19:30
    name="rewards_coordinator_4x_daily",
    description="Run rewards coordinator events 4 times daily at 6-hour intervals",
)

strategy_manager_schedule = dg.ScheduleDefinition(
    job=strategy_manager_job,
    cron_schedule="0 2,8,14,20 * * *",  # start at 02:00, 08:00, 14:00, 20:00
    name="strategy_manager_4x_daily",
    description="Run strategy manager events 4 times daily at 6-hour intervals",
)

eigenpod_manager_schedule = dg.ScheduleDefinition(
    job=eigenpod_manager_job,
    cron_schedule="30 2,8,14,20 * * *",  # start at 02:30, 08:30, 14:30, 20:30
    name="eigenpod_manager_4x_daily",
    description="Run EigenPod manager events 4 times daily at 6-hour intervals",
)


# -----------------------------
# Define resources and definitions
# -----------------------------
@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            # Query builder for dynamic subgraph GraphQL queries
            "query_builder": SubgraphQueryBuilder(),
            # Subgraph client for interacting with the GraphQL endpoint
            "subgraph_client": SubgraphClient(
                endpoint=dg.EnvVar("SUBGRAPH_ENDPOINT"),
                api_key=dg.EnvVar("SUBGRAPH_API_KEY"),
            ),
            # Database client for Postgres
            "db_client": DatabaseClient(
                connection_string=dg.EnvVar("POSTGRES_CONNECTION_STRING"),
                pool_size=5,
                max_overflow=10,
            ),
            # Entity manager to handle DB entity operations
            "entity_manager": EntityManager(),
            # Event loader for fetching and upserting events
            "event_loader": EventLoader(),
            # Transformer for cleaning / shaping event data
            "transformer": EventTransformer(),
        },
        jobs=[
            delegation_manager_job,
            allocation_manager_job,
            avs_directory_job,
            eigenpod_manager_job,
            rewards_coordinator_job,
            strategy_manager_job,
        ],
        schedules=[
            delegation_manager_schedule,
            allocation_manager_schedule,
            avs_directory_schedule,
            eigenpod_manager_schedule,
            rewards_coordinator_schedule,
            strategy_manager_schedule,
        ],
    )
