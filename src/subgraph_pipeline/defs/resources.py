"""
Resources for Subgraph Pipeline
Provides database, subgraph client, and utility resources
"""

import dagster as dg

from subgraph_pipeline.database.database_client import DatabaseClient
from subgraph_pipeline.database.entity_manager import EntityManager
from subgraph_pipeline.database.event_loader import EventLoader
from subgraph_pipeline.utils.event_transformers import EventTransformer
from subgraph_pipeline.utils.query_builder import SubgraphQueryBuilder
from subgraph_pipeline.utils.subgraph_client import SubgraphClient


def get_subgraph_resources():
    """
    Returns all resources needed for the subgraph event pipeline

    Resources:
    - query_builder: Builds GraphQL queries for subgraph
    - subgraph_client: HTTP client for subgraph endpoint
    - db_client: PostgreSQL database client
    - entity_manager: Manages database entities
    - event_loader: Loads and upserts events
    - transformer: Transforms and cleans event data
    """
    return {
        # Query builder for dynamic subgraph GraphQL queries
        "query_builder": SubgraphQueryBuilder(),
        # Subgraph client for interacting with the GraphQL endpoint
        "subgraph_client": SubgraphClient(
            endpoint=dg.EnvVar("SUBGRAPH_ENDPOINT"),
            api_key=dg.EnvVar("SUBGRAPH_API_KEY"),
        ),
        # Database client for Postgres
        "db_client": DatabaseClient(
            connection_string=dg.EnvVar("EVENTS_DB_URL"),
            pool_size=5,
            max_overflow=10,
        ),
        # Entity manager to handle DB entity operations
        "entity_manager": EntityManager(),
        # Event loader for fetching and upserting events
        "event_loader": EventLoader(),
        # Transformer for cleaning / shaping event data
        "transformer": EventTransformer(),
    }
