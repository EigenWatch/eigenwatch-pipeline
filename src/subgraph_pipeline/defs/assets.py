"""
Event extraction and loading assets.
Combines extraction, transformation, entity upserts, and event loading.
"""

from typing import Dict, Any, Optional
import dagster as dg
import pandas as pd
import os

from subgraph_pipeline.config.allocation_manager import ALLOCATION_MANAGER_EVENT_CONFIGS
from subgraph_pipeline.config.avs_directory import AVS_DIRECTORY_EVENT_CONFIGS
from subgraph_pipeline.config.delegation_manager import DELEGATION_MANAGER_EVENT_CONFIGS
from subgraph_pipeline.config.eigenpod_manager import EIGENPOD_MANAGER_EVENT_CONFIGS
from subgraph_pipeline.config.event_config import EventConfig
from subgraph_pipeline.config.rewards_coordinator import (
    REWARDS_COORDINATOR_EVENT_CONFIGS,
)
from subgraph_pipeline.config.strategy_manager import STRATEGY_MANAGER_EVENT_CONFIGS
from subgraph_pipeline.database.database_client import DatabaseClient
from subgraph_pipeline.database.entity_manager import EntityManager
from subgraph_pipeline.database.event_loader import EventLoader
from subgraph_pipeline.utils.event_transformers import EventTransformer
from subgraph_pipeline.utils.query_builder import SubgraphQueryBuilder
from subgraph_pipeline.utils.subgraph_client import SubgraphClient
from subgraph_pipeline.utils.debug_print import debug_print

from dotenv import load_dotenv

load_dotenv()


def create_event_extraction_and_load_assets(
    config: EventConfig,
    first: int = 100,
    order_by: str = "blockNumber",
    order_direction: str = "asc",
    upstream_dependency: Optional[Any] = None,
) -> list[dg.AssetsDefinition]:
    """
    Factory function to create event extraction + load assets.

    This creates multiple assets that:
    1. Extract events from subgraph
    2. Transform data (flatten, type conversions)
    3. Upsert dependent entities (Operator, etc.)
    4. Load events into database

    Args:
        config: EventConfig from event_registry
        first: Number of records to fetch per query
        order_by: Field to order results by
        order_direction: 'asc' or 'desc'

    Returns:
        List of Dagster AssetsDefinition
    """

    # Define asset names for dependency resolution
    extract_asset_name = f"extract_{config['table_name']}"
    transform_asset_name = f"transform_{config['table_name']}"
    upsert_asset_name = f"upsert_entities_{config['table_name']}"
    load_asset_name = f"load_{config['table_name']}"

    # Build the dependency list for the extract asset
    # This ensures it waits for the previous group to complete
    extract_deps = {}
    if upstream_dependency is not None:
        extract_deps = {"upstream_completion": dg.AssetIn(key=upstream_dependency)}

    @dg.asset(
        name=extract_asset_name,
        ins=extract_deps,
        group_name=config["group_name"],
    )
    def _extract_event(
        context: dg.OpExecutionContext,
        query_builder: SubgraphQueryBuilder,
        subgraph_client: SubgraphClient,
        db_client: DatabaseClient,
        event_loader: EventLoader,
        **kwargs,
    ) -> Dict[str, Any] | None:
        """
        Extract {config['graphql_name']} events from subgraph.
        """

        # ========================================
        # STEP 1: EXTRACT FROM SUBGRAPH
        # ========================================
        context.log.info(f"Extracting {config['graphql_name']} from subgraph...")

        # Check for last processed block for incremental loading
        with db_client.get_session() as session:
            # Get last cursor from DB (blockNumber + logIndex)
            last_cursor = event_loader.get_last_cursor(session, config["table_name"])

        # Default values
        cursor = None
        block_number_gte = None

        # Prefer cursor-based pagination if available
        if last_cursor:
            block_number, log_index = last_cursor
            cursor = {
                "blockNumber": block_number,
                "logIndex": log_index or 0,  # fallback to 0 if missing
            }
            context.log.info(
                f"Incremental load using cursor: block {cursor['blockNumber']}, logIndex {cursor['logIndex']}"
            )
        else:
            # Fallback: start from next block after last processed
            with db_client.get_session() as session:
                last_block = event_loader.get_last_processed_block(
                    session, config["table_name"]
                )
            if last_block is not None:
                block_number_gte = last_block + 1
                context.log.info(
                    f"Incremental load: starting from block {block_number_gte}"
                )
            else:
                context.log.info(
                    "No previous cursor or block found — full load will run."
                )

        # ✅ Build query dynamically
        query = query_builder.build_query(
            event_name=config["graphql_name"],
            fields=config["fields"],
            first=first,
            order_by=order_by,
            order_direction=order_direction,
            nested_fields=config.get("nested_fields"),
            cursor=cursor if cursor else None,
            block_number_gte=(
                None
                if cursor
                else (block_number_gte if last_block is not None else None)
            ),
        )

        debug_print(query)

        try:
            response = subgraph_client.query(query)
        except Exception as e:
            context.log.error(f"Subgraph query failed: {e}")
            raise

        # Extract event data
        data = response.get("data", {}).get(config["graphql_name"], [])
        if not data:
            context.log.warning(f"No new {config['graphql_name']} found.")
            return None

        df = pd.DataFrame(data)
        context.log.info(f"Fetched {len(df)} {config['graphql_name']} events.")
        debug_print(df.head())

        return {"df": df, "data": data}

    @dg.asset(
        name=transform_asset_name,
        group_name=config["group_name"],
        ins={
            "extract_output": dg.AssetIn(key=extract_asset_name),
        },
    )
    def _transform_event(
        context: dg.OpExecutionContext,
        extract_output: Dict[str, Any] | None,
        transformer: EventTransformer,
    ) -> pd.DataFrame | None:
        """
        Transform extracted {config['graphql_name']} event data.
        """

        if extract_output is None:
            return None

        # ========================================
        # STEP 2: TRANSFORM DATA
        # ========================================
        context.log.info("Transforming event data...")

        df = extract_output["df"]
        data = extract_output["data"]

        df_transformed = transformer.transform_event_data(
            df=df,
            config=config,
            original_data=data,  # Keep original for raw_data column
        )

        return df_transformed

    @dg.asset(
        name=upsert_asset_name,
        group_name=config["group_name"],
        ins={
            "transform_output": dg.AssetIn(key=transform_asset_name),
        },
    )
    def _upsert_entities(
        context: dg.OpExecutionContext,
        transform_output: pd.DataFrame | None,
        db_client: DatabaseClient,
        entity_manager: EntityManager,
    ) -> Dict[str, Any]:
        """
        Upsert entities for {config['graphql_name']} events.
        """

        if transform_output is None:
            return {}

        # ========================================
        # STEP 3: UPSERT ENTITIES
        # ========================================
        context.log.info("Upserting dependent entities...")

        entity_stats = {}

        with db_client.get_session() as session:
            for entity_type in config["entity_dependencies"]:
                # Extract entity IDs using configured extractor
                extractor = config["entity_extractors"].get(entity_type)
                if not extractor:
                    context.log.warning(
                        f"No extractor defined for entity type: {entity_type}"
                    )
                    continue

                try:
                    entity_ids = extractor(transform_output)

                    # Call appropriate upsert method
                    if entity_type == "Operator":
                        stats = entity_manager.upsert_operators(
                            session, entity_ids, context
                        )
                    elif entity_type == "Staker":
                        stats = entity_manager.upsert_stakers(
                            session, entity_ids, context
                        )
                    elif entity_type == "AVS":
                        stats = entity_manager.upsert_avs(session, entity_ids, context)
                    elif entity_type == "Strategy":
                        stats = entity_manager.upsert_strategies(
                            session, entity_ids, context
                        )
                    elif entity_type == "OperatorSet":
                        # Extract operator set data (returns List[Dict])
                        operator_set_data = extractor(transform_output)
                        stats = entity_manager.upsert_operator_sets(
                            session, operator_set_data, context
                        )
                    elif entity_type == "EigenPod":
                        # Extract eigen pod data (returns List[Dict])
                        pod_data = extractor(transform_output)
                        stats = entity_manager.upsert_eigen_pods(
                            session, pod_data, context
                        )
                    else:
                        context.log.warning(f"Unknown entity type: {entity_type}")
                        stats = {"inserted": 0, "updated": 0, "skipped": 0}

                    entity_stats[entity_type] = stats

                except Exception as e:
                    context.log.error(f"Failed to upsert {entity_type}: {e}")
                    entity_stats[entity_type] = {"error": str(e)}

        return entity_stats

    @dg.asset(
        name=load_asset_name,
        group_name=config["group_name"],
        metadata={
            "event_type": config["graphql_name"],
            "table": config["table_name"],
            "contract": config["contract_source"],
            "entities": config["entity_dependencies"],
        },
        ins={
            "transform_output": dg.AssetIn(key=transform_asset_name),
            "upsert_output": dg.AssetIn(key=upsert_asset_name),
        },
    )
    def _load_event(
        context: dg.OpExecutionContext,
        transform_output: pd.DataFrame | None,
        upsert_output: Dict[str, Any],
        db_client: DatabaseClient,
        event_loader: EventLoader,
    ) -> Dict[str, Any]:
        """
        Load transformed {config['graphql_name']} events into {config['table_name']}.
        """

        if transform_output is None:
            context.log.info(f"No new data to load into {config['table_name']}.")

            with db_client.get_session() as session:
                last_block = event_loader.get_last_processed_block(
                    session, config["table_name"]
                )

            result = {
                "status": "no_new_data",
                "events_fetched": 0,
                "events_inserted": 0,
                "events_updated": 0,
                "events_skipped": 0,
                "events_errors": 0,
                "entities_upserted": {},
                "last_block_processed": last_block,
            }

            context.add_output_metadata(
                {
                    "events_fetched": dg.MetadataValue.int(result["events_fetched"]),
                    "events_inserted": dg.MetadataValue.int(result["events_inserted"]),
                    "last_block": dg.MetadataValue.int(
                        int(result.get("last_block_processed") or 0)
                    ),
                    "entities": dg.MetadataValue.json(result["entities_upserted"]),
                }
            )

            return result

        df_transformed = transform_output
        entity_stats = upsert_output

        # ========================================
        # STEP 4: LOAD EVENTS
        # ========================================
        context.log.info(f"Loading events into {config['table_name']}...")

        with db_client.get_session() as session:
            try:
                load_stats = event_loader.load_events(
                    session=session,
                    df=df_transformed,
                    table_name=config["table_name"],
                    context=context,
                )
            except Exception as e:
                context.log.error(f"Failed to load events: {e}")
                raise

        # ========================================
        # STEP 5: RETURN METADATA
        # ========================================
        result = {
            "status": "success",
            "events_fetched": len(df_transformed),
            "events_inserted": load_stats["inserted"],
            "events_updated": load_stats["updated"],
            "events_skipped": load_stats["skipped"],
            "events_errors": load_stats["errors"],
            "entities_upserted": entity_stats,
            "last_block_processed": df_transformed["block_number"].max(),
        }

        context.log.info(f"Asset completed successfully: {result}")

        # Log to Dagster metadata for visibility in UI
        context.add_output_metadata(
            {
                "events_fetched": dg.MetadataValue.int(result["events_fetched"]),
                "events_inserted": dg.MetadataValue.int(result["events_inserted"]),
                "last_block": dg.MetadataValue.int(
                    int(result.get("last_block_processed") or 0)
                ),
                "entities": dg.MetadataValue.json(result["entities_upserted"]),
            }
        )

        return result

    return [_extract_event, _transform_event, _upsert_entities, _load_event]


# Generate selected assets programmatically
def generate_event_assets(selected_event_configs: Dict[str, Dict[str, Any]]):
    """
    Generate event assets from a provided subset of event configs.

    Args:
        selected_event_configs: Dictionary mapping event_name -> config to generate assets for.

    Returns:
        List of generated asset names.
    """
    assets = []
    previous_group_final_asset = None

    for i, (event_name, config) in enumerate(selected_event_configs.items()):
        # Create assets for this event group
        event_assets = create_event_extraction_and_load_assets(
            config=config,
            first=int(os.getenv("RESULTS_PER_QUERY")) or 1,
            upstream_dependency=previous_group_final_asset,
        )

        assets.extend(event_assets)

        # The last asset (_load_event) becomes the dependency for the next group
        previous_group_final_asset = f"load_{config['table_name']}"

        # Optional logging
        if i == 0:
            print(f"Starting chain with {event_name}")
        else:
            print(f"Chaining {event_name} after previous group")

    print(
        f"Generated {len(assets)} assets across {len(selected_event_configs)} event groups"
    )
    return assets


# -----------------------------
# Generate assets for each event group
# -----------------------------
strategy_manager_event_assets = generate_event_assets(STRATEGY_MANAGER_EVENT_CONFIGS)
avs_directory_event_assets = generate_event_assets(AVS_DIRECTORY_EVENT_CONFIGS)
eigenpod_manager_event_assets = generate_event_assets(EIGENPOD_MANAGER_EVENT_CONFIGS)
delegation_manager_event_assets = generate_event_assets(
    DELEGATION_MANAGER_EVENT_CONFIGS
)
allocation_manager_event_assets = generate_event_assets(
    ALLOCATION_MANAGER_EVENT_CONFIGS
)
rewards_coordinator_event_assets = generate_event_assets(
    REWARDS_COORDINATOR_EVENT_CONFIGS
)
