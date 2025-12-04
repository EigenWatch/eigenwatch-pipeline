# ============================================================================
# ALLOCATION MANAGER EVENTS
# ============================================================================

from typing import Dict
from subgraph_pipeline.config.config_schema import EventConfig

ALLOCATION_DELAY_SET_CONFIG: EventConfig = {
    "graphql_name": "allocationDelaySets",
    "table_name": "allocation_delay_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "delay",
        "effectBlock",
    ],
    "nested_fields": {"operator": ["id", "address"]},
    "entity_dependencies": ["Operator"],
    "entity_extractors": {
        "Operator": lambda df: df["operator"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist()
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "delay": "delay",
        "effectBlock": "effect_block",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

ALLOCATION_EVENT_CONFIG: EventConfig = {
    "graphql_name": "allocationEvents",
    "table_name": "allocation_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "operatorSet",
        "strategy",
        "magnitude",
        "effectBlock",
    ],
    "nested_fields": {
        "operator": ["id", "address"],
        "operatorSet": ["id", "operatorSetId"],
        "strategy": ["id", "address"],
    },
    "entity_dependencies": ["Operator", "OperatorSet", "Strategy"],
    "entity_extractors": {
        "Operator": lambda df: df["operator"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "OperatorSet": lambda df: df["operatorSet"]
        .apply(
            lambda x: (
                {
                    "id": x["id"] if isinstance(x, dict) else x,
                    "avs_id": (
                        x["id"].split("-")[0]
                        if isinstance(x, dict) and "-" in x["id"]
                        else None
                    ),
                    "operator_set_id": (
                        x.get("operatorSetId") if isinstance(x, dict) else None
                    ),
                }
                if x
                else None
            )
        )
        .dropna()
        .tolist(),
        "Strategy": lambda df: df["strategy"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "magnitude": "magnitude",
        "effectBlock": "effect_block",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "operatorSet_id": "operator_set_id",
        "operatorSet_operatorSetId": "operator_set_id_num",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

ENCUMBERED_MAGNITUDE_UPDATED_CONFIG: EventConfig = {
    "graphql_name": "encumberedMagnitudeUpdateds",
    "table_name": "encumbered_magnitude_updated_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "strategy",
        "encumberedMagnitude",
    ],
    "nested_fields": {"operator": ["id", "address"], "strategy": ["id", "address"]},
    "entity_dependencies": ["Operator", "Strategy"],
    "entity_extractors": {
        "Operator": lambda df: df["operator"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "Strategy": lambda df: df["strategy"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "encumberedMagnitude": "encumbered_magnitude",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

MAX_MAGNITUDE_UPDATED_CONFIG: EventConfig = {
    "graphql_name": "maxMagnitudeUpdateds",
    "table_name": "max_magnitude_updated_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "strategy",
        "maxMagnitude",
    ],
    "nested_fields": {"operator": ["id", "address"], "strategy": ["id", "address"]},
    "entity_dependencies": ["Operator", "Strategy"],
    "entity_extractors": {
        "Operator": lambda df: df["operator"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "Strategy": lambda df: df["strategy"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "maxMagnitude": "max_magnitude",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

OPERATOR_SLASHED_CONFIG: EventConfig = {
    "graphql_name": "operatorSlasheds",
    "table_name": "operator_slashed_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "operatorSet",
        "strategies",
        "wadSlashed",
        "description",
    ],
    "nested_fields": {
        "operator": ["id", "address"],
        "operatorSet": ["id", "operatorSetId"],
    },
    "entity_dependencies": ["Operator", "OperatorSet"],
    "entity_extractors": {
        "Operator": lambda df: df["operator"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "OperatorSet": lambda df: df["operatorSet"]
        .apply(
            lambda x: (
                {
                    "id": x["id"] if isinstance(x, dict) else x,
                    "avs_id": (
                        x["id"].split("-")[0]
                        if isinstance(x, dict) and "-" in x["id"]
                        else None
                    ),
                    "operator_set_id": (
                        x.get("operatorSetId") if isinstance(x, dict) else None
                    ),
                }
                if x
                else None
            )
        )
        .dropna()
        .tolist(),
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "strategies": "strategies",
        "wadSlashed": "wad_slashed",
        "description": "description",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "operatorSet_id": "operator_set_id",
        "operatorSet_operatorSetId": "operator_set_id_num",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

AVS_REGISTRAR_SET_CONFIG: EventConfig = {
    "graphql_name": "avsregistrarSets",
    "table_name": "avs_registrar_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "avs",
        "registrar",
    ],
    "nested_fields": {"avs": ["id", "address"]},
    "entity_dependencies": ["AVS"],
    "entity_extractors": {
        "AVS": lambda df: df["avs"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist()
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "registrar": "registrar",
        "avs_id": "avs_id",
        "avs_address": "avs_address",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

AVS_METADATA_UPDATE_CONFIG: EventConfig = {
    "graphql_name": "avsmetadataUpdates",
    "table_name": "avs_metadata_update_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "avs",
        "metadataURI",
    ],
    "nested_fields": {"avs": ["id", "address"]},
    "entity_dependencies": ["AVS"],
    "entity_extractors": {
        "AVS": lambda df: df["avs"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist()
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "metadataURI": "metadata_uri",
        "avs_id": "avs_id",
        "avs_address": "avs_address",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

OPERATOR_SET_CREATED_CONFIG: EventConfig = {
    "graphql_name": "operatorSetCreateds",
    "table_name": "operator_set_created_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operatorSet",
        "avs",
        "operatorSetId",
    ],
    "nested_fields": {"operatorSet": ["id", "operatorSetId"], "avs": ["id", "address"]},
    "entity_dependencies": ["OperatorSet", "AVS"],
    "entity_extractors": {
        "OperatorSet": lambda df: df["operatorSet"]
        .apply(
            lambda x: (
                {
                    "id": x["id"] if isinstance(x, dict) else x,
                    "avs_id": (
                        x["id"].split("-")[0]
                        if isinstance(x, dict) and "-" in x["id"]
                        else None
                    ),
                    "operator_set_id": (
                        x.get("operatorSetId") if isinstance(x, dict) else None
                    ),
                }
                if x
                else None
            )
        )
        .dropna()
        .tolist(),
        "AVS": lambda df: df["avs"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "operatorSetId": "operator_set_id_num",
        "operatorSet_id": "operator_set_id",
        "avs_id": "avs_id",
        "avs_address": "avs_address",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

OPERATOR_ADDED_TO_OPERATOR_SET_CONFIG: EventConfig = {
    "graphql_name": "operatorAddedToOperatorSets",
    "table_name": "operator_added_to_operator_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "operatorSet",
    ],
    "nested_fields": {
        "operator": ["id", "address"],
        "operatorSet": ["id", "operatorSetId"],
    },
    "entity_dependencies": ["Operator", "OperatorSet"],
    "entity_extractors": {
        "Operator": lambda df: df["operator"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "OperatorSet": lambda df: df["operatorSet"]
        .apply(
            lambda x: (
                {
                    "id": x["id"] if isinstance(x, dict) else x,
                    "avs_id": (
                        x["id"].split("-")[0]
                        if isinstance(x, dict) and "-" in x["id"]
                        else None
                    ),
                    "operator_set_id": (
                        x.get("operatorSetId") if isinstance(x, dict) else None
                    ),
                }
                if x
                else None
            )
        )
        .dropna()
        .tolist(),
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "operatorSet_id": "operator_set_id",
        "operatorSet_operatorSetId": "operator_set_id_num",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

OPERATOR_REMOVED_FROM_OPERATOR_SET_CONFIG: EventConfig = {
    "graphql_name": "operatorRemovedFromOperatorSets",
    "table_name": "operator_removed_from_operator_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "operatorSet",
    ],
    "nested_fields": {
        "operator": ["id", "address"],
        "operatorSet": ["id", "operatorSetId"],
    },
    "entity_dependencies": ["Operator", "OperatorSet"],
    "entity_extractors": {
        "Operator": lambda df: df["operator"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "OperatorSet": lambda df: df["operatorSet"]
        .apply(
            lambda x: (
                {
                    "id": x["id"] if isinstance(x, dict) else x,
                    "avs_id": (
                        x["id"].split("-")[0]
                        if isinstance(x, dict) and "-" in x["id"]
                        else None
                    ),
                    "operator_set_id": (
                        x.get("operatorSetId") if isinstance(x, dict) else None
                    ),
                }
                if x
                else None
            )
        )
        .dropna()
        .tolist(),
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "operatorSet_id": "operator_set_id",
        "operatorSet_operatorSetId": "operator_set_id_num",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

REDISTRIBUTION_ADDRESS_SET_CONFIG: EventConfig = {
    "graphql_name": "redistributionAddressSets",
    "table_name": "redistribution_address_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operatorSet",
        "redistributionRecipient",
    ],
    "nested_fields": {"operatorSet": ["id", "operatorSetId"]},
    "entity_dependencies": ["OperatorSet"],
    "entity_extractors": {
        "OperatorSet": lambda df: df["operatorSet"]
        .apply(
            lambda x: (
                {
                    "id": x["id"] if isinstance(x, dict) else x,
                    "avs_id": (
                        x["id"].split("-")[0]
                        if isinstance(x, dict) and "-" in x["id"]
                        else None
                    ),
                    "operator_set_id": (
                        x.get("operatorSetId") if isinstance(x, dict) else None
                    ),
                }
                if x
                else None
            )
        )
        .dropna()
        .tolist()
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "redistributionRecipient": "redistribution_recipient",
        "operatorSet_id": "operator_set_id",
        "operatorSet_operatorSetId": "operator_set_id_num",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

STRATEGY_OPERATOR_SET_EVENT_CONFIG: EventConfig = {
    "graphql_name": "strategyOperatorSetEvents",
    "table_name": "strategy_operator_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operatorSet",
        "strategy",
        "eventType",
    ],
    "nested_fields": {
        "operatorSet": ["id", "operatorSetId"],
        "strategy": ["id", "address"],
    },
    "entity_dependencies": ["OperatorSet", "Strategy"],
    "entity_extractors": {
        "OperatorSet": lambda df: df["operatorSet"]
        .apply(
            lambda x: (
                {
                    "id": x["id"] if isinstance(x, dict) else x,
                    "avs_id": (
                        x["id"].split("-")[0]
                        if isinstance(x, dict) and "-" in x["id"]
                        else None
                    ),
                    "operator_set_id": (
                        x.get("operatorSetId") if isinstance(x, dict) else None
                    ),
                }
                if x
                else None
            )
        )
        .dropna()
        .tolist(),
        "Strategy": lambda df: df["strategy"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
    },
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "eventType": "event_type",
        "operatorSet_id": "operator_set_id",
        "operatorSet_operatorSetId": "operator_set_id_num",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "allocation_manager_events",
    "contract_source": "AllocationManager",
}

ALLOCATION_MANAGER_EVENT_CONFIGS: Dict[str, EventConfig] = {
    "avsRegistrarSets": AVS_REGISTRAR_SET_CONFIG,
    "avsMetadataUpdates": AVS_METADATA_UPDATE_CONFIG,
    "allocationDelaySets": ALLOCATION_DELAY_SET_CONFIG,
    "encumberedMagnitudeUpdateds": ENCUMBERED_MAGNITUDE_UPDATED_CONFIG,
    "maxMagnitudeUpdateds": MAX_MAGNITUDE_UPDATED_CONFIG,
    "allocationEvents": ALLOCATION_EVENT_CONFIG,
    "operatorSlasheds": OPERATOR_SLASHED_CONFIG,
    "operatorSetCreateds": OPERATOR_SET_CREATED_CONFIG,
    "operatorAddedToOperatorSets": OPERATOR_ADDED_TO_OPERATOR_SET_CONFIG,
    "operatorRemovedFromOperatorSets": OPERATOR_REMOVED_FROM_OPERATOR_SET_CONFIG,
    "redistributionAddressSets": REDISTRIBUTION_ADDRESS_SET_CONFIG,
    "strategyOperatorSetEvents": STRATEGY_OPERATOR_SET_EVENT_CONFIG,
}
