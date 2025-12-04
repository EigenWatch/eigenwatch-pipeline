# ============================================================================
# AVS DIRECTORY EVENTS
# ============================================================================

from typing import Dict
from subgraph_pipeline.config.config_schema import EventConfig


OPERATOR_AVS_REGISTRATION_STATUS_UPDATED_CONFIG: EventConfig = {
    "graphql_name": "operatorAVSRegistrationStatusUpdateds",
    "table_name": "operator_avs_registration_status_updated_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "avs",
        "status",
    ],
    "nested_fields": {"operator": ["id", "address"], "avs": ["id", "address"]},
    "entity_dependencies": ["Operator", "AVS"],
    "entity_extractors": {
        "Operator": lambda df: df["operator"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
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
        "status": "status",
        "operator.id": "operator_id",
        "operator.address": "operator_address",
        "avs_id": "avs_id",
        "avs_address": "avs_address",
    },
    "group_name": "avs_directory_events",
    "contract_source": "AVSDirectory",
}

AVS_DIRECTORY_EVENT_CONFIGS: Dict[str, EventConfig] = {
    "operatorAVSRegistrationStatusUpdateds": OPERATOR_AVS_REGISTRATION_STATUS_UPDATED_CONFIG,
}
