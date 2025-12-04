# ============================================================================
# DELEGATION MANAGER EVENTS
# ============================================================================

from typing import Dict
from subgraph_pipeline.config.config_schema import EventConfig

OPERATOR_REGISTERED_CONFIG: EventConfig = {
    "graphql_name": "operatorRegistereds",
    "table_name": "operator_registered_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "delegationApprover",
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
        "delegationApprover": "delegation_approver",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
    },
    "group_name": "delegation_manager_events",
    "contract_source": "DelegationManager",
}

DELEGATION_APPROVER_UPDATED_CONFIG: EventConfig = {
    "graphql_name": "delegationApproverUpdateds",
    "table_name": "delegation_approver_updated_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "newDelegationApprover",
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
        "newDelegationApprover": "new_delegation_approver",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
    },
    "group_name": "delegation_manager_events",
    "contract_source": "DelegationManager",
}

OPERATOR_METADATA_UPDATE_CONFIG: EventConfig = {
    "graphql_name": "operatorMetadataUpdates",
    "table_name": "operator_metadata_update_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "metadataURI",
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
        "metadataURI": "metadata_uri",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
    },
    "group_name": "delegation_manager_events",
    "contract_source": "DelegationManager",
}

OPERATOR_SHARE_EVENT_CONFIG: EventConfig = {
    "graphql_name": "operatorShareEvents",
    "table_name": "operator_share_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "staker",
        "strategy",
        "shares",
        "eventType",
    ],
    "nested_fields": {
        "operator": ["id", "address"],
        "staker": ["id", "address"],
        "strategy": ["id", "address"],
    },
    "entity_dependencies": ["Operator", "Staker", "Strategy"],
    "entity_extractors": {
        "Operator": lambda df: df["operator"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "Staker": lambda df: df["staker"]
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
        "shares": "shares",
        "eventType": "event_type",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "staker_id": "staker_id",
        "staker_address": "staker_address",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "delegation_manager_events",
    "contract_source": "DelegationManager",
}

STAKER_DELEGATION_EVENT_CONFIG: EventConfig = {
    "graphql_name": "stakerDelegationEvents",
    "table_name": "staker_delegation_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "staker",
        "operator",
        "delegationType",
    ],
    "nested_fields": {"staker": ["id", "address"], "operator": ["id", "address"]},
    "entity_dependencies": ["Staker", "Operator"],
    "entity_extractors": {
        "Staker": lambda df: df["staker"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "Operator": lambda df: df["operator"]
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
        "delegationType": "delegation_type",
        "staker_id": "staker_id",
        "staker_address": "staker_address",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
    },
    "group_name": "delegation_manager_events",
    "contract_source": "DelegationManager",
}

STAKER_FORCE_UNDELEGATED_CONFIG: EventConfig = {
    "graphql_name": "stakerForceUndelegateds",
    "table_name": "staker_force_undelegated_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "staker",
        "operator",
    ],
    "nested_fields": {"staker": ["id", "address"], "operator": ["id", "address"]},
    "entity_dependencies": ["Staker", "Operator"],
    "entity_extractors": {
        "Staker": lambda df: df["staker"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "Operator": lambda df: df["operator"]
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
        "staker_id": "staker_id",
        "staker_address": "staker_address",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
    },
    "group_name": "delegation_manager_events",
    "contract_source": "DelegationManager",
}

DEPOSIT_SCALING_FACTOR_UPDATED_CONFIG: EventConfig = {
    "graphql_name": "depositScalingFactorUpdateds",
    "table_name": "deposit_scaling_factor_updated_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "staker",
        "strategy",
        "newDepositScalingFactor",
    ],
    "nested_fields": {"staker": ["id", "address"], "strategy": ["id", "address"]},
    "entity_dependencies": ["Staker", "Strategy"],
    "entity_extractors": {
        "Staker": lambda df: df["staker"]
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
        "newDepositScalingFactor": "new_deposit_scaling_factor",
        "staker_id": "staker_id",
        "staker_address": "staker_address",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "delegation_manager_events",
    "contract_source": "DelegationManager",
}

WITHDRAWAL_EVENT_CONFIG: EventConfig = {
    "graphql_name": "withdrawalEvents",
    "table_name": "withdrawal_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "withdrawalRoot",
        "staker",
        "delegatedTo",
        "withdrawer",
        "nonce",
        "startBlock",
        "strategies",
        "shares",
        "eventType",
    ],
    "nested_fields": {"staker": ["id", "address"], "delegatedTo": ["id", "address"]},
    "entity_dependencies": ["Staker", "Operator"],
    "entity_extractors": {
        "Staker": lambda df: df["staker"]
        .apply(lambda x: x["id"] if isinstance(x, dict) else x)
        .dropna()
        .unique()
        .tolist(),
        "Operator": lambda df: df["delegatedTo"]
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
        "withdrawalRoot": "withdrawal_root",
        "withdrawer": "withdrawer",
        "nonce": "nonce",
        "startBlock": "start_block",
        "strategies": "strategies",
        "shares": "shares",
        "eventType": "event_type",
        "staker_id": "staker_id",
        "staker_address": "staker_address",
        "delegatedTo_id": "delegated_to_id",
        "delegatedTo_address": "delegated_to_address",
    },
    "group_name": "delegation_manager_events",
    "contract_source": "DelegationManager",
}

OPERATOR_SHARES_SLASHED_CONFIG: EventConfig = {
    "graphql_name": "operatorSharesSlasheds",
    "table_name": "operator_shares_slashed_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operator",
        "strategy",
        "totalSlashedShares",
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
        "totalSlashedShares": "total_slashed_shares",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "delegation_manager_events",
    "contract_source": "DelegationManager",
}

DELEGATION_MANAGER_EVENT_CONFIGS: Dict[str, EventConfig] = {
    "operatorRegistereds": OPERATOR_REGISTERED_CONFIG,
    "delegationApproverUpdateds": DELEGATION_APPROVER_UPDATED_CONFIG,
    "operatorMetadataUpdates": OPERATOR_METADATA_UPDATE_CONFIG,
    "operatorShareEvents": OPERATOR_SHARE_EVENT_CONFIG,
    "stakerDelegationEvents": STAKER_DELEGATION_EVENT_CONFIG,
    "stakerForceUndelegateds": STAKER_FORCE_UNDELEGATED_CONFIG,
    "depositScalingFactorUpdateds": DEPOSIT_SCALING_FACTOR_UPDATED_CONFIG,
    "withdrawalEvents": WITHDRAWAL_EVENT_CONFIG,
    "operatorSharesSlasheds": OPERATOR_SHARES_SLASHED_CONFIG,
}
