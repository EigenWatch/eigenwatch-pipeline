# ============================================================================
# STRATEGY MANAGER EVENTS
# ============================================================================

from typing import Dict
from subgraph_pipeline.config.config_schema import EventConfig


DEPOSIT_CONFIG: EventConfig = {
    "graphql_name": "deposits",
    "table_name": "deposit_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "staker",
        "strategy",
        "shares",
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
        "shares": "shares",
        "staker_id": "staker_id",
        "staker_address": "staker_address",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "strategy_manager_events",
    "contract_source": "StrategyManager",
}

STRATEGY_WHITELISTER_CHANGED_CONFIG: EventConfig = {
    "graphql_name": "strategyWhitelisterChangeds",
    "table_name": "strategy_whitelister_changed_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "previousAddress",
        "newAddress",
    ],
    "nested_fields": None,
    "entity_dependencies": [],
    "entity_extractors": {},
    "column_mapping": {
        "logIndex": "log_index",
        "transactionHash": "transaction_hash",
        "blockNumber": "block_number",
        "blockTimestamp": "block_timestamp",
        "contractAddress": "contract_address",
        "previousAddress": "previous_address",
        "newAddress": "new_address",
    },
    "group_name": "strategy_manager_events",
    "contract_source": "StrategyManager",
}

STRATEGY_WHITELIST_EVENT_CONFIG: EventConfig = {
    "graphql_name": "strategyWhitelistEvents",
    "table_name": "strategy_whitelist_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "strategy",
        "eventType",
    ],
    "nested_fields": {"strategy": ["id", "address"]},
    "entity_dependencies": ["Strategy"],
    "entity_extractors": {
        "Strategy": lambda df: df["strategy"]
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
        "eventType": "event_type",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "strategy_manager_events",
    "contract_source": "StrategyManager",
}

BURN_OR_REDISTRIBUTABLE_SHARES_INCREASED_CONFIG: EventConfig = {
    "graphql_name": "burnOrRedistributableSharesIncreaseds",
    "table_name": "burn_or_redistributable_shares_increased_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operatorSet",
        "slashId",
        "strategy",
        "shares",
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
        "slashId": "slash_id",
        "shares": "shares",
        "operatorSet_id": "operator_set_id",
        "operatorSet_operatorSetId": "operator_set_id_num",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "strategy_manager_events",
    "contract_source": "StrategyManager",
}

BURN_OR_REDISTRIBUTABLE_SHARES_DECREASED_CONFIG: EventConfig = {
    "graphql_name": "burnOrRedistributableSharesDecreaseds",
    "table_name": "burn_or_redistributable_shares_decreased_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "operatorSet",
        "slashId",
        "strategy",
        "shares",
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
        "slashId": "slash_id",
        "shares": "shares",
        "operatorSet_id": "operator_set_id",
        "operatorSet_operatorSetId": "operator_set_id_num",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "strategy_manager_events",
    "contract_source": "StrategyManager",
}

BURNABLE_SHARES_DECREASED_CONFIG: EventConfig = {
    "graphql_name": "burnableSharesDecreaseds",
    "table_name": "burnable_shares_decreased_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "strategy",
        "shares",
    ],
    "nested_fields": {"strategy": ["id", "address"]},
    "entity_dependencies": ["Strategy"],
    "entity_extractors": {
        "Strategy": lambda df: df["strategy"]
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
        "shares": "shares",
        "strategy_id": "strategy_id",
        "strategy_address": "strategy_address",
    },
    "group_name": "strategy_manager_events",
    "contract_source": "StrategyManager",
}

STRATEGY_MANAGER_EVENT_CONFIGS: Dict[str, EventConfig] = {
    "deposits": DEPOSIT_CONFIG,
    "strategyWhitelisterChangeds": STRATEGY_WHITELISTER_CHANGED_CONFIG,
    "strategyWhitelistEvents": STRATEGY_WHITELIST_EVENT_CONFIG,
    "burnOrRedistributableSharesIncreaseds": BURN_OR_REDISTRIBUTABLE_SHARES_INCREASED_CONFIG,
    "burnOrRedistributableSharesDecreaseds": BURN_OR_REDISTRIBUTABLE_SHARES_DECREASED_CONFIG,
    "burnableSharesDecreaseds": BURNABLE_SHARES_DECREASED_CONFIG,
}
