# ============================================================================
# EIGEN POD MANAGER EVENTS
# ============================================================================

from typing import Dict
from subgraph_pipeline.config.config_schema import EventConfig


POD_DEPLOYED_CONFIG: EventConfig = {
    "graphql_name": "podDeployeds",
    "table_name": "pod_deployed_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "pod",
        "owner",
    ],
    "nested_fields": {"pod": ["id", "address"], "owner": ["id", "address"]},
    "entity_dependencies": ["EigenPod", "Staker"],
    "entity_extractors": {
        "EigenPod": lambda df: [
            {
                "id": row["pod"]["id"] if isinstance(row["pod"], dict) else row["pod"],
                "address": (
                    row["pod"]["address"]
                    if isinstance(row["pod"], dict)
                    else row["pod"]
                ),
                "owner_id": (
                    row["owner"]["id"]
                    if isinstance(row["owner"], dict)
                    else row["owner"]
                ),
            }
            for _, row in df.iterrows()
            if row.get("pod") is not None
        ],
        "Staker": lambda df: df["owner"]
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
        "pod_id": "pod_id",
        "pod_address": "pod_address",
        "owner_id": "owner_id",
        "owner_address": "owner_address",
    },
    "group_name": "eigen_pod_manager_events",
    "contract_source": "EigenPodManager",
}

BEACON_CHAIN_DEPOSIT_CONFIG: EventConfig = {
    "graphql_name": "beaconChainDeposits",
    "table_name": "beacon_chain_deposit_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "pod",
        "podOwner",
        "amount",
    ],
    "nested_fields": {"pod": ["id", "address"], "podOwner": ["id", "address"]},
    "entity_dependencies": ["EigenPod", "Staker"],
    "entity_extractors": {
        "EigenPod": lambda df: [
            {
                "id": row["pod"]["id"] if isinstance(row["pod"], dict) else row["pod"],
                "address": (
                    row["pod"]["address"]
                    if isinstance(row["pod"], dict)
                    else row["pod"]
                ),
                "owner_id": (
                    row["podOwner"]["id"]
                    if isinstance(row["podOwner"], dict)
                    else row["podOwner"]
                ),
            }
            for _, row in df.iterrows()
            if row.get("pod") is not None
        ],
        "Staker": lambda df: df["podOwner"]
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
        "amount": "amount",
        "pod_id": "pod_id",
        "pod_address": "pod_address",
        "podOwner_id": "pod_owner_id",
        "podOwner_address": "pod_owner_address",
    },
    "group_name": "eigen_pod_manager_events",
    "contract_source": "EigenPodManager",
}

POD_SHARES_UPDATE_CONFIG: EventConfig = {
    "graphql_name": "podSharesUpdates",
    "table_name": "pod_shares_update_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "pod",
        "podOwner",
        "sharesDelta",
        "newTotalShares",
        "updateType",
    ],
    "nested_fields": {"pod": ["id", "address"], "podOwner": ["id", "address"]},
    "entity_dependencies": ["EigenPod", "Staker"],
    "entity_extractors": {
        "EigenPod": lambda df: [
            {
                "id": row["pod"]["id"] if isinstance(row["pod"], dict) else row["pod"],
                "address": (
                    row["pod"]["address"]
                    if isinstance(row["pod"], dict)
                    else row["pod"]
                ),
                "owner_id": (
                    row["podOwner"]["id"]
                    if isinstance(row["podOwner"], dict)
                    else row["podOwner"]
                ),
            }
            for _, row in df.iterrows()
            if row.get("pod") is not None
        ],
        "Staker": lambda df: df["podOwner"]
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
        "sharesDelta": "shares_delta",
        "newTotalShares": "new_total_shares",
        "updateType": "update_type",
        "pod_id": "pod_id",
        "pod_address": "pod_address",
        "podOwner_id": "pod_owner_id",
        "podOwner_address": "pod_owner_address",
    },
    "group_name": "eigen_pod_manager_events",
    "contract_source": "EigenPodManager",
}

BEACON_CHAIN_WITHDRAWAL_CONFIG: EventConfig = {
    "graphql_name": "beaconChainWithdrawals",
    "table_name": "beacon_chain_withdrawal_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "pod",
        "podOwner",
        "shares",
        "nonce",
        "delegatedAddress",
        "withdrawer",
        "withdrawalRoot",
    ],
    "nested_fields": {"pod": ["id", "address"], "podOwner": ["id", "address"]},
    "entity_dependencies": ["EigenPod", "Staker"],
    "entity_extractors": {
        "EigenPod": lambda df: [
            {
                "id": row["pod"]["id"] if isinstance(row["pod"], dict) else row["pod"],
                "address": (
                    row["pod"]["address"]
                    if isinstance(row["pod"], dict)
                    else row["pod"]
                ),
                "owner_id": (
                    row["podOwner"]["id"]
                    if isinstance(row["podOwner"], dict)
                    else row["podOwner"]
                ),
            }
            for _, row in df.iterrows()
            if row.get("pod") is not None
        ],
        "Staker": lambda df: df["podOwner"]
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
        "nonce": "nonce",
        "delegatedAddress": "delegated_address",
        "withdrawer": "withdrawer",
        "withdrawalRoot": "withdrawal_root",
        "pod_id": "pod_id",
        "pod_address": "pod_address",
        "podOwner_id": "pod_owner_id",
        "podOwner_address": "pod_owner_address",
    },
    "group_name": "eigen_pod_manager_events",
    "contract_source": "EigenPodManager",
}

BEACON_CHAIN_ETH_WITHDRAWAL_COMPLETED_CONFIG: EventConfig = {
    "graphql_name": "beaconChainETHWithdrawalCompleteds",
    "table_name": "beacon_chain_eth_withdrawal_completed_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "podOwner",
        "shares",
        "nonce",
        "delegatedAddress",
        "withdrawer",
        "withdrawalRoot",
    ],
    "nested_fields": {"podOwner": ["id", "address"]},
    "entity_dependencies": ["Staker"],
    "entity_extractors": {
        "Staker": lambda df: df["podOwner"]
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
        "nonce": "nonce",
        "delegatedAddress": "delegated_address",
        "withdrawer": "withdrawer",
        "withdrawalRoot": "withdrawal_root",
        "podOwner_id": "pod_owner_id",
        "podOwner_address": "pod_owner_address",
    },
    "group_name": "eigen_pod_manager_events",
    "contract_source": "EigenPodManager",
}

BEACON_CHAIN_SLASHING_EVENT_CONFIG: EventConfig = {
    "graphql_name": "beaconChainSlashingEvents",
    "table_name": "beacon_chain_slashing_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "staker",
        "prevBeaconChainSlashingFactor",
        "newBeaconChainSlashingFactor",
    ],
    "nested_fields": {"staker": ["id", "address"]},
    "entity_dependencies": ["Staker"],
    "entity_extractors": {
        "Staker": lambda df: df["staker"]
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
        "prevBeaconChainSlashingFactor": "prev_beacon_chain_slashing_factor",
        "newBeaconChainSlashingFactor": "new_beacon_chain_slashing_factor",
        "staker_id": "staker_id",
        "staker_address": "staker_address",
    },
    "group_name": "eigen_pod_manager_events",
    "contract_source": "EigenPodManager",
}

BURNABLE_ETH_SHARES_INCREASED_CONFIG: EventConfig = {
    "graphql_name": "burnableETHSharesIncreaseds",
    "table_name": "burnable_eth_shares_increased_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "shares",
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
        "shares": "shares",
    },
    "group_name": "eigen_pod_manager_events",
    "contract_source": "EigenPodManager",
}

PECTRA_FORK_TIMESTAMP_SET_CONFIG: EventConfig = {
    "graphql_name": "pectraForkTimestampSets",
    "table_name": "pectra_fork_timestamp_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "newPectraForkTimestamp",
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
        "newPectraForkTimestamp": "new_pectra_fork_timestamp",
    },
    "group_name": "eigen_pod_manager_events",
    "contract_source": "EigenPodManager",
}

PROOF_TIMESTAMP_SETTER_SET_CONFIG: EventConfig = {
    "graphql_name": "proofTimestampSetterSets",
    "table_name": "proof_timestamp_setter_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "newProofTimestampSetter",
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
        "newProofTimestampSetter": "new_proof_timestamp_setter",
    },
    "group_name": "eigen_pod_manager_events",
    "contract_source": "EigenPodManager",
}


EIGENPOD_MANAGER_EVENT_CONFIGS: Dict[str, EventConfig] = {
    "podDeployeds": POD_DEPLOYED_CONFIG,
    "beaconChainDeposits": BEACON_CHAIN_DEPOSIT_CONFIG,
    "podSharesUpdates": POD_SHARES_UPDATE_CONFIG,
    "beaconChainWithdrawals": BEACON_CHAIN_WITHDRAWAL_CONFIG,
    "beaconChainETHWithdrawalCompleteds": BEACON_CHAIN_ETH_WITHDRAWAL_COMPLETED_CONFIG,
    "beaconChainSlashingEvents": BEACON_CHAIN_SLASHING_EVENT_CONFIG,
    "burnableETHSharesIncreaseds": BURNABLE_ETH_SHARES_INCREASED_CONFIG,
    "pectraForkTimestampSets": PECTRA_FORK_TIMESTAMP_SET_CONFIG,
    "proofTimestampSetterSets": PROOF_TIMESTAMP_SETTER_SET_CONFIG,
}
