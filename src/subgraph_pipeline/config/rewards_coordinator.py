# ============================================================================
# REWARDS COORDINATOR EVENTS
# ============================================================================

from typing import Dict
from subgraph_pipeline.config.config_schema import EventConfig


REWARDS_SUBMISSION_CONFIG: EventConfig = {
    "graphql_name": "rewardsSubmissions",
    "table_name": "rewards_submission_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "avs",
        "submitter",
        "submissionNonce",
        "rewardsSubmissionHash",
        "submissionType",
        "strategiesAndMultipliers",
        "token",
        "amount",
        "startTimestamp",
        "duration",
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
        "submitter": "submitter",
        "submissionNonce": "submission_nonce",
        "rewardsSubmissionHash": "rewards_submission_hash",
        "submissionType": "submission_type",
        "strategiesAndMultipliers": "strategies_and_multipliers",
        "token": "token",
        "amount": "amount",
        "startTimestamp": "start_timestamp",
        "duration": "duration",
        "avs_id": "avs_id",
        "avs_address": "avs_address",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

OPERATOR_DIRECTED_AVS_REWARDS_SUBMISSION_CONFIG: EventConfig = {
    "graphql_name": "operatorDirectedAVSRewardsSubmissions",
    "table_name": "operator_directed_avs_rewards_submission_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "caller",
        "avs",
        "operatorDirectedRewardsSubmissionHash",
        "submissionNonce",
        "strategiesAndMultipliers",
        "token",
        "operatorRewards",
        "startTimestamp",
        "duration",
        "description",
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
        "caller": "caller",
        "operatorDirectedRewardsSubmissionHash": "operator_directed_rewards_submission_hash",
        "submissionNonce": "submission_nonce",
        "strategiesAndMultipliers": "strategies_and_multipliers",
        "token": "token",
        "operatorRewards": "operator_rewards",
        "startTimestamp": "start_timestamp",
        "duration": "duration",
        "description": "description",
        "avs_id": "avs_id",
        "avs_address": "avs_address",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

OPERATOR_DIRECTED_OPERATOR_SET_REWARDS_SUBMISSION_CONFIG: EventConfig = {
    "graphql_name": "operatorDirectedOperatorSetRewardsSubmissions",
    "table_name": "operator_directed_operator_set_rewards_submission_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "caller",
        "operatorDirectedRewardsSubmissionHash",
        "operatorSet",
        "submissionNonce",
        "strategiesAndMultipliers",
        "token",
        "operatorRewards",
        "startTimestamp",
        "duration",
        "description",
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
        "caller": "caller",
        "operatorDirectedRewardsSubmissionHash": "operator_directed_rewards_submission_hash",
        "submissionNonce": "submission_nonce",
        "strategiesAndMultipliers": "strategies_and_multipliers",
        "token": "token",
        "operatorRewards": "operator_rewards",
        "startTimestamp": "start_timestamp",
        "duration": "duration",
        "description": "description",
        "operatorSet_id": "operator_set_id",
        "operatorSet_soperatorSetId": "operator_set_id_num",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

REWARDS_UPDATER_SET_CONFIG: EventConfig = {
    "graphql_name": "rewardsUpdaterSets",
    "table_name": "rewards_updater_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "oldRewardsUpdater",
        "newRewardsUpdater",
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
        "oldRewardsUpdater": "old_rewards_updater",
        "newRewardsUpdater": "new_rewards_updater",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

REWARDS_FOR_ALL_SUBMITTER_SET_CONFIG: EventConfig = {
    "graphql_name": "rewardsForAllSubmitterSets",
    "table_name": "rewards_for_all_submitter_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "rewardsForAllSubmitter",
        "oldValue",
        "newValue",
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
        "rewardsForAllSubmitter": "rewards_for_all_submitter",
        "oldValue": "old_value",
        "newValue": "new_value",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

ACTIVATION_DELAY_SET_CONFIG: EventConfig = {
    "graphql_name": "activationDelaySets",
    "table_name": "activation_delay_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "oldActivationDelay",
        "newActivationDelay",
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
        "oldActivationDelay": "old_activation_delay",
        "newActivationDelay": "new_activation_delay",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

DEFAULT_OPERATOR_SPLIT_BIPS_SET_CONFIG: EventConfig = {
    "graphql_name": "defaultOperatorSplitBipsSets",
    "table_name": "default_operator_split_bips_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "oldDefaultOperatorSplitBips",
        "newDefaultOperatorSplitBips",
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
        "oldDefaultOperatorSplitBips": "old_default_operator_split_bips",
        "newDefaultOperatorSplitBips": "new_default_operator_split_bips",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

OPERATOR_AVS_SPLIT_BIPS_SET_CONFIG: EventConfig = {
    "graphql_name": "operatorAVSSplitBipsSets",
    "table_name": "operator_avs_split_bips_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "caller",
        "operator",
        "avs",
        "activatedAt",
        "oldOperatorAVSSplitBips",
        "newOperatorAVSSplitBips",
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
        "caller": "caller",
        "activatedAt": "activated_at",
        "oldOperatorAVSSplitBips": "old_operator_avs_split_bips",
        "newOperatorAVSSplitBips": "new_operator_avs_split_bips",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "avs_id": "avs_id",
        "avs_address": "avs_address",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

OPERATOR_PI_SPLIT_BIPS_SET_CONFIG: EventConfig = {
    "graphql_name": "operatorPISplitBipsSets",
    "table_name": "operator_pi_split_bips_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "caller",
        "operator",
        "activatedAt",
        "oldOperatorPISplitBips",
        "newOperatorPISplitBips",
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
        "caller": "caller",
        "activatedAt": "activated_at",
        "oldOperatorPISplitBips": "old_operator_pi_split_bips",
        "newOperatorPISplitBips": "new_operator_pi_split_bips",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

OPERATOR_SET_SPLIT_BIPS_SET_CONFIG: EventConfig = {
    "graphql_name": "operatorSetSplitBipsSets",
    "table_name": "operator_set_split_bips_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "caller",
        "operator",
        "operatorSet",
        "activatedAt",
        "oldOperatorSetSplitBips",
        "newOperatorSetSplitBips",
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
        "caller": "caller",
        "activatedAt": "activated_at",
        "oldOperatorSetSplitBips": "old_operator_set_split_bips",
        "newOperatorSetSplitBips": "new_operator_set_split_bips",
        "operator_id": "operator_id",
        "operator_address": "operator_address",
        "operatorSet_id": "operator_set_id",
        "operatorSet_soperatorSetId": "operator_set_id_num",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

CLAIMER_FOR_SET_CONFIG: EventConfig = {
    "graphql_name": "claimerForSets",
    "table_name": "claimer_for_set_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "earner",
        "oldClaimer",
        "claimer",
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
        "earner": "earner",
        "oldClaimer": "old_claimer",
        "claimer": "claimer",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

DISTRIBUTION_ROOT_SUBMITTED_CONFIG: EventConfig = {
    "graphql_name": "distributionRootSubmitteds",
    "table_name": "distribution_root_submitted_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "rootIndex",
        "root",
        "rewardsCalculationEndTimestamp",
        "activatedAt",
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
        "rootIndex": "root_index",
        "root": "root",
        "rewardsCalculationEndTimestamp": "rewards_calculation_end_timestamp",
        "activatedAt": "activated_at",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

DISTRIBUTION_ROOT_DISABLED_CONFIG: EventConfig = {
    "graphql_name": "distributionRootDisableds",
    "table_name": "distribution_root_disabled_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "rootIndex",
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
        "rootIndex": "root_index",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

REWARDS_CLAIMED_CONFIG: EventConfig = {
    "graphql_name": "rewardsClaimeds",
    "table_name": "rewards_claimed_events",
    "fields": [
        "id",
        "logIndex",
        "transactionHash",
        "blockNumber",
        "blockTimestamp",
        "contractAddress",
        "root",
        "earner",
        "claimer",
        "recipient",
        "token",
        "claimedAmount",
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
        "root": "root",
        "earner": "earner",
        "claimer": "claimer",
        "recipient": "recipient",
        "token": "token",
        "claimedAmount": "claimed_amount",
    },
    "group_name": "rewards_coordinator_events",
    "contract_source": "RewardsCoordinator",
}

REWARDS_COORDINATOR_EVENT_CONFIGS: Dict[str, EventConfig] = {
    "operatorAVSSplitBipsSets": OPERATOR_AVS_SPLIT_BIPS_SET_CONFIG,
    "operatorDirectedAVSRewardsSubmissions": OPERATOR_DIRECTED_AVS_REWARDS_SUBMISSION_CONFIG,
    "rewardsSubmissions": REWARDS_SUBMISSION_CONFIG,
    "operatorDirectedOperatorSetRewardsSubmissions": OPERATOR_DIRECTED_OPERATOR_SET_REWARDS_SUBMISSION_CONFIG,
    "rewardsUpdaterSets": REWARDS_UPDATER_SET_CONFIG,
    "rewardsForAllSubmitterSets": REWARDS_FOR_ALL_SUBMITTER_SET_CONFIG,
    "activationDelaySets": ACTIVATION_DELAY_SET_CONFIG,
    "defaultOperatorSplitBipsSets": DEFAULT_OPERATOR_SPLIT_BIPS_SET_CONFIG,
    "operatorPISplitBipsSets": OPERATOR_PI_SPLIT_BIPS_SET_CONFIG,
    "operatorSetSplitBipsSets": OPERATOR_SET_SPLIT_BIPS_SET_CONFIG,
    "claimerForSets": CLAIMER_FOR_SET_CONFIG,
    "distributionRootSubmitteds": DISTRIBUTION_ROOT_SUBMITTED_CONFIG,
    "distributionRootDisableds": DISTRIBUTION_ROOT_DISABLED_CONFIG,
    "rewardsClaimeds": REWARDS_CLAIMED_CONFIG,
}
