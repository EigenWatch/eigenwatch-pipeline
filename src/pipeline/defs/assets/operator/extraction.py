# from dagster import asset, OpExecutionContext
# import pandas as pd

# from pipeline.defs.resources import SubgraphDBResource
# from utils.normalizers import normalize_bytes_columns


# @asset(group_name="extraction")
# def extract_operators(
#     context: OpExecutionContext, subgraph_db: SubgraphDBResource
# ) -> pd.DataFrame:
#     """Extract current operator state from subgraph database"""

#     query = "SELECT * FROM {deployment_hash}.operator"
#     df = subgraph_db.execute_query(query)
#     context.log.info(f"Extracted {len(df)} operators")

#     # Convert column names to snake_case
#     df.columns = [
#         "vid",
#         "block_range",
#         "id",
#         "address",
#         "delegation_approver",
#         "metadata_uri",
#         "registered_at",
#         "registered_at_block",
#         "registered_at_transaction",
#         "is_registered",
#         "delegator_count",
#         "avs_registration_count",
#         "operator_set_count",
#         "slashing_event_count",
#         "registration_event",
#         "last_activity_at",
#         "updated_at",
#     ]

#     # Normalize any bytes-like values to hex
#     df = normalize_bytes_columns(df)

#     # Convert timestamps
#     for ts_col in ["registered_at", "last_activity_at", "updated_at"]:
#         if ts_col in df.columns:
#             df[ts_col] = pd.to_datetime(df[ts_col], unit="s", errors="coerce")

#     return df


# @asset(group_name="extraction")
# def extract_staker_delegations(
#     context: OpExecutionContext, subgraph_db: SubgraphDBResource
# ) -> pd.DataFrame:
#     """Extract delegation relationships between stakers and operators"""

#     query = "SELECT * FROM {deployment_hash}.staker_delegation"
#     df = subgraph_db.execute_query(query)
#     context.log.info(f"Extracted {len(df)} staker delegation records")

#     # Convert column names to snake_case
#     df.columns = [
#         "vid",
#         "block_range",
#         "id",
#         "staker",
#         "operator",
#         "delegation_type",
#         "transaction_hash",
#         "block_number",
#         "block_timestamp",
#         "log_index",
#     ]

#     # Normalize any bytes-like values to hex
#     df = normalize_bytes_columns(df)

#     # Convert timestamp
#     if "block_timestamp" in df.columns:
#         df["block_timestamp"] = pd.to_datetime(
#             df["block_timestamp"], unit="s", errors="coerce"
#         )

#     return df


# @asset(group_name="extraction")
# def extract_operator_share_events(
#     context: OpExecutionContext, subgraph_db: SubgraphDBResource
# ) -> pd.DataFrame:
#     """Extract operator share events for delegation amounts"""

#     query = "SELECT * FROM {deployment_hash}.operator_share_event"
#     df = subgraph_db.execute_query(query)
#     context.log.info(f"Extracted {len(df)} operator share events")

#     # Convert column names to snake_case
#     df.columns = [
#         "vid",
#         "block_range",
#         "id",
#         "transaction_hash",
#         "log_index",
#         "block_number",
#         "block_timestamp",
#         "contract_address",
#         "operator",
#         "staker",
#         "strategy",
#         "shares",
#         "event_type",
#     ]

#     # Normalize any bytes-like values to hex
#     df = normalize_bytes_columns(df)

#     # Convert timestamp
#     if "block_timestamp" in df.columns:
#         df["block_timestamp"] = pd.to_datetime(
#             df["block_timestamp"], unit="s", errors="coerce"
#         )

#     # Convert shares to numeric
#     if "shares" in df.columns:
#         df["shares"] = pd.to_numeric(df["shares"], errors="coerce")

#     return df


# @asset(group_name="extraction")
# def extract_operator_slashed_events(
#     context: OpExecutionContext, subgraph_db: SubgraphDBResource
# ) -> pd.DataFrame:
#     """Extract slashing events for operators"""

#     query = "SELECT * FROM {deployment_hash}.operator_slashed"
#     df = subgraph_db.execute_query(query)
#     context.log.info(f"Extracted {len(df)} operator slashing events")

#     # Convert column names to snake_case
#     df.columns = [
#         "vid",
#         "block_range",
#         "id",
#         "transaction_hash",
#         "log_index",
#         "block_number",
#         "block_timestamp",
#         "contract_address",
#         "operator",
#         "operator_set",
#         "strategies",
#         "wad_slashed",
#         "description",
#     ]

#     # Normalize any bytes-like values to hex
#     df = normalize_bytes_columns(df)

#     # Convert timestamp
#     if "block_timestamp" in df.columns:
#         df["block_timestamp"] = pd.to_datetime(
#             df["block_timestamp"], unit="s", errors="coerce"
#         )

#     return df


# @asset(group_name="extraction")
# def extract_operator_commission_events(
#     context: OpExecutionContext, subgraph_db: SubgraphDBResource
# ) -> pd.DataFrame:
#     """Extract operator commission change events"""

#     query = "SELECT * FROM {deployment_hash}.operator_commission_event"
#     df = subgraph_db.execute_query(query)
#     context.log.info(f"Extracted {len(df)} operator commission events")

#     # Convert column names to snake_case
#     df.columns = [
#         "vid",
#         "block_range",
#         "id",
#         "transaction_hash",
#         "log_index",
#         "block_number",
#         "block_timestamp",
#         "contract_address",
#         "operator",
#         "caller",
#         "commission_type",
#         "activated_at",
#         "old_commission_bips",
#         "new_commission_bips",
#         "target_avs",
#         "target_operator_set",
#     ]

#     # Normalize any bytes-like values to hex
#     df = normalize_bytes_columns(df)

#     # Convert timestamps
#     for ts_col in ["block_timestamp", "activated_at"]:
#         if ts_col in df.columns:
#             df[ts_col] = pd.to_datetime(df[ts_col], unit="s", errors="coerce")

#     # Convert commission values to numeric
#     for col in ["old_commission_bips", "new_commission_bips"]:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")

#     return df


# @asset(group_name="extraction")
# def extract_staker_delegation_events(
#     context: OpExecutionContext, subgraph_db: SubgraphDBResource
# ) -> pd.DataFrame:
#     """Extract raw staker delegation events"""

#     query = "SELECT * FROM {deployment_hash}.staker_delegation_event"
#     df = subgraph_db.execute_query(query)
#     context.log.info(f"Extracted {len(df)} staker delegation events")

#     # Convert column names to snake_case
#     df.columns = [
#         "vid",
#         "block_range",
#         "id",
#         "transaction_hash",
#         "log_index",
#         "block_number",
#         "block_timestamp",
#         "contract_address",
#         "staker",
#         "operator",
#         "delegation_type",
#     ]

#     # Normalize any bytes-like values to hex
#     df = normalize_bytes_columns(df)

#     # Convert timestamp
#     if "block_timestamp" in df.columns:
#         df["block_timestamp"] = pd.to_datetime(
#             df["block_timestamp"], unit="s", errors="coerce"
#         )

#     return df
