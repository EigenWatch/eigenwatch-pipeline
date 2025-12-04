"""
Data transformers for event data extracted from subgraph.
Handles flattening, type conversions, and preparing data for DB insertion.
"""

from typing import Dict, List, Optional
from datetime import datetime, timezone

import dagster as dg
import pandas as pd


class EventTransformer(dg.ConfigurableResource):
    """
    Transforms raw subgraph event data into database-ready format.
    """

    def flatten_nested_fields(
        self, df: pd.DataFrame, nested_config: Optional[Dict[str, List[str]]] = None
    ) -> pd.DataFrame:
        """
        Flatten nested GraphQL objects into top-level columns.

        Example:
            Input:  {"operator": {"id": "0x123", "address": "0x123"}}
            Output: {"operator_id": "0x123", "operator_address": "0x123"}

        Args:
            df: DataFrame with nested objects
            nested_config: Dict mapping field names to their sub-fields
                          e.g., {"operator": ["id", "address"]}

        Returns:
            DataFrame with flattened columns
        """
        if not nested_config or df.empty:
            return df

        df = df.copy()

        for parent_field, sub_fields in nested_config.items():
            if parent_field not in df.columns:
                continue

            # Extract nested fields
            for sub_field in sub_fields:
                new_col_name = f"{parent_field}_{sub_field}"

                df[new_col_name] = df[parent_field].apply(
                    lambda x: x.get(sub_field) if isinstance(x, dict) else None
                )

            # Keep the parent field for raw_data, but we can also drop it
            # For now, we'll keep it

        return df

    def prepare_raw_data(
        self, df: pd.DataFrame, original_data: Optional[List[Dict]] = None
    ) -> pd.DataFrame:
        """
        Create raw_data JSONB column from original GraphQL response.

        Args:
            df: Current DataFrame
            original_data: Original list of dicts from subgraph response

        Returns:
            DataFrame with raw_data column added
        """
        if df.empty:
            return df

        df = df.copy()

        if original_data:
            # Use original data if provided
            df["raw_data"] = original_data
        else:
            # Convert current row to JSON (excluding binary columns)
            df["raw_data"] = df.apply(
                lambda row: {
                    k: v for k, v in row.to_dict().items() if not isinstance(v, bytes)
                },
                axis=1,
            )

        return df

    def add_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add created_at and updated_at timestamp columns.

        Args:
            df: DataFrame

        Returns:
            DataFrame with timestamp columns
        """
        if df.empty:
            return df

        df = df.copy()
        now = datetime.now(timezone.utc)

        df["created_at"] = now
        df["updated_at"] = now

        return df

    def rename_columns(
        self, df: pd.DataFrame, column_mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Rename columns to match database schema.

        Example:
            GraphQL:  "transactionHash"
            Database: "transaction_hash"

        Args:
            df: DataFrame
            column_mapping: Dict mapping old names to new names

        Returns:
            DataFrame with renamed columns
        """
        if df.empty:
            return df

        return df.rename(columns=column_mapping)

    def extract_entity_ids(
        self, df: pd.DataFrame, entity_type: str, id_column: str
    ) -> List[str]:
        """
        Extract unique entity IDs from a column.

        Args:
            df: DataFrame
            entity_type: Type of entity (for logging)
            id_column: Column name containing IDs

        Returns:
            List of unique IDs
        """
        if df.empty or id_column not in df.columns:
            return []

        # Handle both direct IDs and nested objects
        ids = df[id_column].apply(lambda x: x.get("id") if isinstance(x, dict) else x)

        # Filter out None values and return unique
        return [id for id in ids.unique() if id is not None]

    def transform_event_data(
        self,
        df: pd.DataFrame,
        config: Dict[str, any],
        original_data: Optional[List[Dict]] = None,
    ) -> pd.DataFrame:
        """
        Complete transformation pipeline for event data.

        Args:
            df: Raw DataFrame from subgraph
            config: Event configuration dict with transformation rules
            original_data: Original response data for raw_data column

        Returns:
            Fully transformed DataFrame ready for DB insertion
        """
        if df.empty:
            return df

        # 1. Flatten nested fields
        if config.get("nested_fields"):
            df = self.flatten_nested_fields(df, config["nested_fields"])

        # 2. Rename columns to match DB schema
        if config.get("column_mapping"):
            df = self.rename_columns(df, config["column_mapping"])

        # 3. Add raw_data JSONB
        df = self.prepare_raw_data(df, original_data)

        # 4. Add timestamps
        df = self.add_timestamps(df)

        return df
