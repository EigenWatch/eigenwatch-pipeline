"""
Event loader for inserting event records into PostgreSQL.
Handles deduplication, type conversions, and conflict logging.
"""

from typing import Dict, Any
import json

import dagster as dg
import pandas as pd
from sqlalchemy import Table, MetaData, case, desc, literal_column
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session


class EventLoader(dg.ConfigurableResource):
    """
    Loads event data into PostgreSQL event tables.
    Handles ON CONFLICT with detailed logging.
    """

    def load_events(
        self,
        session: Session,
        df: pd.DataFrame,
        table_name: str,
        context: dg.OpExecutionContext = None,
    ) -> Dict[str, int]:
        """
        Load events from DataFrame into specified table.

        Args:
            session: SQLAlchemy session
            df: DataFrame with event data (already transformed)
            table_name: Target table name
            context: Dagster context for logging

        Returns:
            {"inserted": X, "updated": Y, "skipped": Z, "errors": W}
        """
        if df.empty:
            return {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}

        inserted = 0
        updated = 0
        skipped = 0
        errors = 0

        # Get table metadata
        metadata = MetaData()
        metadata.reflect(bind=session.bind, only=[table_name])
        table = metadata.tables[table_name]

        # Process each row
        for idx, row in df.iterrows():
            try:
                row_data = self._prepare_row_data(row, table)

                stmt = insert(table).values(**row_data)
                update_dict = {
                    col.name: stmt.excluded[col.name]
                    for col in table.columns
                    if col.name not in ["id", "created_at"]  # Don't update created_at
                }
                update_dict["updated_at"] = stmt.excluded.updated_at

                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_=update_dict,
                    where=(
                        table.c.updated_at != stmt.excluded.updated_at
                    ),  # skip identical updates
                ).returning(
                    table.c.id,
                    # Compare created_at with updated_at from the RESULT table
                    # If they're equal, it was just inserted
                    case(
                        (
                            table.c.created_at == table.c.updated_at,
                            literal_column("'inserted'"),
                        ),
                        else_=literal_column("'updated'"),
                    ).label("action"),
                )

                result = session.execute(stmt)
                row_result = result.fetchone()

                if row_result is not None:
                    if row_result.action == "inserted":
                        inserted += 1
                    else:
                        updated += 1
                else:
                    skipped += 1

            except Exception as e:
                errors += 1
                if context:
                    context.log.warning(
                        f"Failed to load event row {idx} (id: {row.get('id', 'unknown')}): {e}"
                    )
                continue

        if context:
            context.log.info(
                f"Event load complete for {table_name}: "
                f"{inserted} inserted, {updated} updated, "
                f"{skipped} skipped, {errors} errors"
            )

        return {
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
        }

    def _prepare_row_data(self, row: pd.Series, table: Table) -> Dict[str, Any]:
        """
        Prepare row data for insertion, handling type conversions.

        Converts:
        - Dicts/lists to JSON for JSONB columns
        - Ensures proper types for numeric columns
        """
        row_data = {}

        for col in table.columns:
            col_name = col.name

            # Skip if column not in row
            if col_name not in row.index:
                continue

            value = row[col_name]

            # Handle NaN/None
            if pd.isna(value):
                row_data[col_name] = None
                continue

            # Type conversions based on column type
            col_type = str(col.type).upper()

            if "JSONB" in col_type or "JSON" in col_type:
                # Ensure it's valid JSON
                if isinstance(value, (dict, list)):
                    row_data[col_name] = value
                elif isinstance(value, str):
                    row_data[col_name] = json.loads(value)
                else:
                    row_data[col_name] = value

            elif "BIGINT" in col_type or "INTEGER" in col_type:
                # Ensure numeric
                row_data[col_name] = int(value)

            elif "ARRAY" in col_type:
                # Ensure it's a list
                if isinstance(value, list):
                    row_data[col_name] = value
                elif isinstance(value, str):
                    row_data[col_name] = json.loads(value)
                else:
                    row_data[col_name] = [value]
            else:
                # Default: use as-is
                row_data[col_name] = value

        return row_data

    def get_last_processed_id(
        self, session: Session, table_name: str, id_column: str = "id"
    ) -> str:
        """
        Get the last processed event ID for incremental loading.

        Args:
            session: SQLAlchemy session
            table_name: Event table name
            id_column: Column to order by (default: 'id')

        Returns:
            Last ID as string, or None if table is empty
        """
        metadata = MetaData()
        metadata.reflect(bind=session.bind, only=[table_name])
        table = metadata.tables[table_name]

        query = (
            session.query(table.c[id_column])
            .order_by(table.c[id_column].desc())
            .limit(1)
        )

        result = query.first()
        return result[0] if result else None

    def get_last_processed_block(
        self,
        session: Session,
        table_name: str,
    ) -> int:
        """
        Get the last processed block number for incremental loading.

        Args:
            session: SQLAlchemy session
            table_name: Event table name

        Returns:
            Last block number, or None if table is empty
        """
        metadata = MetaData()
        metadata.reflect(bind=session.bind, only=[table_name])
        table = metadata.tables[table_name]

        query = (
            session.query(table.c.block_number)
            .order_by(table.c.block_number.desc())
            .limit(1)
        )

        result = query.first()
        return result[0] if result else None

    def get_last_cursor(self, session: Session, table_name: str):
        """
        Get the last processed (block_number, log_index) for incremental loading.

        Args:
            session: SQLAlchemy session
            table_name: Event table name

        Returns:
            Tuple (block_number, log_index) or (None, None) if table is empty
        """
        metadata = MetaData()
        metadata.reflect(bind=session.bind, only=[table_name])
        table = metadata.tables[table_name]

        query = (
            session.query(table.c.block_number, table.c.log_index)
            .order_by(desc(table.c.block_number), desc(table.c.log_index))
            .limit(1)
        )

        result = query.first()
        return (result.block_number, result.log_index) if result else (None, None)
