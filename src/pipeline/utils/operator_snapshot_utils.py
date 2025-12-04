from typing import Optional
from datetime import date
from pipeline.utils.operator_event_query import (
    build_operator_event_query,
    default_operator_event_tables,
)
from pipeline.defs.resources import DatabaseResource
from pipeline.utils.debug_log import debug_print


def get_snapshot_block_for_date(
    db: DatabaseResource, snapshot_date, event_tables: Optional[list] = None
) -> int:
    """
    Get the highest block number on or before the snapshot date.

    Args:
        db: Database resource
        snapshot_date: The date (YYYY-MM-DD) to find the block for
        event_tables: List of event table names to check

    Returns:
        The highest block number found, or 0 if no events exist
    """
    if not event_tables:
        event_tables = default_operator_event_tables

    # Convert to string if it's a date object
    if isinstance(snapshot_date, date):
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
    else:
        snapshot_date_str = snapshot_date

    # Build per-table max-block queries
    # block_timestamp is bigint (Unix timestamp in seconds)
    per_table_max_queries = [
        f"""
        SELECT MAX(block_number) as max_block
        FROM {table}
        WHERE DATE(to_timestamp(block_timestamp)) <= :snapshot_date
        """
        for table in event_tables
    ]

    # Combine all queries to find the overall max
    query = f"""
    SELECT MAX(max_block) as max_block_overall FROM (
        {" UNION ALL ".join(per_table_max_queries)}
    ) t
    """

    result = db.execute_query(
        query,
        {"snapshot_date": snapshot_date_str},
        db="events",
    )

    max_block = result[0][0] if result and result[0][0] is not None else 0

    if max_block > 0:
        print(f"Found max block {max_block} for date {snapshot_date_str}")
    else:
        print(f"WARNING: No blocks found for date {snapshot_date_str}")

    debug_print(f"Snapshot block for date {snapshot_date_str}: {max_block}")
    return max_block


def get_operators_active_by_block(
    db: DatabaseResource, snapshot_block: int, event_tables: Optional[list] = None
) -> set:
    """
    Get all operators that had any activity up to the snapshot block.
    """
    if not event_tables:
        event_tables = default_operator_event_tables

    query = build_operator_event_query(
        event_tables,
        cutoff_column="block_number",
        cutoff_param=":up_to_block",
    )

    result = db.execute_query(
        query,
        {"up_to_block": snapshot_block},
        db="events",
    )

    operator_ids = {row[0] for row in result if row[0] is not None}
    print(f"Found {len(operator_ids)} operators active up to block {snapshot_block}")

    return operator_ids
