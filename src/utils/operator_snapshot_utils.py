from typing import Optional
from utils.operator_event_query import (
    build_operator_event_query,
    default_operator_event_tables,
)

from pipeline.defs.resources import DatabaseResource


def get_snapshot_block_for_date(
    db: DatabaseResource, snapshot_date, event_tables: Optional[list]
) -> int:
    """
    Get the highest block number on or before the snapshot date from multiple event tables.

    Args:
        db: Database resource
        snapshot_date: The date to find the block for
        event_tables: List of event table names to check

    Returns:
        The highest block number found, or 0 if no events exist
    """
    if not event_tables:
        event_tables = default_operator_event_tables
    union_queries = [
        f"SELECT MAX(block_number) as max_block FROM {table} WHERE DATE(block_timestamp) <= :snapshot_date"
        for table in event_tables
    ]

    query = " UNION ALL ".join(union_queries) + " ORDER BY max_block DESC LIMIT 1"

    result = db.execute_query(
        query,
        {"snapshot_date": snapshot_date},
        db="events",
    )

    return result[0][0] if result and result[0][0] else 0


def get_operators_active_by_block(db: DatabaseResource, snapshot_block: int) -> set:
    """
    Get all operators that had any activity up to the snapshot block.

    Args:
        db: Database resource
        snapshot_block: Block number to check up to

    Returns:
        Set of operator IDs
    """
    query = query = build_operator_event_query(
        default_operator_event_tables,
        cutoff_column="block_number",
        cutoff_param=":up_to_block",
    )

    result = db.execute_query(
        query,
        {"up_to_block": snapshot_block},
        db="events",
    )

    return {row[0] for row in result}
