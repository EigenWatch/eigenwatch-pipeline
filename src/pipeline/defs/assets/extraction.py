# analytics_pipeline/assets/extraction.py
"""
Extraction Assets - Identify operators with changes since last run
"""

from dagster import asset, OpExecutionContext
from datetime import datetime, timezone
from typing import Set

from utils.operator_event_query import (
    build_operator_event_query,
    default_operator_event_tables,
)
from ..resources import DatabaseResource, ConfigResource

from dagster import asset
from typing import Set
from datetime import datetime, timezone


@asset(
    description="Identifies operators that have had events since the last pipeline run",
    compute_kind="sql",
)
def changed_operators_since_last_run(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
) -> Set[str]:
    """
    Query all event tables to find operators with events since last checkpoint.

    Returns:
        Set of operator_ids that need state rebuilding
    """
    start_time = datetime.now(timezone.utc)

    # Get last checkpoint
    checkpoint_result = db.execute_query(
        config.get_checkpoint_query(),
        {"pipeline_name": config.checkpoint_key},
        db="analytics",
    )

    if checkpoint_result:
        last_processed_at = checkpoint_result[0][0]
        context.log.info(f"Last processed at: {last_processed_at}")
    else:
        # First run - use a far past date
        last_processed_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        context.log.info("First pipeline run - processing all operators")

    query = build_operator_event_query(
        default_operator_event_tables,
        cutoff_column="created_at",
        cutoff_param=":last_processed_at",
    )
    results = db.execute_query(
        query,
        {"last_processed_at": last_processed_at},
        db="events",
    )

    changed_operators = {row[0] for row in results}

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    context.log.info(
        f"Found {len(changed_operators)} operators with changes "
        f"since {last_processed_at}"
    )
    context.log.info(
        f"Query duration: {duration:.2f}s, "
        f"Sample operators: {', '.join(list(changed_operators)[:5]) if changed_operators else 'None'}"
    )

    return changed_operators
