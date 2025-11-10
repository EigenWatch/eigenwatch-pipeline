# analytics_pipeline/assets/extraction.py
"""
Extraction Assets - Identify operators with changes since last run
"""

from dagster import asset, OpExecutionContext, Output, MetadataValue
from datetime import datetime, timezone
from typing import Set
from ..resources import DatabaseResource, ConfigResource
from utils.sql_queries import get_operators_since_last_run


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
        db="analytics",  # TODO: review db name
    )

    if checkpoint_result:
        last_processed_at = checkpoint_result[0][0]
        context.log.info(f"Last processed at: {last_processed_at}")
    else:
        # First run - use a far past date
        last_processed_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        context.log.info("First pipeline run - processing all operators")

    results = db.execute_query(
        get_operators_since_last_run,
        {"last_processed_at": last_processed_at},
        db="events",  # TODO: review db name
    )

    changed_operators = {row[0] for row in results}

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    context.log.info(
        f"Found {len(changed_operators)} operators with changes "
        f"since {last_processed_at}"
    )

    # Early exit if no changes
    if not changed_operators:
        context.log.info("No operators to process - exiting early")
        return changed_operators

    # Log metadata
    return Output(
        changed_operators,
        metadata={
            "num_operators": len(changed_operators),
            "last_processed_at": str(last_processed_at),
            "query_duration_seconds": duration,
            "sample_operators": (
                MetadataValue.text("\n".join(list(changed_operators)[:5]))
                if changed_operators
                else "None"
            ),
        },
    )
