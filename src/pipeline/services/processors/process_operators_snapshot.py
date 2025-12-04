from dagster import OpExecutionContext
from datetime import datetime, timezone
from pipeline.defs.resources import DatabaseResource, ConfigResource


def process_operators_for_snapshot(
    context: OpExecutionContext,
    db: DatabaseResource,
    config: ConfigResource,
    operators: set,
    reconstructor,
    snapshot_date,
    snapshot_block: int,
    snapshot_name: str,
) -> int:
    """
    Process operators for snapshot creation.

    Args:
        context: Dagster execution context
        db: Database resource
        config: Config resource
        operators: Set of operator IDs to process
        reconstructor: The reconstructor to use
        snapshot_date: Date of the snapshot
        snapshot_block: Block number of the snapshot
        snapshot_name: Name for logging

    Returns:
        Number of operators processed
    """
    if not operators:
        context.log.info(f"No operators to snapshot for {snapshot_name}")
        return 0

    start_time = datetime.now(timezone.utc)
    processed_count = 0
    total_rows_inserted = 0

    for idx, operator_id in enumerate(operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"{snapshot_name}: Snapshotting {idx}/{len(operators)}: {operator_id}"
            )

        try:
            # Fetch state as of snapshot_block
            rows = reconstructor.fetch_state_for_operator(operator_id, snapshot_block)

            if not rows:
                continue

            # Add snapshot metadata to each row
            for row in rows:
                row["snapshot_date"] = snapshot_date
                row["snapshot_block"] = snapshot_block

            # Insert into snapshot table
            inserted = reconstructor.insert_state_rows(
                operator_id, rows, is_snapshot=True
            )
            total_rows_inserted += inserted
            processed_count += 1

        except Exception as exc:
            context.log.error(
                f"{snapshot_name}: Snapshot failed for {operator_id}: {exc}"
            )
            continue

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    context.log.info(
        f"{snapshot_name}: Complete - {processed_count} operators, "
        f"{total_rows_inserted} rows, "
        f"duration: {duration:.2f}s"
    )

    return processed_count
