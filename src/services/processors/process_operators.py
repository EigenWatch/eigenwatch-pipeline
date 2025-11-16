from datetime import datetime, timezone
from services.reconstructors.base import BaseReconstructor


def process_operators(
    context,
    changed_operators: set[str],
    reconstructor: BaseReconstructor,
    log_prefix: str,
    config,
) -> int:
    """
    Unified operator processing.
    Uses reconstructor's fetch/insert and optional row_transformer.
    """
    if not changed_operators:
        context.log.info(f"No operators to process for {log_prefix}")
        return 0

    start_time = datetime.now(timezone.utc)
    processed_count = 0
    total_rows_fetched = 0
    total_rows_inserted = 0

    for idx, operator_id in enumerate(changed_operators, 1):
        if idx % config.log_batch_progress_every == 0:
            context.log.info(
                f"{log_prefix} {idx}/{len(changed_operators)}: {operator_id}"
            )

        try:
            rows = reconstructor.fetch_state_for_operator(operator_id)
        except Exception as exc:
            context.log.error(f"{log_prefix}: fetch failed for {operator_id}: {exc}")
            continue

        total_rows_fetched += len(rows) if rows else 0

        try:
            inserted = reconstructor.insert_state_rows(operator_id, rows)
            total_rows_inserted += inserted
        except Exception as exc:
            context.log.error(f"{log_prefix}: insert failed for {operator_id}: {exc}")
            continue

        processed_count += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    context.log.info(
        f"{log_prefix}: Processed {processed_count} operators, "
        f"rows fetched: {total_rows_fetched}, "
        f"rows inserted/updated: {total_rows_inserted}, "
        f"duration: {duration:.2f}s"
    )

    return processed_count
