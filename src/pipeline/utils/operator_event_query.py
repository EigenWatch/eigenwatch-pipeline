def build_operator_event_query(
    event_tables: list, cutoff_column: str, cutoff_param: str
) -> str:
    """
    Dynamically build a query to fetch operator_ids that have events in the given tables
    occurring AFTER the cutoff parameter.

    Args:
        event_tables: List of table names to check
        cutoff_column: Column to compare (e.g., 'block_number' or 'created_at')
        cutoff_param: Placeholder for cutoff value (e.g., ':last_processed_at')

    Returns:
        SQL query string
    """
    if not event_tables:
        raise ValueError("event_tables cannot be empty")

    # Build a list of SELECT statements with the "greater than" condition
    select_queries = []
    for table in event_tables:
        select_queries.append(
            f"""
            SELECT operator_id
            FROM {table}
            WHERE {cutoff_column} > {cutoff_param}
            """
        )

    # Combine them with UNION
    # UNION automatically removes duplicates between the tables
    query = "\nUNION\n".join(select_queries)

    return query


default_operator_event_tables = [
    "allocation_events",
    "operator_share_events",
    "operator_registered_events",
    "operator_metadata_update_events",
    "operator_avs_registration_status_updated_events",
    "operator_slashed_events",
    "delegation_approver_updated_events",
    "max_magnitude_updated_events",
    "encumbered_magnitude_updated_events",
    "operator_avs_split_bips_set_events",
    "operator_pi_split_bips_set_events",
    "operator_set_split_bips_set_events",
    "staker_delegation_events",
    "staker_force_undelegated_events",
    "operator_added_to_operator_set_events",
    "operator_removed_from_operator_set_events",
]
