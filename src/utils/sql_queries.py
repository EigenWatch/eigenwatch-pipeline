# Query to find all operators with events since last run
get_operators_since_last_run = """
    WITH changed_via_allocation AS (
        SELECT DISTINCT operator_id
        FROM allocation_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_shares AS (
        SELECT DISTINCT operator_id
        FROM operator_share_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_registration AS (
        SELECT DISTINCT operator_id
        FROM operator_registered_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_metadata AS (
        SELECT DISTINCT operator_id
        FROM operator_metadata_update_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_avs_registration AS (
        SELECT DISTINCT operator_id
        FROM operator_avs_registration_status_updated_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_slashing AS (
        SELECT DISTINCT operator_id
        FROM operator_slashed_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_delegation_approver AS (
        SELECT DISTINCT operator_id
        FROM delegation_approver_updated_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_max_magnitude AS (
        SELECT DISTINCT operator_id
        FROM max_magnitude_updated_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_encumbered_magnitude AS (
        SELECT DISTINCT operator_id
        FROM encumbered_magnitude_updated_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_commission_avs AS (
        SELECT DISTINCT operator_id
        FROM operator_avs_split_bips_set_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_commission_pi AS (
        SELECT DISTINCT operator_id
        FROM operator_pi_split_bips_set_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_commission_opset AS (
        SELECT DISTINCT operator_id
        FROM operator_set_split_bips_set_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_delegators AS (
        SELECT DISTINCT operator_id
        FROM staker_delegation_events
        WHERE created_at > :last_processed_at
        
        UNION
        
        SELECT DISTINCT operator_id
        FROM staker_force_undelegated_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_operator_set_join AS (
        SELECT DISTINCT operator_id
        FROM operator_added_to_operator_set_events
        WHERE created_at > :last_processed_at
    ),
    changed_via_operator_set_leave AS (
        SELECT DISTINCT operator_id
        FROM operator_removed_from_operator_set_events
        WHERE created_at > :last_processed_at
    )
    -- Combine all sources
    SELECT operator_id FROM changed_via_allocation
    UNION
    SELECT operator_id FROM changed_via_shares
    UNION
    SELECT operator_id FROM changed_via_registration
    UNION
    SELECT operator_id FROM changed_via_metadata
    UNION
    SELECT operator_id FROM changed_via_avs_registration
    UNION
    SELECT operator_id FROM changed_via_slashing
    UNION
    SELECT operator_id FROM changed_via_delegation_approver
    UNION
    SELECT operator_id FROM changed_via_max_magnitude
    UNION
    SELECT operator_id FROM changed_via_encumbered_magnitude
    UNION
    SELECT operator_id FROM changed_via_commission_avs
    UNION
    SELECT operator_id FROM changed_via_commission_pi
    UNION
    SELECT operator_id FROM changed_via_commission_opset
    UNION
    SELECT operator_id FROM changed_via_delegators
    UNION
    SELECT operator_id FROM changed_via_operator_set_join
    UNION
    SELECT operator_id FROM changed_via_operator_set_leave
"""
