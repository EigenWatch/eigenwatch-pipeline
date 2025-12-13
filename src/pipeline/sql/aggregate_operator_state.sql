WITH operator_info AS (
    SELECT 
        id as operator_id,
        address as operator_address
    FROM operators
    WHERE id = :operator_id
),

-- REGISTRATION & DELEGATION APPROVER
delegation_approver_current AS (
    SELECT 
        operator_id,
        new_delegation_approver as current_delegation_approver,
        changed_at as delegation_approver_updated_at
    FROM operator_delegation_approver_history
    WHERE operator_id = :operator_id
    ORDER BY changed_at DESC, changed_at_block DESC
    LIMIT 1
),

-- METADATA
metadata_info AS (
    SELECT 
        operator_id,
        metadata_uri as current_metadata_uri,
        -- metadata_json removed
        -- metadata_fetched_at removed
        last_updated_at as last_metadata_update_at
    FROM operator_metadata
    WHERE operator_id = :operator_id
),

-- REGISTRATION INFO
registration_info AS (
    SELECT 
        operator_id,
        registered_at,
        registration_block
    FROM operator_registration
    WHERE operator_id = :operator_id
),

-- FIRST ACTIVITY (FIXED!)
first_activity AS (
    SELECT 
        MIN(event_time) as first_activity_at,
        MIN(event_block) as first_activity_block,
        (ARRAY_AGG(event_type ORDER BY event_time, event_block))[1] as first_activity_type
    FROM (
        SELECT registered_at as event_time, registration_block as event_block, 'REGISTRATION' as event_type
        FROM operator_registration WHERE operator_id = :operator_id
        UNION ALL
        SELECT allocated_at, allocated_at_block, 'ALLOCATION'
        FROM operator_allocations WHERE operator_id = :operator_id
        UNION ALL
        SELECT status_changed_at, status_changed_block, 'AVS_REGISTRATION'
        FROM operator_avs_registration_history WHERE operator_id = :operator_id
        UNION ALL
        SELECT event_timestamp, event_block, 'DELEGATION'
        FROM operator_delegator_history 
        WHERE operator_id = :operator_id AND delegation_type = 'DELEGATED'
        UNION ALL
        SELECT slashed_at, slashed_at_block, 'SLASHING'
        FROM operator_slashing_incidents WHERE operator_id = :operator_id
        UNION ALL
        SELECT updated_at, updated_at_block, 'METADATA_UPDATE'
        FROM operator_metadata_history WHERE operator_id = :operator_id
    ) all_events
    WHERE event_block IS NOT NULL
),

-- LAST ACTIVITY (FIXED!)
last_activity AS (
    SELECT GREATEST(
        COALESCE(MAX(allocated_at), '1970-01-01'::timestamp),
        COALESCE(MAX(status_changed_at), '1970-01-01'::timestamp),
        COALESCE(MAX(event_timestamp), '1970-01-01'::timestamp),
        COALESCE(MAX(slashed_at), '1970-01-01'::timestamp),
        COALESCE(MAX(updated_at), '1970-01-01'::timestamp),
        COALESCE(MAX(changed_at), '1970-01-01'::timestamp)
    ) as last_activity_at
    FROM (
        SELECT allocated_at as allocated_at, NULL::timestamp as status_changed_at, 
               NULL::timestamp as event_timestamp, NULL::timestamp as slashed_at,
               NULL::timestamp as updated_at, NULL::timestamp as changed_at
        FROM operator_allocations WHERE operator_id = :operator_id
        UNION ALL
        SELECT NULL, status_changed_at, NULL, NULL, NULL, NULL
        FROM operator_avs_registration_history WHERE operator_id = :operator_id
        UNION ALL
        SELECT NULL, NULL, event_timestamp, NULL, NULL, NULL
        FROM operator_delegator_history WHERE operator_id = :operator_id
        UNION ALL
        SELECT NULL, NULL, NULL, slashed_at, NULL, NULL
        FROM operator_slashing_incidents WHERE operator_id = :operator_id
        UNION ALL
        SELECT NULL, NULL, NULL, NULL, updated_at, NULL
        FROM operator_metadata_history WHERE operator_id = :operator_id
        UNION ALL
        SELECT NULL, NULL, NULL, NULL, NULL, changed_at
        FROM operator_delegation_approver_history WHERE operator_id = :operator_id
    ) all_timestamps
),

-- PI COMMISSION (NEW!)
pi_commission_info AS (
    SELECT 
        current_bips as current_pi_split_bips,
        current_activated_at as pi_split_activated_at
    FROM operator_commission_rates
    WHERE operator_id = :operator_id AND commission_type = 'PI'
),

-- FORCE UNDELEGATIONS (NEW!)
force_undelegation_info AS (
    SELECT COUNT(*) as force_undelegation_count
    FROM operator_delegator_history
    WHERE operator_id = :operator_id AND delegation_type = 'FORCE_UNDELEGATED'
),

-- COMMISSION CHANGES (NEW!)
commission_change_info AS (
    SELECT MAX(changed_at) as last_commission_change_at
    FROM operator_commission_history
    WHERE operator_id = :operator_id
),

-- AVS/OPERATOR SET COUNTS
counts AS (
    SELECT
        COUNT(DISTINCT avs_id) FILTER (WHERE current_status = 'REGISTERED') as active_avs_count,
        COUNT(DISTINCT avs_id) as registered_avs_count
    FROM operator_avs_relationships
    WHERE operator_id = :operator_id
),

operator_set_count AS (
    SELECT COUNT(DISTINCT operator_set_id) as active_operator_set_count
    FROM operator_allocations
    WHERE operator_id = :operator_id
),

-- DELEGATOR COUNTS
delegator_counts AS (
    SELECT 
        COUNT(*) as total_delegators,
        COUNT(*) FILTER (WHERE is_delegated = TRUE) as active_delegators
    FROM operator_delegators
    WHERE operator_id = :operator_id
),

-- SLASHING INFO
slashing_info AS (
    SELECT 
        COUNT(*) as total_slash_events,
        MAX(slashed_at) as last_slashed_at
    FROM operator_slashing_incidents
    WHERE operator_id = :operator_id
),

-- ALLOCATION INFO
activity_info AS (
    SELECT MAX(allocated_at) as last_allocation_at
    FROM operator_allocations
    WHERE operator_id = :operator_id
)

-- FINAL INSERT
INSERT INTO operator_state (
    operator_id, operator_address,
    current_metadata_uri, -- metadata_fetched_at removed
    registered_at, registration_block,
    first_activity_at, first_activity_block, first_activity_type,
    current_delegation_approver, is_permissioned, delegation_approver_updated_at,
    current_pi_split_bips, pi_split_activated_at,
    active_avs_count, registered_avs_count, active_operator_set_count,
    total_delegators, active_delegators,
    total_slash_events, last_slashed_at,
    force_undelegation_count,
    last_allocation_at, last_commission_change_at, last_metadata_update_at, last_activity_at,
    operational_days,
    is_active, updated_at
)
SELECT 
    oi.operator_id,
    oi.operator_address,
    -- Metadata
    mi.current_metadata_uri,
    -- metadata_fetched_at removed
    -- Registration
    ri.registered_at,
    ri.registration_block,
    -- First Activity
    fa.first_activity_at,
    fa.first_activity_block,
    fa.first_activity_type,
    -- Delegation Approver
    COALESCE(dac.current_delegation_approver, '0x0000000000000000000000000000000000000000'),
    CASE 
        WHEN COALESCE(dac.current_delegation_approver, '0x0000000000000000000000000000000000000000') 
             != '0x0000000000000000000000000000000000000000'
        THEN TRUE ELSE FALSE 
    END as is_permissioned,
    dac.delegation_approver_updated_at,
    -- PI Commission
    pic.current_pi_split_bips,
    pic.pi_split_activated_at,
    -- Counts
    c.active_avs_count,
    c.registered_avs_count,
    osc.active_operator_set_count,
    dc.total_delegators,
    dc.active_delegators,
    -- Slashing
    COALESCE(si.total_slash_events, 0),
    si.last_slashed_at,
    -- Force Undelegations
    COALESCE(fui.force_undelegation_count, 0),
    -- Activity Timestamps
    ai.last_allocation_at,
    cci.last_commission_change_at,
    mi.last_metadata_update_at,
    COALESCE(la.last_activity_at, NOW()),
    -- Operational Days
    CASE 
        WHEN fa.first_activity_at IS NOT NULL 
        THEN EXTRACT(DAY FROM NOW() - fa.first_activity_at)::INTEGER
        ELSE 0
    END as operational_days,
    -- Status
    TRUE as is_active,
    NOW() as updated_at
FROM operator_info oi
LEFT JOIN registration_info ri ON oi.operator_id = ri.operator_id
LEFT JOIN metadata_info mi ON oi.operator_id = mi.operator_id
LEFT JOIN delegation_approver_current dac ON oi.operator_id = dac.operator_id
LEFT JOIN first_activity fa ON TRUE
LEFT JOIN last_activity la ON TRUE
LEFT JOIN pi_commission_info pic ON TRUE
LEFT JOIN force_undelegation_info fui ON TRUE
LEFT JOIN commission_change_info cci ON TRUE
CROSS JOIN counts c
CROSS JOIN operator_set_count osc
CROSS JOIN delegator_counts dc
LEFT JOIN slashing_info si ON TRUE
LEFT JOIN activity_info ai ON TRUE

ON CONFLICT (operator_id) DO UPDATE SET
    current_metadata_uri = EXCLUDED.current_metadata_uri,
    -- metadata_fetched_at removed
    registered_at = EXCLUDED.registered_at,
    registration_block = EXCLUDED.registration_block,
    first_activity_at = EXCLUDED.first_activity_at,
    first_activity_block = EXCLUDED.first_activity_block,
    first_activity_type = EXCLUDED.first_activity_type,
    current_delegation_approver = EXCLUDED.current_delegation_approver,
    is_permissioned = EXCLUDED.is_permissioned,
    delegation_approver_updated_at = EXCLUDED.delegation_approver_updated_at,
    current_pi_split_bips = EXCLUDED.current_pi_split_bips,
    pi_split_activated_at = EXCLUDED.pi_split_activated_at,
    active_avs_count = EXCLUDED.active_avs_count,
    registered_avs_count = EXCLUDED.registered_avs_count,
    active_operator_set_count = EXCLUDED.active_operator_set_count,
    total_delegators = EXCLUDED.total_delegators,
    active_delegators = EXCLUDED.active_delegators,
    total_slash_events = EXCLUDED.total_slash_events,
    last_slashed_at = EXCLUDED.last_slashed_at,
    force_undelegation_count = EXCLUDED.force_undelegation_count,
    last_allocation_at = EXCLUDED.last_allocation_at,
    last_commission_change_at = EXCLUDED.last_commission_change_at,
    last_metadata_update_at = EXCLUDED.last_metadata_update_at,
    last_activity_at = EXCLUDED.last_activity_at,
    operational_days = EXCLUDED.operational_days,
    updated_at = EXCLUDED.updated_at;
