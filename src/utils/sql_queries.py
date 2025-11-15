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

strategy_state_reconstructor_query = """
   WITH latest_max_magnitude AS (
    SELECT DISTINCT ON (strategy_id)
        strategy_id,
        max_magnitude,
        block_timestamp AS max_magnitude_updated_at,
        block_number AS max_magnitude_updated_block
    FROM max_magnitude_updated_events
    WHERE operator_id = :operator_id
    ORDER BY strategy_id, block_number DESC, log_index DESC
),
latest_encumbered_magnitude AS (
    SELECT DISTINCT ON (strategy_id)
        strategy_id,
        encumbered_magnitude,
        block_timestamp AS encumbered_magnitude_updated_at,
        block_number AS encumbered_magnitude_updated_block
    FROM encumbered_magnitude_updated_events
    WHERE operator_id = :operator_id
    ORDER BY strategy_id, block_number DESC, log_index DESC
),
strategy_info AS (
    SELECT 
        id AS strategy_id,
        symbol AS strategy_symbol,
        decimals AS strategy_decimals
    FROM strategies
),
combined_state AS (
    SELECT
        :operator_id AS operator_id,
        COALESCE(mm.strategy_id, em.strategy_id) AS strategy_id,
        COALESCE(mm.max_magnitude, 0) AS max_magnitude,
        mm.max_magnitude_updated_at,
        mm.max_magnitude_updated_block,
        COALESCE(em.encumbered_magnitude, 0) AS encumbered_magnitude,
        em.encumbered_magnitude_updated_at,
        em.encumbered_magnitude_updated_block,
        CASE 
            WHEN COALESCE(mm.max_magnitude, 0) > 0 
            THEN (COALESCE(em.encumbered_magnitude, 0)::NUMERIC / mm.max_magnitude::NUMERIC * 100)
            ELSE 0 
        END AS utilization_rate,
        si.strategy_symbol,
        si.strategy_decimals,
        NOW() AS updated_at
    FROM latest_max_magnitude mm
    FULL OUTER JOIN latest_encumbered_magnitude em 
        ON mm.strategy_id = em.strategy_id
    LEFT JOIN strategy_info si 
        ON COALESCE(mm.strategy_id, em.strategy_id) = si.strategy_id
)
INSERT INTO operator_strategy_state (
    id, operator_id, strategy_id,
    max_magnitude, max_magnitude_updated_at, max_magnitude_updated_block,
    encumbered_magnitude, encumbered_magnitude_updated_at, encumbered_magnitude_updated_block,
    utilization_rate, updated_at
)
SELECT 
    CAST(operator_id AS TEXT) || '-' || CAST(strategy_id AS TEXT) AS id,
    operator_id, strategy_id,
    max_magnitude, max_magnitude_updated_at, max_magnitude_updated_block,
    encumbered_magnitude, encumbered_magnitude_updated_at, encumbered_magnitude_updated_block,
    utilization_rate, updated_at
FROM combined_state
ON CONFLICT (id) DO UPDATE SET
    max_magnitude = EXCLUDED.max_magnitude,
    max_magnitude_updated_at = EXCLUDED.max_magnitude_updated_at,
    max_magnitude_updated_block = EXCLUDED.max_magnitude_updated_block,
    encumbered_magnitude = EXCLUDED.encumbered_magnitude,
    encumbered_magnitude_updated_at = EXCLUDED.encumbered_magnitude_updated_at,
    encumbered_magnitude_updated_block = EXCLUDED.encumbered_magnitude_updated_block,
    utilization_rate = EXCLUDED.utilization_rate,
    updated_at = EXCLUDED.updated_at;
"""

allocation_state_reconstructor_query = """
WITH latest_allocations AS (
    SELECT DISTINCT ON (operator_set_id, strategy_id)
        operator_id,
        operator_set_id,
        strategy_id,
        magnitude,
        effect_block,
        block_timestamp AS allocated_at,
        block_number AS allocated_at_block,
        CASE WHEN effect_block <= :current_block THEN TRUE ELSE FALSE END AS is_active,
        CASE WHEN effect_block <= :current_block 
             THEN TO_TIMESTAMP(block_timestamp) 
             ELSE NULL 
        END AS activated_at
    FROM allocation_events
    WHERE operator_id = :operator_id
    ORDER BY operator_set_id, strategy_id, block_number DESC, log_index DESC
),
allocation_with_metadata AS (
    SELECT
        la.*,
        os.avs_id,
        NOW() AS updated_at
    FROM latest_allocations la
    LEFT JOIN operator_sets os ON la.operator_set_id = os.id
    LEFT JOIN avs ON os.avs_id = avs.id
    LEFT JOIN strategies s ON la.strategy_id = s.id
)
INSERT INTO operator_allocations (
    id, operator_id, operator_set_id, strategy_id,
    magnitude, effect_block, is_active,
    allocated_at, allocated_at_block, activated_at,
    avs_id, updated_at
)
SELECT 
    CAST(operator_id AS TEXT) || '-' || CAST(operator_set_id AS TEXT) || '-' || CAST(strategy_id AS TEXT) AS id,
    operator_id, operator_set_id, strategy_id,
    magnitude, effect_block, is_active,
    allocated_at, allocated_at_block, activated_at,
    avs_id, updated_at
FROM allocation_with_metadata
ON CONFLICT (id) DO UPDATE SET
    magnitude = EXCLUDED.magnitude,
    effect_block = EXCLUDED.effect_block,
"""

avs_relationship_reconstructor_history_query = """
    INSERT INTO operator_avs_registration_history (
        operator_id, avs_id, status,
        status_changed_at, status_changed_block,
        period_start, period_end, period_duration_days,
        event_id, transaction_hash, created_at, updated_at
    )
    SELECT 
        operator_id,
        avs_id,
        status::TEXT,
        TO_TIMESTAMP(block_timestamp) as status_changed_at,
        block_number as status_changed_block,
        TO_TIMESTAMP(block_timestamp) as period_start,
        NULL as period_end,
        NULL as period_duration_days,
        NULL as event_id,
        transaction_hash,
        NOW() as created_at,
        NOW() as updated_at
    FROM operator_avs_registration_status_updated_events oar
    LEFT JOIN avs ON oar.avs_id = avs.id
    WHERE operator_id = :operator_id
    ON CONFLICT DO NOTHING
"""

avs_relationship_reconstructor_query = """
    WITH current_status AS (
        SELECT DISTINCT ON (avs_id)
            avs_id,
            status,
            status_changed_at as current_status_since
        FROM operator_avs_registration_history
        WHERE operator_id = :operator_id
        ORDER BY avs_id, status_changed_at DESC
    ),
    registration_stats AS (
        SELECT
            avs_id,
            MIN(CASE WHEN status = 'REGISTERED' THEN status_changed_at END) as first_registered_at,
            MAX(CASE WHEN status = 'REGISTERED' THEN status_changed_at END) as last_registered_at,
            MAX(CASE WHEN status = 'UNREGISTERED' THEN status_changed_at END) as last_unregistered_at,
            COUNT(CASE WHEN status = 'REGISTERED' THEN 1 END) as total_registration_cycles,
            SUM(
                CASE WHEN status = 'REGISTERED' THEN
                    EXTRACT(EPOCH FROM (
                        COALESCE(
                            LEAD(status_changed_at) OVER (PARTITION BY avs_id ORDER BY status_changed_at),
                            NOW()
                        ) - status_changed_at
                    )) / 86400
                ELSE 0 END
            )::INTEGER as total_days_registered
        FROM operator_avs_registration_history
        WHERE operator_id = :operator_id
        GROUP BY avs_id
    ),
    current_period AS (
        SELECT 
            avs_id,
            MAX(status_changed_at) as current_period_start,
            EXTRACT(EPOCH FROM (NOW() - MAX(status_changed_at)))::INTEGER / 86400 as current_period_days
        FROM operator_avs_registration_history
        WHERE operator_id = :operator_id
            AND status = 'REGISTERED'
        GROUP BY avs_id
    ),
    operator_set_info AS (
        SELECT
            avs_id,
            JSON_AGG(DISTINCT operator_set_id) as active_operator_set_ids,
            COUNT(DISTINCT operator_set_id) as active_operator_set_count
        FROM operator_allocations
        WHERE operator_id = :operator_id
            AND is_active = TRUE
        GROUP BY avs_id
    ),
    combined AS (
        SELECT
            :operator_id as operator_id,
            cs.avs_id,
            cs.status as current_status,
            cs.current_status_since,
            rs.first_registered_at,
            rs.last_registered_at,
            rs.last_unregistered_at,
            rs.total_registration_cycles,
            rs.total_days_registered,
            cp.current_period_start,
            cp.current_period_days,
            COALESCE(osi.active_operator_set_ids, '[]'::JSON) as active_operator_set_ids,
            COALESCE(osi.active_operator_set_count, 0) as active_operator_set_count,
            NULL::INTEGER as avs_commission_bips,
            GREATEST(rs.last_registered_at, rs.last_unregistered_at) as last_activity_at,
            avs.name as avs_name,
            NOW() as updated_at
        FROM current_status cs
        LEFT JOIN registration_stats rs ON cs.avs_id = rs.avs_id
        LEFT JOIN current_period cp ON cs.avs_id = cp.avs_id
        LEFT JOIN operator_set_info osi ON cs.avs_id = osi.avs_id
        LEFT JOIN avs ON cs.avs_id = avs.id
    )
    INSERT INTO operator_avs_relationships (
        id, operator_id, avs_id, current_status, current_status_since,
        first_registered_at, last_registered_at, last_unregistered_at,
        total_registration_cycles, total_days_registered,
        current_period_start, current_period_days,
        active_operator_set_ids, active_operator_set_count,
        avs_commission_bips, last_activity_at, avs_name, updated_at
    )
    SELECT 
        operator_id || '-' || avs_id as id,
        operator_id, avs_id, current_status, current_status_since,
        first_registered_at, last_registered_at, last_unregistered_at,
        total_registration_cycles, total_days_registered,
        current_period_start, current_period_days,
        active_operator_set_ids, active_operator_set_count,
        avs_commission_bips, last_activity_at, avs_name, updated_at
    FROM combined
    ON CONFLICT (id) DO UPDATE SET
        current_status = EXCLUDED.current_status,
        current_status_since = EXCLUDED.current_status_since,
        first_registered_at = EXCLUDED.first_registered_at,
        last_registered_at = EXCLUDED.last_registered_at,
        last_unregistered_at = EXCLUDED.last_unregistered_at,
        total_registration_cycles = EXCLUDED.total_registration_cycles,
        total_days_registered = EXCLUDED.total_days_registered,
        current_period_start = EXCLUDED.current_period_start,
        current_period_days = EXCLUDED.current_period_days,
        active_operator_set_ids = EXCLUDED.active_operator_set_ids,
        active_operator_set_count = EXCLUDED.active_operator_set_count,
        last_activity_at = EXCLUDED.last_activity_at,
        avs_name = EXCLUDED.avs_name,
        updated_at = EXCLUDED.updated_at
"""

current_state_reconstructor_query = """
    WITH operator_info AS (
        SELECT 
            id as operator_id,
            address as operator_address
        FROM operators
        WHERE id = :operator_id
    ),
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
        WHERE operator_id = :operator_id AND is_active = TRUE
    ),
    delegator_counts AS (
        SELECT 
            COUNT(*) as total_delegators,
            COUNT(*) FILTER (WHERE is_delegated = TRUE) as active_delegators
        FROM operator_delegators
        WHERE operator_id = :operator_id
    ),
    slashing_info AS (
        SELECT 
            COUNT(*) as total_slash_events,
            MAX(slashed_at) as last_slashed_at
        FROM operator_slashing_incidents
        WHERE operator_id = :operator_id
    ),
    activity_info AS (
        SELECT MAX(allocated_at) as last_allocation_at
        FROM operator_allocations
        WHERE operator_id = :operator_id
    )
    INSERT INTO operator_current_state (
        operator_id, operator_address,
        active_avs_count, registered_avs_count, active_operator_set_count,
        total_delegators, active_delegators,
        total_slash_events, last_slashed_at,
        last_allocation_at, last_activity_at,
        is_active, updated_at
    )
    SELECT 
        oi.operator_id,
        oi.operator_address,
        c.active_avs_count,
        c.registered_avs_count,
        osc.active_operator_set_count,
        dc.total_delegators,
        dc.active_delegators,
        si.total_slash_events,
        si.last_slashed_at,
        ai.last_allocation_at,
        NOW() as last_activity_at,
        TRUE as is_active,
        NOW() as updated_at
    FROM operator_info oi
    CROSS JOIN counts c
    CROSS JOIN operator_set_count osc
    CROSS JOIN delegator_counts dc
    CROSS JOIN slashing_info si
    CROSS JOIN activity_info ai
    ON CONFLICT (operator_id) DO UPDATE SET
        active_avs_count = EXCLUDED.active_avs_count,
        registered_avs_count = EXCLUDED.registered_avs_count,
        active_operator_set_count = EXCLUDED.active_operator_set_count,
        total_delegators = EXCLUDED.total_delegators,
        active_delegators = EXCLUDED.active_delegators,
        total_slash_events = EXCLUDED.total_slash_events,
        last_slashed_at = EXCLUDED.last_slashed_at,
        last_allocation_at = EXCLUDED.last_allocation_at,
        last_activity_at = EXCLUDED.last_activity_at,
        updated_at = EXCLUDED.updated_at
"""

commission_rate_reconstruction_query = """
    WITH pi_commissions AS (
        SELECT DISTINCT ON (operator_id)
            operator_id,
            'PI' as commission_type,
            NULL as avs_id,
            NULL as operator_set_id,
            new_operator_pi_split_bips as current_bips,
            TO_TIMESTAMP(activated_at) as current_activated_at,
            block_number as current_set_at_block,
            old_operator_pi_split_bips as previous_bips,
            TO_TIMESTAMP(block_timestamp) as first_set_at
        FROM operator_pi_split_bips_set_events
        WHERE operator_id = :operator_id
        ORDER BY operator_id, block_number DESC, log_index DESC
    ),
    avs_commissions AS (
        SELECT DISTINCT ON (operator_id, avs_id)
            operator_id,
            'AVS' as commission_type,
            avs_id,
            NULL as operator_set_id,
            new_operator_avs_split_bips as current_bips,
            TO_TIMESTAMP(activated_at) as current_activated_at,
            block_number as current_set_at_block,
            old_operator_avs_split_bips as previous_bips,
            TO_TIMESTAMP(block_timestamp) as first_set_at
        FROM operator_avs_split_bips_set_events
        WHERE operator_id = :operator_id
        ORDER BY operator_id, avs_id, block_number DESC, log_index DESC
    ),
    operator_set_commissions AS (
        SELECT DISTINCT ON (operator_id, operator_set_id)
            operator_id,
            'OPERATOR_SET' as commission_type,
            NULL as avs_id,
            operator_set_id,
            new_operator_set_split_bips as current_bips,
            TO_TIMESTAMP(activated_at) as current_activated_at,
            block_number as current_set_at_block,
            old_operator_set_split_bips as previous_bips,
            TO_TIMESTAMP(block_timestamp) as first_set_at
        FROM operator_set_split_bips_set_events
        WHERE operator_id = :operator_id
        ORDER BY operator_id, operator_set_id, block_number DESC, log_index DESC
    ),
    all_commissions AS (
        SELECT * FROM pi_commissions
        UNION ALL
        SELECT * FROM avs_commissions
        UNION ALL
        SELECT * FROM operator_set_commissions
    ),
    commission_counts AS (
        SELECT 
            operator_id,
            commission_type,
            avs_id,
            operator_set_id,
            COUNT(*) as total_changes
        FROM (
            SELECT operator_id, 'PI' as commission_type, NULL as avs_id, NULL as operator_set_id
            FROM operator_pi_split_bips_set_events
            WHERE operator_id = :operator_id
            
            UNION ALL
            
            SELECT operator_id, 'AVS', avs_id, NULL
            FROM operator_avs_split_bips_set_events
            WHERE operator_id = :operator_id
            
            UNION ALL
            
            SELECT operator_id, 'OPERATOR_SET', NULL, operator_set_id
            FROM operator_set_split_bips_set_events
            WHERE operator_id = :operator_id
        ) sub
        GROUP BY operator_id, commission_type, avs_id, operator_set_id
    ),
    commission_with_metadata AS (
        SELECT
            ac.operator_id,
            ac.commission_type,
            ac.avs_id,
            ac.operator_set_id,
            ac.current_bips,
            ac.current_activated_at,
            ac.current_set_at_block,
            NULL::INTEGER as upcoming_bips,
            NULL::TIMESTAMP as upcoming_activated_at,
            NULL::INTEGER as upcoming_set_at_block,
            ac.previous_bips,
            cc.total_changes,
            ac.first_set_at,
            NOW() as updated_at
        FROM all_commissions ac
        LEFT JOIN commission_counts cc 
            ON ac.operator_id = cc.operator_id 
            AND ac.commission_type = cc.commission_type
            AND (ac.avs_id = cc.avs_id OR (ac.avs_id IS NULL AND cc.avs_id IS NULL))
            AND (ac.operator_set_id = cc.operator_set_id OR (ac.operator_set_id IS NULL AND cc.operator_set_id IS NULL))
        LEFT JOIN avs ON ac.avs_id = avs.id
    )
    INSERT INTO operator_commission_rates (
        id, operator_id, commission_type, avs_id, operator_set_id,
        current_bips, current_activated_at, current_set_at_block,
        upcoming_bips, upcoming_activated_at, upcoming_set_at_block,
        previous_bips, total_changes, first_set_at, updated_at
    )
    SELECT 
        operator_id || '-' || commission_type || '-' || 
            COALESCE(avs_id, '') || '-' || COALESCE(operator_set_id, '') as id,
        operator_id, commission_type, avs_id, operator_set_id,
        current_bips, current_activated_at, current_set_at_block,
        upcoming_bips, upcoming_activated_at, upcoming_set_at_block,
        previous_bips, total_changes, first_set_at, updated_at
    FROM commission_with_metadata
    ON CONFLICT (id) DO UPDATE SET
        current_bips = EXCLUDED.current_bips,
        current_activated_at = EXCLUDED.current_activated_at,
        current_set_at_block = EXCLUDED.current_set_at_block,
        upcoming_bips = EXCLUDED.upcoming_bips,
        upcoming_activated_at = EXCLUDED.upcoming_activated_at,
        upcoming_set_at_block = EXCLUDED.upcoming_set_at_block,
        previous_bips = EXCLUDED.previous_bips,
        total_changes = EXCLUDED.total_changes,
        first_set_at = EXCLUDED.first_set_at,
        updated_at = EXCLUDED.updated_at
"""

delegator_reconstructor_history_query = """
    INSERT INTO operator_delegator_history (
        operator_id, staker_id, delegation_type,
        event_timestamp, event_block, transaction_hash, created_at, updated_at
    )
    SELECT 
        operator_id,
        staker_id,
        delegation_type::TEXT,
        TO_TIMESTAMP(block_timestamp) as event_timestamp,
        block_number as event_block,
        transaction_hash,
        NOW() as created_at,
        NOW() as updated_at
    FROM staker_delegation_events
    WHERE operator_id = :operator_id
    
    UNION ALL
    
    SELECT 
        operator_id,
        staker_id,
        'FORCE_UNDELEGATED' as delegation_type,
        TO_TIMESTAMP(block_timestamp),
        block_number,
        transaction_hash,
        NOW(),
        NOW()
    FROM staker_force_undelegated_events
    WHERE operator_id = :operator_id
    
    ON CONFLICT DO NOTHING
"""

delegator_reconstructor_state_query = """
    WITH latest_delegation AS (
        SELECT DISTINCT ON (staker_id)
            staker_id,
            delegation_type,
            event_timestamp
        FROM operator_delegator_history
        WHERE operator_id = :operator_id
        ORDER BY staker_id, event_timestamp DESC
    ),
    first_delegation AS (
        SELECT 
            staker_id,
            MIN(event_timestamp) as delegated_at
        FROM operator_delegator_history
        WHERE operator_id = :operator_id
            AND delegation_type = 'DELEGATED'
        GROUP BY staker_id
    )
    INSERT INTO operator_delegators (
        id, operator_id, staker_id,
        is_delegated, delegated_at, undelegated_at, updated_at
    )
    SELECT 
        :operator_id || '-' || ld.staker_id as id,
        :operator_id as operator_id,
        ld.staker_id,
        CASE WHEN ld.delegation_type = 'DELEGATED' THEN TRUE ELSE FALSE END,
        fd.delegated_at,
        CASE WHEN ld.delegation_type != 'DELEGATED' THEN ld.event_timestamp END,
        NOW() as updated_at
    FROM latest_delegation ld
    LEFT JOIN first_delegation fd ON ld.staker_id = fd.staker_id
    ON CONFLICT (id) DO UPDATE SET
        is_delegated = EXCLUDED.is_delegated,
        delegated_at = EXCLUDED.delegated_at,
        undelegated_at = EXCLUDED.undelegated_at,
        updated_at = EXCLUDED.updated_at
"""

delegator_reconstructor_shares_query = """
    WITH share_events AS (
        SELECT 
            operator_id,
            staker_id,
            strategy_id,
            shares,
            event_type,
            block_number,
            log_index,
            block_timestamp
        FROM operator_share_events
        WHERE operator_id = :operator_id
        ORDER BY strategy_id, staker_id, block_number, log_index
    ),
    cumulative_shares AS (
        SELECT 
            operator_id,
            staker_id,
            strategy_id,
            SUM(
                CASE 
                    WHEN event_type = 'INCREASED' THEN shares
                    WHEN event_type = 'DECREASED' THEN -shares
                    ELSE 0
                END
            ) as total_shares,
            MAX(block_timestamp) as shares_updated_at,
            MAX(block_number) as shares_updated_block
        FROM share_events
        GROUP BY operator_id, staker_id, strategy_id
        HAVING SUM(
            CASE 
                WHEN event_type = 'INCREASED' THEN shares
                WHEN event_type = 'DECREASED' THEN -shares
                ELSE 0
            END
        ) > 0
    ),
    shares_with_metadata AS (
        SELECT
            cs.*,
            s.symbol as strategy_symbol,
            NOW() as updated_at
        FROM cumulative_shares cs
        LEFT JOIN strategies s ON cs.strategy_id = s.id
    )
    INSERT INTO operator_delegator_shares (
        id, operator_id, staker_id, strategy_id,
        shares, shares_updated_at, shares_updated_block,
        updated_at
    )
    SELECT 
        operator_id || '-' || staker_id || '-' || strategy_id as id,
        operator_id, staker_id, strategy_id,
        total_shares, 
        TO_TIMESTAMP(shares_updated_at),
        shares_updated_block,
        updated_at
    FROM shares_with_metadata
    ON CONFLICT (id) DO UPDATE SET
        shares = EXCLUDED.shares,
        shares_updated_at = EXCLUDED.shares_updated_at,
        shares_updated_block = EXCLUDED.shares_updated_block,
        updated_at = EXCLUDED.updated_at
"""

rebuild_slashing_amounts_query = """
    WITH recent_incidents AS (
        SELECT 
            si.id as slashing_incident_id,
            si.operator_id,
            si.slashed_at,
            ose.strategies,
            ose.wad_slashed
        FROM operator_slashing_incidents si
        JOIN operator_slashed_events ose 
            ON si.operator_id = ose.operator_id
            AND si.slashed_at_block = ose.block_number
            AND si.transaction_hash = ose.transaction_hash
        WHERE si.operator_id = :operator_id
    ),
    unpacked_slashing AS (
        SELECT 
            slashing_incident_id,
            operator_id,
            slashed_at,
            UNNEST(strategies) as strategy_id,
            UNNEST(wad_slashed) as wad_slashed
        FROM recent_incidents
    ),
    amounts_with_metadata AS (
        SELECT
            us.slashing_incident_id,
            us.operator_id,
            us.strategy_id,
            us.wad_slashed,
            us.slashed_at,
            NOW() as created_at,
            NOW() as updated_at
        FROM unpacked_slashing us
        LEFT JOIN strategies s ON us.strategy_id = s.id
    )
    INSERT INTO operator_slashing_amounts (
        slashing_incident_id, operator_id, strategy_id,
        wad_slashed, slashed_at,
        created_at, updated_at
    )
    SELECT 
        slashing_incident_id, operator_id, strategy_id,
        wad_slashed, slashed_at,
        created_at, updated_at
    FROM amounts_with_metadata
    ON CONFLICT DO NOTHING
"""

rebuild_slashing_incidents_query = """
    WITH slashing_events AS (
        SELECT 
            operator_id,
            operator_set_id,
            TO_TIMESTAMP(block_timestamp) as slashed_at,
            block_number as slashed_at_block,
            description,
            transaction_hash,
            strategies,
            wad_slashed
        FROM operator_slashed_events
        WHERE operator_id = :operator_id
    ),
    incidents_with_metadata AS (
        SELECT
            se.operator_id,
            se.operator_set_id,
            se.slashed_at,
            se.slashed_at_block,
            se.description,
            se.transaction_hash,
            os.avs_id,
            se.strategies,
            se.wad_slashed,
            NOW() as created_at,
            NOW() as updated_at
        FROM slashing_events se
        LEFT JOIN operator_sets os ON se.operator_set_id = os.id
        LEFT JOIN avs ON os.avs_id = avs.id
    )
    INSERT INTO operator_slashing_incidents (
        operator_id, operator_set_id, slashed_at, slashed_at_block,
        description, avs_id, transaction_hash,
        created_at, updated_at
    )
    SELECT 
        operator_id, operator_set_id, slashed_at, slashed_at_block,
        description, avs_id, transaction_hash,
        created_at, updated_at
    FROM incidents_with_metadata
    ON CONFLICT DO NOTHING
    RETURNING id
"""
