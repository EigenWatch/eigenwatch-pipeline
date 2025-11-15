# services/query_builders/avs_relationship_current_builder.py
from .base_builder import BaseQueryBuilder

avs_relationship_current_query = """
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
)
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
    GREATEST(rs.last_registered_at, rs.last_unregistered_at) as last_activity_at,
    NOW() as updated_at
FROM current_status cs
LEFT JOIN registration_stats rs ON cs.avs_id = rs.avs_id
"""


class AVSRelationshipCurrentQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str):
        return avs_relationship_current_query, {"operator_id": operator_id}

    def build_insert_query(self) -> str:
        return """
INSERT INTO operator_avs_relationships (
    id, operator_id, avs_id, current_status, current_status_since,
    first_registered_at, last_registered_at, last_unregistered_at,
    total_registration_cycles, total_days_registered,
    last_activity_at, updated_at
)
VALUES (
    :id, :operator_id, :avs_id, :current_status, :current_status_since,
    :first_registered_at, :last_registered_at, :last_unregistered_at,
    :total_registration_cycles, :total_days_registered,
    :last_activity_at, :updated_at
)
ON CONFLICT (id) DO UPDATE SET
    current_status = EXCLUDED.current_status,
    current_status_since = EXCLUDED.current_status_since,
    first_registered_at = EXCLUDED.first_registered_at,
    last_registered_at = EXCLUDED.last_registered_at,
    last_unregistered_at = EXCLUDED.last_unregistered_at,
    total_registration_cycles = EXCLUDED.total_registration_cycles,
    total_days_registered = EXCLUDED.total_days_registered,
    last_activity_at = EXCLUDED.last_activity_at,
    updated_at = EXCLUDED.updated_at
"""

    def generate_id(self, row: dict) -> str:
        return f"{row['operator_id']}-{row['avs_id']}"

    def get_column_names(self) -> list:
        return [
            "operator_id",
            "avs_id",
            "current_status",
            "current_status_since",
            "first_registered_at",
            "last_registered_at",
            "last_unregistered_at",
            "total_registration_cycles",
            "total_days_registered",
            "last_activity_at",
            "updated_at",
        ]
