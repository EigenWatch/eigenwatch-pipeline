# services/query_builders/slashing_amounts_builder.py
from .base_builder import BaseQueryBuilder

slashing_amounts_query = """
WITH recent_incidents AS (
    SELECT 
        si.id as slashing_incident_id,
        si.operator_id,
        si.slashed_at,
        sec.strategies,
        sec.wad_slashed
    FROM operator_slashing_incidents si
    JOIN slashing_events_cache sec
        ON si.operator_id = sec.operator_id
        AND si.slashed_at_block = sec.block_number
        AND si.transaction_hash = sec.transaction_hash
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
)
SELECT
    slashing_incident_id,
    operator_id,
    strategy_id,
    wad_slashed,
    slashed_at,
    NOW() as created_at,
    NOW() as updated_at
FROM unpacked_slashing
"""


class SlashingAmountsQueryBuilder(BaseQueryBuilder):
    def build_fetch_query(self, operator_id: str):
        return slashing_amounts_query, {"operator_id": operator_id}

    def build_insert_query(self) -> str:
        return """
INSERT INTO operator_slashing_amounts (
    slashing_incident_id, operator_id, strategy_id, wad_slashed,
    created_at, updated_at
)
VALUES (
    :slashing_incident_id, :operator_id, :strategy_id, :wad_slashed,
    :created_at, :updated_at
)
ON CONFLICT DO NOTHING
"""

    def generate_id(self, row: dict) -> str:
        return f"{row['slashing_incident_id']}-{row['strategy_id']}"

    def get_column_names(self) -> list:
        return [
            "slashing_incident_id",
            "operator_id",
            "strategy_id",
            "wad_slashed",
            "created_at",
            "updated_at",
        ]
