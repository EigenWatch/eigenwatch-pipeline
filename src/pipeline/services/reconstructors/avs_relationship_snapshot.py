# services/reconstructors/avs_relationship_snapshot.py

from typing import Dict, List, Optional
from .base import BaseReconstructor
from pipeline.services.validators.fieldValidator import FieldValidator
from ..query_builders.avs_relationship_snapshot_builder import (
    AVSRelationshipSnapshotQueryBuilder,
)


class AVSRelationshipSnapshotReconstructor(BaseReconstructor):
    """Reconstructor for AVS relationship snapshots"""

    def __init__(self, db, logger):
        query_builder = AVSRelationshipSnapshotQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field("avs_id", "avs", nullable=False)

        # Integer fields
        # (days_registered_to_date, current_period_days, total_registration_cycles,
        #  active_operator_set_count, avs_commission_bips are already integers)

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )

    def fetch_state_for_operator(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> List[Dict]:
        """Override to fetch from events DB and enrich with analytics DB data"""

        # Fetch relationship data from events DB
        fetch_query, params = self.query_builder.build_fetch_query(
            operator_id, up_to_block
        )
        rows = self.db.execute_query(fetch_query, params, db="events")
        relationship_data = self.tuple_to_dict_transformer(self.column_names)(rows)

        # Fetch operator set counts and commission from analytics DB
        analytics_metrics = self.query_builder.fetch_analytics_metrics(
            self.db, operator_id, up_to_block
        )

        # Enrich relationship data with analytics metrics
        for row in relationship_data:
            avs_id = row["avs_id"]
            metrics = analytics_metrics.get(avs_id, {})
            row["active_operator_set_count"] = metrics.get(
                "active_operator_set_count", 0
            )
            row["avs_commission_bips"] = metrics.get("avs_commission_bips")

        return relationship_data
