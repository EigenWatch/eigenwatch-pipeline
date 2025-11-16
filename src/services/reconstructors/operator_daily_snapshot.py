# services/reconstructors/operator_daily_snapshot.py

from typing import Dict, List, Optional
from .base import BaseReconstructor
from services.validators.fieldValidator import FieldValidator
from ..query_builders.operator_daily_snapshot_builder import (
    OperatorDailySnapshotQueryBuilder,
)


class OperatorDailySnapshotReconstructor(BaseReconstructor):
    """Reconstructor for operator daily snapshots"""

    def __init__(self, db, logger):
        query_builder = OperatorDailySnapshotQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )

        # Integer fields are already integers from the query
        # Boolean field is already boolean from the query

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
        """Override to fetch from both events and analytics DBs"""

        # Fetch analytics DB data
        fetch_query, params = self.query_builder.build_fetch_query(
            operator_id, up_to_block
        )
        rows = self.db.execute_query(fetch_query, params, db="analytics")
        analytics_data = self.tuple_to_dict_transformer(self.column_names)(rows)

        # Fetch events DB data
        events_data = self.query_builder.fetch_events_metrics(
            self.db, operator_id, up_to_block
        )

        # Merge the data
        if analytics_data:
            analytics_data[0].update(events_data)
            return analytics_data

        return []
