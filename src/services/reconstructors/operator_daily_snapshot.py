# services/reconstructors/operator_daily_snapshot.py

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
