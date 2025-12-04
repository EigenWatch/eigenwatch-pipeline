# services/reconstructors/operator_strategy_snapshot.py

from .base import BaseReconstructor
from pipeline.services.validators.fieldValidator import FieldValidator
from ..query_builders.operator_strategy_snapshot_builder import (
    OperatorStrategySnapshotQueryBuilder,
)


class OperatorStrategySnapshotReconstructor(BaseReconstructor):
    """Reconstructor for operator-strategy daily snapshots"""

    def __init__(self, db, logger):
        query_builder = OperatorStrategySnapshotQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field(
            "strategy_id", "strategies", nullable=False
        )

        # Decimal fields
        field_validator.add_decimal_field("max_magnitude", nullable=False)
        field_validator.add_decimal_field("encumbered_magnitude", nullable=False)
        field_validator.add_decimal_field("utilization_rate", nullable=False)

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
