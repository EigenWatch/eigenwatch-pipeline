# services/reconstructors/delegator_shares_snapshot.py

from .base import BaseReconstructor
from services.validators.fieldValidator import FieldValidator
from ..query_builders.delegator_shares_snapshot_builder import (
    DelegatorSharesSnapshotQueryBuilder,
)


class DelegatorSharesSnapshotReconstructor(BaseReconstructor):
    """Reconstructor for delegator shares snapshots"""

    def __init__(self, db, logger):
        query_builder = DelegatorSharesSnapshotQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field("staker_id", "stakers", nullable=False)
        field_validator.add_foreign_key_field(
            "strategy_id", "strategies", nullable=False
        )

        # Decimal field
        field_validator.add_decimal_field("shares", nullable=False)

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
