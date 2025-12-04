# services/reconstructors/commission_rates_snapshot.py

from .base import BaseReconstructor
from pipeline.services.validators.fieldValidator import FieldValidator
from ..query_builders.commission_rates_snapshot_builder import (
    CommissionRatesSnapshotQueryBuilder,
)


class CommissionRatesSnapshotReconstructor(BaseReconstructor):
    """Reconstructor for commission rates snapshots"""

    def __init__(self, db, logger):
        query_builder = CommissionRatesSnapshotQueryBuilder()
        column_names = query_builder.get_column_names()

        # Configure field validation
        field_validator = FieldValidator()

        # Foreign key fields
        field_validator.add_foreign_key_field(
            "operator_id", "operators", nullable=False
        )
        field_validator.add_foreign_key_field("avs_id", "avs", nullable=True)
        field_validator.add_foreign_key_field(
            "operator_set_id", "operator_sets", nullable=True
        )

        # Integer field (current_bips is already integer)

        super().__init__(
            db,
            logger,
            query_builder,
            column_names=column_names,
            field_validator=field_validator,
        )
