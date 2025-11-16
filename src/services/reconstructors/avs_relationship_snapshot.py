# services/reconstructors/avs_relationship_snapshot.py

from .base import BaseReconstructor
from services.validators.fieldValidator import FieldValidator
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
