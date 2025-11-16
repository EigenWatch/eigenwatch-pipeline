# services/reconstructors/delegator_shares_snapshot.py

from typing import Dict, List, Optional
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

    def fetch_state_for_operator(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> List[Dict]:
        """Override to fetch from events DB and enrich with delegation status"""

        # Fetch shares data
        fetch_query, params = self.query_builder.build_fetch_query(
            operator_id, up_to_block
        )
        rows = self.db.execute_query(fetch_query, params, db="events")
        shares_data = self.tuple_to_dict_transformer(self.column_names)(rows)

        # Fetch delegation status
        delegation_status = self.query_builder.fetch_delegation_status(
            self.db, operator_id, up_to_block
        )

        # Enrich shares data with is_delegated
        for row in shares_data:
            row["is_delegated"] = delegation_status.get(row["staker_id"], False)

        return shares_data
