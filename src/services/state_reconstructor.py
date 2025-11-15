# analytics_pipeline/services/state_reconstructor.py
"""
State Reconstruction Services - SQL-first approach for rebuilding operator state
"""
import logging
from utils.sql_queries import (
    avs_relationship_reconstructor_history_query,
    avs_relationship_reconstructor_query,
    current_state_reconstructor_query,
    commission_rate_reconstruction_query,
    delegator_reconstructor_history_query,
    delegator_reconstructor_state_query,
    delegator_reconstructor_shares_query,
    rebuild_slashing_amounts_query,
    rebuild_slashing_incidents_query,
)


class AVSRelationshipReconstructor:
    """Rebuilds AVS registration history and current relationships"""

    def __init__(self, db, logger: logging.Logger):
        self.db = db
        self.logger = logger

    def rebuild_for_operator(self, operator_id: str) -> int:
        """
        Rebuild AVS registration history and relationships.
        Uses append-only pattern for history.
        """
        # First: Rebuild history table (append-only)
        self.db.execute_update(
            avs_relationship_reconstructor_history_query,
            {"operator_id": operator_id},
            db="analytics",
        )

        # Second: Build relationships from history
        rowcount = self.db.execute_update(
            avs_relationship_reconstructor_query,
            {"operator_id": operator_id},
            db="analytics",
        )

        return rowcount


class CommissionRateReconstructor:
    """Rebuilds operator_commission_rates from commission events"""

    def __init__(self, db, logger: logging.Logger):
        self.db = db
        self.logger = logger

    def rebuild_for_operator(self, operator_id: str) -> int:
        """Rebuild commission rates (PI, AVS, Operator Set)"""

        rowcount = self.db.execute_update(
            commission_rate_reconstruction_query,
            {"operator_id": operator_id},
            db="analytics",
        )

        return rowcount


class DelegatorReconstructor:
    """Rebuilds operator_delegators and operator_delegator_shares"""

    def __init__(self, db, logger: logging.Logger):
        self.db = db
        self.logger = logger

    def rebuild_for_operator(self, operator_id: str) -> int:
        """Rebuild delegator state and shares"""
        # Step 1: Rebuild delegator history (append-only)
        self._rebuild_delegator_history(operator_id)

        # Step 2: Rebuild current delegator state from history
        self._rebuild_delegator_state(operator_id)

        # Step 3: Rebuild delegator shares
        rowcount = self._rebuild_delegator_shares(operator_id)

        return rowcount

    def _rebuild_delegator_history(self, operator_id: str):
        """Insert delegation events into history table"""

        self.db.execute_update(
            delegator_reconstructor_history_query,
            {"operator_id": operator_id},
            db="analytics",
        )

    def _rebuild_delegator_state(self, operator_id: str):
        """Build current delegator state from history"""

        self.db.execute_update(
            delegator_reconstructor_state_query,
            {"operator_id": operator_id},
            db="analytics",
        )

    def _rebuild_delegator_shares(self, operator_id: str) -> int:
        """Build delegator shares per strategy"""

        rowcount = self.db.execute_update(
            delegator_reconstructor_shares_query,
            {"operator_id": operator_id},
            db="analytics",
        )

        return rowcount


class SlashingReconstructor:
    """Rebuilds operator_slashing_incidents and operator_slashing_amounts"""

    def __init__(self, db, logger: logging.Logger):
        self.db = db
        self.logger = logger

    def rebuild_for_operator(self, operator_id: str) -> int:
        """Rebuild slashing incidents and amounts"""
        # Step 1: Rebuild slashing incidents
        self._rebuild_slashing_incidents(operator_id)

        # Step 2: Rebuild slashing amounts (per strategy)
        rowcount = self._rebuild_slashing_amounts(operator_id)

        return rowcount

    def _rebuild_slashing_incidents(self, operator_id: str):
        """Insert slashing incidents from events"""

        self.db.execute_update(
            rebuild_slashing_incidents_query,
            {"operator_id": operator_id},
            db="analytics",
        )

    def _rebuild_slashing_amounts(self, operator_id: str) -> int:
        """Insert slashing amounts per strategy from incidents"""

        rowcount = self.db.execute_update(
            rebuild_slashing_amounts_query, {"operator_id": operator_id}, db="analytics"
        )

        return rowcount


class CurrentStateAggregator:
    """Aggregates all derived state into operator_current_state"""

    def __init__(self, db, logger: logging.Logger):
        self.db = db
        self.logger = logger

    def aggregate_for_operator(self, operator_id: str):
        """
        Update operator_current_state with counts and metadata
        aggregated from all other tables.
        """

        self.db.execute_update(
            current_state_reconstructor_query,
            {"operator_id": operator_id},
            db="analytics",
        )
