# analytics_pipeline/services/state_reconstructor.py
"""
State Reconstruction Services - SQL-first approach for rebuilding operator state
"""
import logging
from utils.sql_queries import (
    strategy_state_reconstructor_query,
    allocation_state_reconstructor_query,
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


class StrategyStateReconstructor:
    """Rebuilds operator_strategy_state from max_magnitude and encumbered_magnitude events"""

    def __init__(self, db, logger: logging.Logger):
        self.db = db
        self.logger = logger

    def rebuild_for_operator(self, operator_id: str) -> int:
        """
        Rebuild strategy state for one operator.
        Returns count of strategy records updated.
        """

        rowcount = self.db.execute_update(
            strategy_state_reconstructor_query,
            {"operator_id": operator_id},
            db="analytics",
        )

        return rowcount


class AllocationReconstructor:
    """Rebuilds operator_allocations from allocation_events"""

    def __init__(self, db, logger: logging.Logger):
        self.db = db
        self.logger = logger

    def rebuild_for_operator(self, operator_id: str) -> int:
        """
        Rebuild allocations for one operator.
        Determines is_active based on current block vs effect_block.
        """
        # Get current block number from latest event
        current_block_query = """
            SELECT MAX(block_number) as current_block
            FROM (
                SELECT block_number FROM allocation_events LIMIT 1
                UNION ALL
                SELECT block_number FROM operator_share_events LIMIT 1
            ) latest
        """
        result = self.db.execute_query(current_block_query, db="events")
        current_block = result[0][0] if result and result[0][0] else 0

        rowcount = self.db.execute_update(
            allocation_state_reconstructor_query,
            {"operator_id": operator_id, "current_block": current_block},
            db="analytics",
        )

        return rowcount


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
