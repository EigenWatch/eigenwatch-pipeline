# query_builders/base_builder.py

from abc import ABC, abstractmethod
from typing import Tuple, Dict, Optional


class BaseQueryBuilder(ABC):
    """
    Abstract base class for query builders with optional point-in-time support.

    This allows the same query builder to be used for:
    1. Current state (up_to_block = None, is_snapshot = False)
    2. Historical snapshots (up_to_block = specific block, is_snapshot = True)
    """

    @abstractmethod
    def build_fetch_query(
        self, operator_id: str, up_to_block: Optional[int] = None
    ) -> Tuple[str, Dict]:
        """
        Build fetch query from events DB.

        Args:
            operator_id: The operator to fetch data for
            up_to_block: If provided, only fetch events up to this block number.
                        If None, fetch all events (for current state).

        Returns:
            Tuple of (SQL query string, parameters dict)
        """
        pass

    @abstractmethod
    def build_insert_query(self, is_snapshot: bool = False) -> str:
        """
        Build insert query for analytics DB.

        Args:
            is_snapshot: If True, insert into snapshot table.
                        If False, insert into current state table.

        Returns:
            SQL insert query string with ON CONFLICT handling
        """
        pass

    @abstractmethod
    def generate_id(self, row: Dict, is_snapshot: bool = False) -> str:
        """
        Generate composite ID for the row.

        Args:
            row: Data row as dictionary
            is_snapshot: If True, generating ID for snapshot (may return None for auto-increment)

        Returns:
            Composite ID string, or None if table uses auto-increment IDs
        """
        pass

    @abstractmethod
    def get_column_names(self) -> list:
        """
        Get list of column names returned by fetch query.
        Used to map tuple results to dictionaries.

        Returns:
            List of column names in order
        """
        pass
