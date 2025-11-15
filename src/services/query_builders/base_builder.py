from abc import ABC, abstractmethod
from typing import Tuple, Dict


class BaseQueryBuilder(ABC):
    """
    Abstract base class for query builders.
    """

    @abstractmethod
    def build_fetch_query(self, operator_id: str) -> Tuple[str, Dict]:
        pass

    @abstractmethod
    def build_insert_query(self) -> str:
        pass

    @abstractmethod
    def generate_id(self, row: Dict) -> str:
        pass
