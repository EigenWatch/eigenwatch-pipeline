from .base import BaseReconstructor
from ..query_builders.strategy_state_builder import StrategyStateQueryBuilder


class StrategyStateReconstructor(BaseReconstructor):
    def __init__(self, db, logger):
        query_builder = StrategyStateQueryBuilder()
        super().__init__(db, logger, query_builder)
