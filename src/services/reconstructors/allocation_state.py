from .base import BaseReconstructor
from ..query_builders.allocation_state_builder import AllocationStateQueryBuilder


class AllocationReconstructor(BaseReconstructor):
    def __init__(self, db, logger, current_block: int):
        query_builder = AllocationStateQueryBuilder(current_block=current_block)
        super().__init__(db, logger, query_builder)
