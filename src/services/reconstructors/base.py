from typing import List, Dict
import logging


class BaseReconstructor:
    """
    Generic reconstructor for fetching from events DB and inserting/updating
    in analytics DB. Works with a query_builder providing fetch/insert queries.
    """

    def __init__(self, db, logger: logging.Logger, query_builder):
        self.db = db
        self.logger = logger
        self.query_builder = query_builder

    def rebuild_for_operator(self, operator_id: str) -> int:
        """
        Full rebuild for a single operator: fetch rows from events, insert/update analytics.
        Returns total inserted/updated rows.
        """
        rows = self.fetch_state_for_operator(operator_id)
        return self.insert_state_rows(operator_id, rows)

    def fetch_state_for_operator(self, operator_id: str) -> List[Dict]:
        """
        Fetch raw rows from the events DB.
        """
        fetch_query, params = self.query_builder.build_fetch_query(operator_id)
        rows = self.db.execute_query(fetch_query, params, db="events")
        return rows or []

    def insert_state_rows(self, operator_id: str, rows: List[Dict]) -> int:
        """
        Insert or update rows into the analytics DB.
        """
        if not rows:
            return 0

        insert_query = self.query_builder.build_insert_query()
        total = 0

        for row in rows:
            row["id"] = self.query_builder.generate_id(row)
            self.db.execute_update(insert_query, row, db="analytics")
            total += 1

        return total
