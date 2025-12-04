from typing import Optional, List, Dict, Any, Union
from dagster import ConfigurableResource


class SubgraphQueryBuilder(ConfigurableResource):
    """
    Utility for dynamically generating GraphQL queries for subgraph event fetching.
    Supports nested field selections defined per query.
    """

    def _build_where_clause(self, **filters: Dict[str, Any]) -> str:
        """Convert Python filters (including nested or arrays) into GraphQL 'where' syntax."""
        if not filters:
            return ""

        def serialize(value: Any) -> str:
            if isinstance(value, str):
                return f'"{value}"'
            elif isinstance(value, dict):
                return (
                    "{"
                    + ", ".join(f"{k}: {serialize(v)}" for k, v in value.items())
                    + "}"
                )
            elif isinstance(value, list):
                return "[" + ", ".join(serialize(v) for v in value) + "]"
            else:
                return str(value)

        parts = []
        for k, v in filters.items():
            if v is None:
                continue
            parts.append(f"{k}: {serialize(v)}")

        return f"where: {{{', '.join(parts)}}}" if parts else ""

    def _build_fields_block(
        self,
        fields: List[str],
        nested_fields: Optional[Dict[str, List[str]]] = None,
    ) -> str:
        """
        Recursively build GraphQL selection sets.
        Handles nested fields like {"operator": ["id", "address"]}.
        """
        lines = []
        for field in fields:
            if nested_fields and field in nested_fields:
                subfields = nested_fields[field]
                sub_block = self._build_fields_block(subfields)
                lines.append(f"{field} {{\n{sub_block}\n}}")
            else:
                lines.append(field)
        return "\n".join(lines)

    def _build_cursor_filter(self, cursor: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build a cursor filter for (blockNumber, logIndex) based pagination.
        Generates:
          or: [
            { blockNumber_gt: cursor.blockNumber }
            { blockNumber: cursor.blockNumber, logIndex_gt: cursor.logIndex }
          ]
        """
        if not cursor or "blockNumber" not in cursor:
            return {}

        block_number = cursor["blockNumber"]
        log_index = cursor.get("logIndex")

        if log_index is not None:
            return {
                "or": [
                    {"blockNumber_gt": block_number},
                    {"blockNumber": block_number, "logIndex_gt": log_index},
                ]
            }
        else:
            # fallback: only use block number
            return {"blockNumber_gt": block_number}

    def build_query(
        self,
        event_name: str,
        fields: List[str],
        first: int = 200,
        last_id: Optional[str] = None,
        block_number_gte: Optional[int] = None,
        block_number_lt: Optional[int] = None,
        order_by: str = "id",
        order_direction: str = "asc",
        extra_filters: Optional[Dict[str, Any]] = None,
        nested_fields: Optional[Dict[str, List[str]]] = None,
        cursor: Optional[Dict[str, Any]] = None,  # 👈 NEW
    ) -> str:
        """
        Build a complete GraphQL query for subgraph event fetching.
        """
        filters: Dict[str, Any] = {}

        if last_id:
            filters["id_gt"] = last_id
        if block_number_gte is not None:
            filters["blockNumber_gte"] = block_number_gte
        if block_number_lt is not None:
            filters["blockNumber_lt"] = block_number_lt

        # add cursor logic
        if cursor and cursor.get("blockNumber") is not None:
            cursor_filter = self._build_cursor_filter(cursor)
            filters.update(cursor_filter)

        if extra_filters:
            filters.update(extra_filters)

        where_clause = self._build_where_clause(**filters)

        fields_block = self._build_fields_block(fields, nested_fields)

        query = f"""
        query {{
          {event_name}(
            first: {first},
            orderBy: {order_by},
            orderDirection: {order_direction}
            {where_clause}
          ) {{
            {fields_block}
          }}
        }}
        """
        return query.strip()

    def build_block_range_queries(
        self,
        block_ranges: List[Dict[str, int]],
        first: int = 200,
        nested_fields: Optional[Dict[str, List[str]]] = None,
        **kwargs,
    ) -> List[str]:
        """
        Build multiple paginated queries over block ranges.
        """
        queries = []
        for r in block_ranges:
            q = self.build_query(
                first=first,
                block_number_gte=r.get("gte"),
                block_number_lt=r.get("lt"),
                nested_fields=nested_fields,
                **kwargs,
            )
            queries.append(q)
        return queries
