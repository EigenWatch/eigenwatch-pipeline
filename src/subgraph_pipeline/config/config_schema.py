from typing import TypedDict, List, Optional, Dict, Callable


class EventConfig(TypedDict):
    """Configuration for a single event type."""

    graphql_name: str
    table_name: str
    fields: List[str]
    nested_fields: Optional[Dict[str, List[str]]]
    entity_dependencies: List[str]
    entity_extractors: Dict[str, Callable]
    column_mapping: Dict[str, str]
    group_name: str
    contract_source: str
