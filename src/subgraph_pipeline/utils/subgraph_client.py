from dagster import ConfigurableResource
import requests
from typing import Any, Dict


class SubgraphClient(ConfigurableResource):
    """Dagster resource for interacting with a The Graph subgraph endpoint."""

    endpoint: str
    api_key: str

    def query(
        self, query: str, variables: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """
        Execute a GraphQL query against the configured subgraph.

        Args:
            query (str): The GraphQL query string.
            variables (dict, optional): Variables for the query.

        Returns:
            dict: Parsed JSON response.
        """
        payload = {
            "query": query,
            "operationName": "Subgraphs",
            "variables": variables or {},
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        response = requests.post(self.endpoint, json=payload, headers=headers)

        # Raise a clear error if it fails
        if not response.ok:
            raise RuntimeError(
                f"Subgraph query failed with status {response.status_code}: {response.text}"
            )

        return response.json()
