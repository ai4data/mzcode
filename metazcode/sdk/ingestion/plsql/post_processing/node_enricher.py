"""Node enrichment for PL/SQL parser."""

import logging
from typing import List

from ....models.graph import Node

logger = logging.getLogger(__name__)


class NodeEnricher:
    """
    Enriches nodes with additional metadata after initial parsing.

    Similar to SSIS/Informatica post-processing enrichment.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def enrich_nodes(self, nodes: List[Node]) -> List[Node]:
        """
        Enrich nodes with additional computed metadata.

        Args:
            nodes: List of nodes to enrich

        Returns:
            List of enriched nodes
        """
        enriched_nodes = []

        for node in nodes:
            # Add computed properties based on node type
            if node.node_type == "operation":
                self._enrich_operation_node(node)
            elif node.node_type == "data_asset":
                self._enrich_data_asset_node(node)
            elif node.node_type == "pipeline":
                self._enrich_pipeline_node(node)

            enriched_nodes.append(node)

        self.logger.info(f"Enriched {len(enriched_nodes)} nodes with additional metadata")
        return enriched_nodes

    def _enrich_operation_node(self, node: Node) -> None:
        """Add computed metadata to operation nodes."""
        props = node.properties

        # Compute complexity score
        complexity = 0
        if "sql_statements_count" in props:
            complexity += props["sql_statements_count"]
        if "column_lineage" in props:
            complexity += len(props["column_lineage"])
        if "cursor_lineage" in props:
            complexity += len(props["cursor_lineage"]) * 2

        props["complexity_score"] = complexity

        # Categorize complexity
        if complexity >= 20:
            props["complexity_level"] = "HIGH"
        elif complexity >= 10:
            props["complexity_level"] = "MEDIUM"
        else:
            props["complexity_level"] = "LOW"

    def _enrich_data_asset_node(self, node: Node) -> None:
        """Add computed metadata to data asset nodes."""
        props = node.properties

        # Add asset classification
        if "type_mapping" in props:
            props["has_type_mapping"] = True
            props["column_count"] = len(props["type_mapping"].get("columns", []))
        else:
            props["has_type_mapping"] = False
            props["column_count"] = 0

    def _enrich_pipeline_node(self, node: Node) -> None:
        """Add computed metadata to pipeline nodes."""
        props = node.properties

        # Add pipeline classification
        props["pipeline_classification"] = "plsql_file"
