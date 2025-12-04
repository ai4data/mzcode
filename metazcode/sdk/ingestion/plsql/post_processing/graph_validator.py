"""Graph validation and cleanup for PL/SQL parser."""

import logging
from typing import List, Tuple

from ....models.graph import Node, Edge

logger = logging.getLogger(__name__)


class GraphValidator:
    """
    Validates and cleans up the parsed graph.

    Similar to SSIS/Informatica post-processing validation.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.validation_stats = {
            "nodes_validated": 0,
            "edges_validated": 0,
            "invalid_nodes_removed": 0,
            "invalid_edges_removed": 0,
            "duplicate_nodes_removed": 0
        }

    def validate_graph(
        self,
        nodes: List[Node],
        edges: List[Edge]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Validate and clean the graph.

        Args:
            nodes: List of nodes to validate
            edges: List of edges to validate

        Returns:
            Tuple of (validated_nodes, validated_edges)
        """
        # Step 1: Remove duplicate nodes
        validated_nodes = self._remove_duplicate_nodes(nodes)

        # Step 2: Validate edges point to existing nodes
        node_ids = {node.node_id for node in validated_nodes}
        validated_edges = self._validate_edges(edges, node_ids)

        self.logger.info(
            f"Graph validation complete: {len(validated_nodes)} nodes, "
            f"{len(validated_edges)} edges. Removed {self.validation_stats['invalid_edges_removed']} invalid edges."
        )

        return validated_nodes, validated_edges

    def _remove_duplicate_nodes(self, nodes: List[Node]) -> List[Node]:
        """Remove duplicate nodes, keeping the first occurrence."""
        seen_ids = set()
        unique_nodes = []

        for node in nodes:
            if node.node_id not in seen_ids:
                seen_ids.add(node.node_id)
                unique_nodes.append(node)
                self.validation_stats["nodes_validated"] += 1
            else:
                self.validation_stats["duplicate_nodes_removed"] += 1
                self.logger.debug(f"Removed duplicate node: {node.node_id}")

        return unique_nodes

    def _validate_edges(self, edges: List[Edge], valid_node_ids: set) -> List[Edge]:
        """Validate that edges point to existing nodes."""
        validated_edges = []

        for edge in edges:
            if edge.source_id in valid_node_ids and edge.target_id in valid_node_ids:
                validated_edges.append(edge)
                self.validation_stats["edges_validated"] += 1
            else:
                self.validation_stats["invalid_edges_removed"] += 1
                self.logger.debug(
                    f"Removed invalid edge: {edge.source_id} -> {edge.target_id} "
                    f"(source_exists={edge.source_id in valid_node_ids}, "
                    f"target_exists={edge.target_id in valid_node_ids})"
                )

        return validated_edges

    def get_validation_report(self) -> dict:
        """Get validation statistics report."""
        return self.validation_stats.copy()
