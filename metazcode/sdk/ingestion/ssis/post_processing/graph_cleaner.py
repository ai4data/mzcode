"""
Graph Cleaner for SSIS Parser Post-Processing

Handles node de-duplication, normalization, and graph cleanup operations.
Addresses issues like:
- Duplicate nodes (e.g., table vs data_asset for the same database table)
- Inconsistent node IDs
- Redundant edges
- Property normalization
"""

import logging
from typing import List, Dict, Any, Set, Tuple
from collections import defaultdict

from ....models.graph import Node, Edge
from ....models.canonical_types import NodeType, EdgeType

logger = logging.getLogger(__name__)


class GraphCleaner:
    """
    Cleans and normalizes the graph after raw extraction.

    Responsibilities:
    1. De-duplicate nodes (consolidate table/data_asset duplicates)
    2. Normalize node IDs and names
    3. Merge redundant edges
    4. Standardize properties
    """

    def __init__(self):
        self.stats = {
            "nodes_before": 0,
            "nodes_after": 0,
            "nodes_removed": 0,
            "edges_before": 0,
            "edges_after": 0,
            "edges_removed": 0,
            "edges_rewired": 0,
        }

    def clean(self, nodes: List[Node], edges: List[Edge]) -> Tuple[List[Node], List[Edge]]:
        """
        Clean and normalize the graph.

        Args:
            nodes: List of nodes from raw extraction
            edges: List of edges from raw extraction

        Returns:
            Tuple of (cleaned_nodes, cleaned_edges)
        """
        logger.info("Starting graph cleanup...")

        self.stats["nodes_before"] = len(nodes)
        self.stats["edges_before"] = len(edges)

        # Step 1: Identify and merge duplicate table/data_asset nodes
        node_mapping, cleaned_nodes = self._deduplicate_table_nodes(nodes)

        # Step 2: Rewire edges to point to consolidated nodes
        cleaned_edges = self._rewire_edges(edges, node_mapping)

        # Step 3: Remove duplicate edges
        cleaned_edges = self._deduplicate_edges(cleaned_edges)

        # Step 4: Normalize node properties
        cleaned_nodes = self._normalize_node_properties(cleaned_nodes)

        self.stats["nodes_after"] = len(cleaned_nodes)
        self.stats["edges_after"] = len(cleaned_edges)
        self.stats["nodes_removed"] = self.stats["nodes_before"] - self.stats["nodes_after"]
        self.stats["edges_removed"] = self.stats["edges_before"] - self.stats["edges_after"]

        logger.info(f"Graph cleanup complete: {self.stats['nodes_removed']} nodes removed, "
                   f"{self.stats['edges_removed']} edges removed, "
                   f"{self.stats['edges_rewired']} edges rewired")

        return cleaned_nodes, cleaned_edges

    def _deduplicate_table_nodes(self, nodes: List[Node]) -> Tuple[Dict[str, str], List[Node]]:
        """
        Deduplicate table and data_asset nodes that represent the same database table.

        The refactored parser sometimes creates both:
        - node_type=table, name="dbo].[customer_dim"
        - node_type=data_asset, name="dbo.customer_dim"

        This method consolidates them into a single table node.

        Returns:
            Tuple of (mapping dict, cleaned nodes list)
            mapping dict: old_node_id -> new_node_id
        """
        node_mapping = {}
        cleaned_nodes_dict = {}
        table_name_to_node = {}  # Normalized name -> canonical node

        # First pass: identify canonical nodes for each unique table
        for node in nodes:
            if node.node_type in [NodeType.TABLE, NodeType.DATA_ASSET]:
                # Normalize the table name for comparison
                normalized_name = self._normalize_table_name(node.name)

                if normalized_name in table_name_to_node:
                    # Duplicate found - map to canonical node
                    canonical_node_id = table_name_to_node[normalized_name]
                    node_mapping[node.node_id] = canonical_node_id

                    # Merge properties from duplicate into canonical
                    canonical_node = cleaned_nodes_dict[canonical_node_id]
                    canonical_node.properties.update({
                        k: v for k, v in node.properties.items()
                        if k not in canonical_node.properties
                    })

                    logger.debug(f"Merged duplicate table node {node.node_id} into {canonical_node_id}")
                else:
                    # First occurrence - use as canonical, prefer TABLE type
                    canonical_node = node
                    if node.node_type == NodeType.DATA_ASSET:
                        # Convert to TABLE type
                        canonical_node = Node(
                            node_id=node.node_id,
                            node_type=NodeType.TABLE,
                            name=node.name,
                            properties=node.properties
                        )

                    table_name_to_node[normalized_name] = canonical_node.node_id
                    cleaned_nodes_dict[canonical_node.node_id] = canonical_node
                    node_mapping[node.node_id] = canonical_node.node_id
            else:
                # Non-table nodes pass through unchanged
                cleaned_nodes_dict[node.node_id] = node
                node_mapping[node.node_id] = node.node_id

        return node_mapping, list(cleaned_nodes_dict.values())

    def _normalize_table_name(self, name: str) -> str:
        """
        Normalize table name for comparison.

        Handles variations like:
        - "dbo].[customer_dim" vs "dbo.customer_dim"
        - "[dbo].[customer_dim]" vs "dbo.customer_dim"
        """
        # Remove brackets
        normalized = name.replace('[', '').replace(']', '')

        # Normalize dot notation
        normalized = normalized.replace('.', '_')
        normalized = normalized.replace('_', '')

        # Case insensitive
        normalized = normalized.lower()

        return normalized

    def _rewire_edges(self, edges: List[Edge], node_mapping: Dict[str, str]) -> List[Edge]:
        """
        Rewire edges to point to canonical nodes after deduplication.

        Args:
            edges: Original edges
            node_mapping: Mapping from old node IDs to canonical node IDs

        Returns:
            List of rewired edges
        """
        rewired_edges = []
        rewired_count = 0

        for edge in edges:
            old_source = edge.source_id
            old_target = edge.target_id

            new_source = node_mapping.get(old_source, old_source)
            new_target = node_mapping.get(old_target, old_target)

            if new_source != old_source or new_target != old_target:
                rewired_count += 1
                logger.debug(f"Rewiring edge: {old_source} -> {old_target} to {new_source} -> {new_target}")

            rewired_edge = Edge(
                source_id=new_source,
                target_id=new_target,
                relation=edge.relation,
                properties=edge.properties
            )
            rewired_edges.append(rewired_edge)

        self.stats["edges_rewired"] = rewired_count
        return rewired_edges

    def _deduplicate_edges(self, edges: List[Edge]) -> List[Edge]:
        """
        Remove duplicate edges (same source, target, and relation).

        Args:
            edges: List of edges

        Returns:
            List of unique edges
        """
        seen_edges = set()
        unique_edges = []

        for edge in edges:
            # Handle both string and enum values for relation
            relation_value = edge.relation.value if hasattr(edge.relation, 'value') else str(edge.relation)
            edge_signature = (edge.source_id, edge.target_id, relation_value)

            if edge_signature not in seen_edges:
                seen_edges.add(edge_signature)
                unique_edges.append(edge)
            else:
                logger.debug(f"Removing duplicate edge: {edge.source_id} --[{relation_value}]--> {edge.target_id}")

        return unique_edges

    def _normalize_node_properties(self, nodes: List[Node]) -> List[Node]:
        """
        Normalize node properties for consistency.

        Args:
            nodes: List of nodes

        Returns:
            List of nodes with normalized properties
        """
        for node in nodes:
            # Ensure technology property exists
            if "technology" not in node.properties:
                node.properties["technology"] = "SSIS"

            # Normalize table properties
            if node.node_type == NodeType.TABLE:
                # Ensure database_type is set
                if "database_type" not in node.properties and "source_type" in node.properties:
                    node.properties["database_type"] = node.properties["source_type"]

                # Remove data_asset-specific properties if they exist
                if "data_asset_type" in node.properties:
                    del node.properties["data_asset_type"]

        return nodes

    def get_statistics(self) -> Dict[str, Any]:
        """Get cleanup statistics."""
        return self.stats.copy()
