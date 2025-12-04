"""
Graph Cleaner for Informatica Parser Post-Processing

Handles node de-duplication, normalization, and graph cleanup operations.
Addresses issues like:
- Duplicate source/target definitions
- Duplicate data assets
- Inconsistent node IDs
- Redundant edges
- Property normalization

Informatica-specific considerations:
- Source definitions can appear in multiple mappings
- Target definitions can appear in multiple mappings
- Transformations are instance-based (not reused)
- Workflows and mappings form the pipeline hierarchy
"""

import logging
from typing import List, Dict, Any, Set, Tuple
from collections import defaultdict

from ....models.graph import Node, Edge
from ....models.canonical_types import NodeType, EdgeType

logger = logging.getLogger(__name__)


class InformaticaGraphCleaner:
    """
    Cleans and normalizes the Informatica graph after raw extraction.

    Responsibilities:
    1. De-duplicate source/target definition nodes
    2. De-duplicate data asset nodes
    3. Normalize node IDs and names
    4. Merge redundant edges
    5. Standardize properties
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
        logger.info("Starting Informatica graph cleanup...")

        self.stats["nodes_before"] = len(nodes)
        self.stats["edges_before"] = len(edges)

        # Step 1: Identify and merge duplicate source/target/data_asset nodes
        node_mapping, cleaned_nodes = self._deduplicate_data_nodes(nodes)

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

        logger.info(f"Informatica graph cleanup complete: {self.stats['nodes_removed']} nodes removed, "
                   f"{self.stats['edges_removed']} edges removed, "
                   f"{self.stats['edges_rewired']} edges rewired")

        return cleaned_nodes, cleaned_edges

    def _deduplicate_data_nodes(self, nodes: List[Node]) -> Tuple[Dict[str, str], List[Node]]:
        """
        Deduplicate data-related nodes (sources, targets, data_assets, tables).

        In Informatica:
        - Source definitions can appear in multiple mappings
        - Target definitions can appear in multiple mappings
        - Data assets represent the same physical tables

        Returns:
            Tuple of (mapping dict, cleaned nodes list)
            mapping dict: old_node_id -> new_node_id
        """
        node_mapping = {}
        cleaned_nodes_dict = {}
        canonical_data_nodes = {}  # Normalized name -> canonical node

        # First pass: identify canonical nodes for each unique data entity
        for node in nodes:
            if node.node_type in [NodeType.TABLE, NodeType.DATA_ASSET]:
                # Normalize the name for comparison
                normalized_name = self._normalize_data_name(node.name, node.node_type)

                if normalized_name in canonical_data_nodes:
                    # Duplicate found - map to canonical node
                    canonical_node_id = canonical_data_nodes[normalized_name]
                    node_mapping[node.node_id] = canonical_node_id

                    # Merge properties from duplicate into canonical
                    canonical_node = cleaned_nodes_dict[canonical_node_id]
                    canonical_node.properties.update({
                        k: v for k, v in node.properties.items()
                        if k not in canonical_node.properties
                    })

                    logger.debug(f"Merged duplicate data node {node.node_id} into {canonical_node_id}")
                else:
                    # First occurrence - use as canonical
                    # Prefer DATA_ASSET type for consistency
                    canonical_node = node
                    if node.node_type == NodeType.TABLE:
                        # Convert to DATA_ASSET type
                        canonical_node = Node(
                            node_id=f"data_asset:{self._extract_asset_type(node)}:{normalized_name}",
                            node_type=NodeType.DATA_ASSET,
                            name=node.name,
                            properties=node.properties
                        )

                    canonical_data_nodes[normalized_name] = canonical_node.node_id
                    cleaned_nodes_dict[canonical_node.node_id] = canonical_node
                    node_mapping[node.node_id] = canonical_node.node_id
            else:
                # Non-data nodes pass through unchanged
                cleaned_nodes_dict[node.node_id] = node
                node_mapping[node.node_id] = node.node_id

        return node_mapping, list(cleaned_nodes_dict.values())

    def _normalize_data_name(self, name: str, node_type: NodeType) -> str:
        """
        Normalize data entity name for comparison.

        Handles variations like:
        - "EMPLOYEES" vs "employees"
        - "dbo.EMPLOYEES" vs "EMPLOYEES"
        - Removes schema prefixes for matching
        """
        # Remove common schema prefixes
        normalized = name
        for schema_prefix in ['dbo.', 'public.', 'staging.']:
            if normalized.lower().startswith(schema_prefix):
                normalized = normalized[len(schema_prefix):]

        # Case insensitive
        normalized = normalized.lower()

        # Remove special characters
        normalized = normalized.replace('[', '').replace(']', '').replace('"', '')

        return normalized

    def _extract_asset_type(self, node: Node) -> str:
        """
        Extract asset type from node properties.

        Returns 'source', 'target', or 'table' based on node properties.
        """
        props = node.properties

        # Check for source indicators
        if props.get('source_type') or props.get('database_type') and 'source' in str(props).lower():
            return 'source'

        # Check for target indicators
        if props.get('target_type') or 'target' in str(props).lower():
            return 'target'

        # Default to table
        return 'table'

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
                node.properties["technology"] = "Informatica"

            # Normalize data asset properties
            if node.node_type == NodeType.DATA_ASSET:
                # Ensure asset_type is set
                if "asset_type" not in node.properties:
                    if "source_type" in node.properties:
                        node.properties["asset_type"] = "source"
                    elif "target_type" in node.properties:
                        node.properties["asset_type"] = "target"
                    else:
                        node.properties["asset_type"] = "table"

            # Normalize operation properties
            if node.node_type == NodeType.OPERATION:
                # Ensure operation_subtype is set
                if "operation_subtype" not in node.properties and "native_type" in node.properties:
                    node.properties["operation_subtype"] = self._categorize_operation(
                        node.properties["native_type"]
                    )

        return nodes

    def _categorize_operation(self, native_type: str) -> str:
        """Categorize Informatica operations into standard subtypes."""
        native_lower = native_type.lower()

        if "source qualifier" in native_lower:
            return "SOURCE"
        elif any(kw in native_lower for kw in ["expression", "aggregator", "rank"]):
            return "TRANSFORM"
        elif any(kw in native_lower for kw in ["joiner", "lookup", "union"]):
            return "JOIN"
        elif "router" in native_lower or "filter" in native_lower:
            return "FILTER"
        elif "sorter" in native_lower:
            return "SORT"
        elif "session" in native_lower or "command" in native_lower:
            return "EXECUTE"
        else:
            return "TRANSFORM"

    def get_statistics(self) -> Dict[str, Any]:
        """Get cleanup statistics."""
        return self.stats.copy()
