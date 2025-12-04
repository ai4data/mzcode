"""
Connection builder for PL/SQL parser.

Handles creation of connection nodes and USES_CONNECTION edges.
This addresses the critical gap in the original implementation.
"""

import logging
import re
from typing import List, Dict, Any, Optional

from ....models.graph import Node, Edge
from ....models.canonical_types import EdgeType
from ..models.parsing_context import PlsqlParsingContext
from .graph_builder import PlsqlGraphBuilder

logger = logging.getLogger(__name__)


class ConnectionBuilder:
    """
    Builds connection nodes and edges for PL/SQL operations.

    This follows the same pattern as SSIS and Informatica connection builders,
    ensuring operations are linked to their database connections.
    """

    # Pattern to detect database links in SQL
    DB_LINK_PATTERN = re.compile(r'@([a-zA-Z0-9_]+)', re.IGNORECASE)

    # Pattern to detect connection strings in CONNECT statements
    CONNECT_PATTERN = re.compile(r'CONNECT\s+\w+/[^@\s]+@([^\s;]+)', re.IGNORECASE)

    def __init__(self, context: PlsqlParsingContext, graph_builder: PlsqlGraphBuilder):
        """
        Initialize connection builder.

        Args:
            context: Parsing context
            graph_builder: Graph builder for creating nodes/edges
        """
        self.context = context
        self.graph_builder = graph_builder
        self.logger = logging.getLogger(__name__)

    def create_connection_nodes(self) -> List[Node]:
        """
        Create connection nodes from Oracle connection context.

        Returns:
            List of connection nodes
        """
        connection_nodes = []

        for conn_name, conn_data in self.context.connections_context.items():
            node = self.graph_builder.create_connection_node(conn_name, conn_data)
            if node:
                connection_nodes.append(node)
                self.logger.debug(f"Created connection node: {conn_name}")

        return connection_nodes

    def create_uses_connection_edge(
        self,
        operation_id: str,
        block: str,
        edges: List[Edge]
    ) -> None:
        """
        Create USES_CONNECTION edge if operation uses a database connection.

        This addresses the critical gap identified in the review where the original
        implementation created connection nodes but never created USES_CONNECTION edges.

        Args:
            operation_id: ID of the operation node
            block: SQL block text to analyze
            edges: List to append edges to
        """
        # Extract connection references from the block
        connection_names = self._extract_connection_references(block)

        for conn_name in connection_names:
            conn_node_id = self.context.get_connection_node_id(conn_name)

            if conn_node_id:
                edge = self.graph_builder.create_edge(
                    source_id=operation_id,
                    target_id=conn_node_id,
                    relation=EdgeType.USES_CONNECTION,
                    properties={
                        "connection_name": conn_name,
                        "derivation_method": "sql_analysis",
                        "confidence_level": "high"
                    }
                )
                edges.append(edge)
                self.context.parse_stats["connections_used"] += 1
                self.logger.debug(f"Created USES_CONNECTION edge: {operation_id} -> {conn_node_id}")

    def _extract_connection_references(self, block: str) -> List[str]:
        """
        Extract connection references from PL/SQL block.

        Looks for:
        1. Database links (@connection_name)
        2. CONNECT statements
        3. Known connection names from context

        Args:
            block: SQL block text

        Returns:
            List of connection names referenced
        """
        connection_names = set()

        # Method 1: Extract database links (e.g., table@dblink)
        for match in self.DB_LINK_PATTERN.finditer(block):
            db_link = match.group(1)
            # Check if this matches a known connection
            if db_link in self.context.connections_context:
                connection_names.add(db_link)
            # Also check if it's part of a TNS entry
            for conn_name in self.context.connections_context:
                if db_link.upper() in conn_name.upper():
                    connection_names.add(conn_name)

        # Method 2: Extract CONNECT statements
        for match in self.CONNECT_PATTERN.finditer(block):
            conn_string = match.group(1)
            # Check if this matches a known connection
            for conn_name, conn_data in self.context.connections_context.items():
                service_name = conn_data.get("service_name", "")
                if service_name and service_name.upper() in conn_string.upper():
                    connection_names.add(conn_name)

        # Method 3: If no explicit connections found, check if any default connection exists
        # This handles the common case where connection is implicit
        if not connection_names and self.context.connections_context:
            # Look for connection usage patterns in comments
            for conn_name in self.context.connections_context:
                # Case-insensitive search for connection name in block
                if re.search(rf'\b{re.escape(conn_name)}\b', block, re.IGNORECASE):
                    connection_names.add(conn_name)

        return list(connection_names)

    def create_parameter_edges(
        self,
        operation_id: str,
        parameter_usage: Dict[str, Any],
        edges: List[Edge]
    ) -> None:
        """
        Create USES_PARAMETER edges for operation.

        Args:
            operation_id: ID of the operation node
            parameter_usage: Dictionary with parameter usage information
            edges: List to append edges to
        """
        used_parameters = parameter_usage.get("parameters_used", [])

        for param_name in used_parameters:
            param_node_id = self.context.get_parameter_node_id(param_name)

            if param_node_id:
                edge = self.graph_builder.create_edge(
                    source_id=operation_id,
                    target_id=param_node_id,
                    relation=EdgeType.USES_PARAMETER,
                    properties={
                        "parameter_name": param_name,
                        "usage_type": "reference"
                    }
                )
                edges.append(edge)
                self.context.parse_stats["parameters_used"] += 1
                self.logger.debug(f"Created USES_PARAMETER edge: {operation_id} -> {param_node_id}")
