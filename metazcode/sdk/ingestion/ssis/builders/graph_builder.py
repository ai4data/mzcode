"""
Graph Builder for SSIS Parser Refactoring

This module centralizes all graph construction logic, eliminating the 46 duplicated
node/edge creation patterns found throughout the legacy monolithic parser.

Legacy Issue:
- nodes.append(Node(...)) scattered across 18 methods
- edges.append(Edge(...)) scattered across 28 methods
- SourceContext.create_* called 23 times in different places
- No consistency in how nodes/edges are created

Refactored Solution:
- Single source of truth for graph construction
- Automatic traceability injection
- Consistent ID generation
- Validation and deduplication
"""

from typing import Dict, Any, Optional
import logging

from ....models.canonical_types import NodeType, EdgeType
from ....models.graph import Node, Edge
from ....models.traceability import SourceContext
from ..models.parsing_context import ParsingContext

logger = logging.getLogger(__name__)


class GraphBuilder:
    """
    Centralized builder for creating nodes and edges with automatic traceability.

    Eliminates duplication by providing a single, consistent interface for
    graph construction operations.
    """

    def __init__(self, context: ParsingContext):
        """
        Initialize the graph builder with parsing context.

        Args:
            context: The parsing context containing nodes, edges, and metadata
        """
        self.context = context
        self._node_id_cache = set()  # For duplicate detection

    def create_node(
        self,
        node_type: NodeType,
        node_id: str,
        name: str,
        properties: Optional[Dict[str, Any]] = None,
        xml_path: Optional[str] = None,
        auto_traceability: bool = True,
    ) -> Node:
        """
        Create a node with automatic traceability and add to context.

        Args:
            node_type: Type of the node (PIPELINE, OPERATION, TABLE, etc.)
            node_id: Unique identifier for the node
            name: Human-readable name
            properties: Additional properties (optional)
            xml_path: XPath to source element (optional)
            auto_traceability: Whether to automatically inject traceability (default: True)

        Returns:
            The created Node instance

        Note:
            - Automatically adds node to context.nodes
            - Injects traceability metadata if auto_traceability=True
            - Logs warning if duplicate node ID detected
        """
        # Duplicate detection
        if node_id in self._node_id_cache:
            logger.warning(f"Duplicate node ID detected: {node_id}")
        self._node_id_cache.add(node_id)

        # Prepare properties
        node_properties = properties or {}

        # Inject traceability if enabled
        if auto_traceability:
            traceability = SourceContext.create_node_traceability(
                source_file_path=self.context.file_path,
                source_file_type="dtsx",
                xml_path=xml_path or f"//DTS:Executable[@DTS:ObjectName='{name}']",
                parent_package=self.context.package_name,
            )
            node_properties.update(traceability)

        # Create node
        node = Node(
            node_id=node_id,
            node_type=node_type,
            name=name,
            properties=node_properties,
        )

        # Add to context
        self.context.add_node(node)

        logger.debug(f"Created node: {node_type.value} | {node_id} | {name}")
        return node

    def create_edge(
        self,
        source_id: str,
        target_id: str,
        relation: EdgeType,
        properties: Optional[Dict[str, Any]] = None,
        xml_path: Optional[str] = None,
        auto_traceability: bool = True,
    ) -> Edge:
        """
        Create an edge with automatic traceability and add to context.

        Args:
            source_id: Source node ID
            target_id: Target node ID
            relation: Edge type (CONTAINS, USES_CONNECTION, READS_FROM, etc.)
            properties: Additional properties (optional)
            xml_path: XPath to source element (optional)
            auto_traceability: Whether to automatically inject traceability (default: True)

        Returns:
            The created Edge instance

        Note:
            - Automatically adds edge to context.edges
            - Injects traceability metadata if auto_traceability=True
        """
        # Prepare properties
        edge_properties = properties or {}

        # Inject traceability if enabled
        if auto_traceability:
            traceability = SourceContext.create_edge_traceability(
                source_file_path=self.context.file_path,
                source_file_type="dtsx",
                xml_path=xml_path or "",
                parent_package=self.context.package_name,
            )
            edge_properties.update(traceability)

        # Create edge
        edge = Edge(
            source_id=source_id,
            target_id=target_id,
            relation=relation,
            properties=edge_properties,
        )

        # Add to context
        self.context.add_edge(edge)

        logger.debug(f"Created edge: {source_id} --[{relation.value}]--> {target_id}")
        return edge

    def create_pipeline_node(
        self,
        package_name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a pipeline node for an SSIS package.

        Args:
            package_name: Name of the SSIS package
            properties: Additional properties (optional)

        Returns:
            The created pipeline Node instance
        """
        pipeline_id = f"pipeline:{package_name}"

        pipeline_properties = properties or {}
        pipeline_properties["technology"] = "SSIS"

        return self.create_node(
            node_type=NodeType.PIPELINE,
            node_id=pipeline_id,
            name=package_name,
            properties=pipeline_properties,
            xml_path="//DTS:Executable[@DTS:ExecutableType='Package']",
        )

    def create_operation_node(
        self,
        task_name: str,
        native_type: str,
        operation_subtype: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create an operation node for an SSIS task/component.

        Args:
            task_name: Name of the task
            native_type: SSIS native type (e.g., 'Microsoft.Pipeline')
            operation_subtype: Categorized subtype (CONTROL_FLOW, DATA_FLOW, etc.)
            properties: Additional properties (optional)

        Returns:
            The created operation Node instance
        """
        task_id = f"{self.context.pipeline_id}:operation:{task_name}"

        operation_properties = properties or {}
        operation_properties.update({
            "native_type": native_type,
            "operation_subtype": operation_subtype,
            "technology": "SSIS",
        })

        return self.create_node(
            node_type=NodeType.OPERATION,
            node_id=task_id,
            name=task_name,
            properties=operation_properties,
            xml_path=f"//DTS:Executable[@DTS:ObjectName='{task_name}']",
        )

    def create_table_node(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        database: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a table node for a database table.

        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            database: Database name (optional)
            properties: Additional properties (optional)

        Returns:
            The created table Node instance
        """
        # Build qualified table name
        qualified_name = table_name
        if schema_name:
            qualified_name = f"{schema_name}.{table_name}"

        table_id = f"table:{qualified_name}"

        table_properties = properties or {}
        if database:
            table_properties["database"] = database
        if schema_name:
            table_properties["schema"] = schema_name

        return self.create_node(
            node_type=NodeType.TABLE,
            node_id=table_id,
            name=qualified_name,
            properties=table_properties,
        )

    def create_column_node(
        self,
        table_id: str,
        column_name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a column node for a table column.

        Args:
            table_id: Parent table node ID
            column_name: Name of the column
            properties: Additional properties (data type, length, etc.)

        Returns:
            The created column Node instance
        """
        column_id = f"{table_id}:column:{column_name}"

        return self.create_node(
            node_type=NodeType.COLUMN,
            node_id=column_id,
            name=column_name,
            properties=properties or {},
        )

    def create_connection_node(
        self,
        connection_name: str,
        connection_guid: str,
        connection_string: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a connection node for an SSIS connection manager.

        Args:
            connection_name: Name of the connection
            connection_guid: Unique GUID from SSIS
            connection_string: Connection string
            properties: Additional properties (optional)

        Returns:
            The created connection Node instance
        """
        connection_id = f"connection:{connection_name}"

        connection_properties = properties or {}
        connection_properties.update({
            "connection_string": connection_string,
            "guid": connection_guid,
        })

        # Register in context for later resolution
        self.context.register_connection(connection_guid, connection_id)

        return self.create_node(
            node_type=NodeType.CONNECTION,
            node_id=connection_id,
            name=connection_name,
            properties=connection_properties,
            xml_path=f"//DTS:ConnectionManager[@DTS:ObjectName='{connection_name}']",
        )

    def create_parameter_node(
        self,
        parameter_name: str,
        data_type: str,
        value: Any,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a parameter node for an SSIS parameter.

        Args:
            parameter_name: Name of the parameter
            data_type: Data type of the parameter
            value: Default value
            properties: Additional properties (optional)

        Returns:
            The created parameter Node instance
        """
        parameter_id = f"{self.context.pipeline_id}:parameter:{parameter_name}"

        parameter_properties = properties or {}
        parameter_properties.update({
            "data_type": data_type,
            "value": value,
        })

        # Register in context for later resolution
        metadata = {"data_type": data_type, "value": value}
        self.context.register_parameter(parameter_name, parameter_id, metadata)

        return self.create_node(
            node_type=NodeType.PARAMETER,
            node_id=parameter_id,
            name=parameter_name,
            properties=parameter_properties,
        )

    def create_contains_edge(self, parent_id: str, child_id: str) -> Edge:
        """Create a CONTAINS edge (parent contains child)."""
        return self.create_edge(
            source_id=parent_id,
            target_id=child_id,
            relation=EdgeType.CONTAINS,
        )

    def create_uses_connection_edge(self, task_id: str, connection_id: str) -> Edge:
        """Create a USES_CONNECTION edge (task uses connection)."""
        return self.create_edge(
            source_id=task_id,
            target_id=connection_id,
            relation=EdgeType.USES_CONNECTION,
        )

    def create_reads_from_edge(self, operation_id: str, table_id: str) -> Edge:
        """Create a READS_FROM edge (operation reads from table)."""
        return self.create_edge(
            source_id=operation_id,
            target_id=table_id,
            relation=EdgeType.READS_FROM,
        )

    def create_writes_to_edge(self, operation_id: str, table_id: str) -> Edge:
        """Create a WRITES_TO edge (operation writes to table)."""
        return self.create_edge(
            source_id=operation_id,
            target_id=table_id,
            relation=EdgeType.WRITES_TO,
        )

    def create_column_lineage_edge(
        self,
        source_column_id: str,
        target_column_id: str,
        transformation: Optional[str] = None,
    ) -> Edge:
        """
        Create a DERIVES_FROM edge for column lineage.

        Args:
            source_column_id: Source column node ID
            target_column_id: Target column node ID
            transformation: Optional transformation expression

        Returns:
            The created edge
        """
        properties = {}
        if transformation:
            properties["transformation"] = transformation

        return self.create_edge(
            source_id=target_column_id,
            target_id=source_column_id,
            relation=EdgeType.DERIVES_FROM,
            properties=properties,
        )

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about graph construction."""
        return {
            "nodes_created": len(self.context.nodes),
            "edges_created": len(self.context.edges),
            "unique_node_ids": len(self._node_id_cache),
            "duplicates_detected": len(self.context.nodes) - len(self._node_id_cache),
            "context_stats": self.context.get_stats(),
        }
