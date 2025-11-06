"""
Graph Builder for Informatica Parser Refactoring

Centralizes all graph construction logic, eliminating the ~30 duplicated
node/edge creation patterns found throughout the legacy monolithic parser.

Legacy Issues:
- Node() instantiation scattered across 24+ locations
- Edge() instantiation scattered across 30+ locations
- SourceContext.create_* called 30+ times manually
- No consistency in how nodes/edges are created
- Boilerplate repeated in 18 transformation parser methods

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
from ..models.parsing_context import InformaticaParsingContext

logger = logging.getLogger(__name__)


class InformaticaGraphBuilder:
    """
    Centralized builder for creating Informatica nodes and edges.

    Eliminates duplication by providing a single, consistent interface for
    graph construction operations specific to Informatica workflows and mappings.
    """

    def __init__(self, context: InformaticaParsingContext):
        """
        Initialize the graph builder with parsing context.

        Args:
            context: The Informatica parsing context
        """
        self.context = context
        self._node_id_cache = set()  # For duplicate detection

    def create_workflow_node(
        self,
        workflow_name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a workflow (pipeline) node.

        Args:
            workflow_name: Name of the workflow
            properties: Additional properties

        Returns:
            The created workflow Node
        """
        workflow_id = f"workflow:{workflow_name}"

        workflow_properties = properties or {}
        workflow_properties.update({
            "technology": "Informatica",
            "workflow_type": "PowerCenter",
        })

        return self.create_node(
            node_type=NodeType.PIPELINE,
            node_id=workflow_id,
            name=workflow_name,
            properties=workflow_properties,
            xml_path=f"//WORKFLOW[@NAME='{workflow_name}']",
        )

    def create_mapping_node(
        self,
        mapping_name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a mapping (pipeline) node.

        Args:
            mapping_name: Name of the mapping
            properties: Additional properties

        Returns:
            The created mapping Node
        """
        mapping_id = f"mapping:{mapping_name}"

        mapping_properties = properties or {}
        mapping_properties.update({
            "technology": "Informatica",
            "mapping_type": "PowerCenter",
        })

        return self.create_node(
            node_type=NodeType.PIPELINE,
            node_id=mapping_id,
            name=mapping_name,
            properties=mapping_properties,
            xml_path=f"//MAPPING[@NAME='{mapping_name}']",
        )

    def create_task_node(
        self,
        task_name: str,
        task_type: str,
        workflow_id: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a task operation node.

        Args:
            task_name: Name of the task
            task_type: Type of task (Session, Command, Email, etc.)
            workflow_id: Parent workflow ID
            properties: Additional properties

        Returns:
            The created task Node
        """
        task_id = f"{workflow_id}:task:{task_name}"

        task_properties = properties or {}
        task_properties.update({
            "native_type": task_type,
            "operation_subtype": self._categorize_task_type(task_type),
            "technology": "Informatica",
        })

        return self.create_node(
            node_type=NodeType.OPERATION,
            node_id=task_id,
            name=task_name,
            properties=task_properties,
            xml_path=f"//TASKINSTANCE[@NAME='{task_name}']",
        )

    def create_transformation_node(
        self,
        instance_name: str,
        transformation_type: str,
        mapping_id: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a transformation operation node.

        Args:
            instance_name: Instance name of the transformation
            transformation_type: Type (Source Qualifier, Expression, Joiner, etc.)
            mapping_id: Parent mapping ID
            properties: Additional properties

        Returns:
            The created transformation Node
        """
        instance_id = f"{mapping_id}:{transformation_type.replace(' ', '_').lower()}:{instance_name}"

        transformation_properties = properties or {}
        transformation_properties.update({
            "native_type": transformation_type,
            "operation_subtype": self._categorize_transformation_type(transformation_type),
            "technology": "Informatica",
        })

        return self.create_node(
            node_type=NodeType.OPERATION,
            node_id=instance_id,
            name=instance_name,
            properties=transformation_properties,
        )

    def create_source_node(
        self,
        source_name: str,
        database_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a source definition node.

        Args:
            source_name: Name of the source
            database_type: Database type (Oracle, SQL Server, etc.)
            properties: Additional properties

        Returns:
            The created source Node
        """
        source_id = f"source:{source_name}"

        source_properties = properties or {}
        source_properties.update({
            "database_type": database_type,
            "source_type": "Database",
        })

        return self.create_node(
            node_type=NodeType.TABLE,
            node_id=source_id,
            name=source_name,
            properties=source_properties,
        )

    def create_target_node(
        self,
        target_name: str,
        database_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Create a target definition node.

        Args:
            target_name: Name of the target
            database_type: Database type
            properties: Additional properties

        Returns:
            The created target Node
        """
        target_id = f"target:{target_name}"

        target_properties = properties or {}
        target_properties.update({
            "database_type": database_type,
            "target_type": "Database",
        })

        return self.create_node(
            node_type=NodeType.TABLE,
            node_id=target_id,
            name=target_name,
            properties=target_properties,
        )

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
        Create a generic node with automatic traceability.

        Args:
            node_type: Type of the node
            node_id: Unique identifier
            name: Human-readable name
            properties: Additional properties
            xml_path: XPath to source element
            auto_traceability: Whether to inject traceability

        Returns:
            The created Node instance
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
                source_file_type="xml",
                xml_path=xml_path or f"//*[@NAME='{name}']",
                parent_package=self.context.workflow_name or self.context.mapping_name,
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
        Create an edge with automatic traceability.

        Args:
            source_id: Source node ID
            target_id: Target node ID
            relation: Edge type
            properties: Additional properties
            xml_path: XPath to source element
            auto_traceability: Whether to inject traceability

        Returns:
            The created Edge instance
        """
        # Prepare properties
        edge_properties = properties or {}

        # Inject traceability if enabled
        if auto_traceability:
            traceability = SourceContext.create_edge_traceability(
                source_file_path=self.context.file_path,
                derivation_method="informatica_parsing",
                xml_location=xml_path or "",
                context_info={
                    "parent_package": self.context.workflow_name or self.context.mapping_name,
                },
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

    # Specialized edge builders
    def create_contains_edge(self, parent_id: str, child_id: str) -> Edge:
        """Create a CONTAINS edge."""
        return self.create_edge(
            source_id=parent_id,
            target_id=child_id,
            relation=EdgeType.CONTAINS,
        )

    def create_depends_on_edge(self, task_id: str, predecessor_id: str) -> Edge:
        """Create a DEPENDS_ON edge for task dependencies."""
        return self.create_edge(
            source_id=task_id,
            target_id=predecessor_id,
            relation=EdgeType.DEPENDS_ON,
        )

    def create_reads_from_edge(self, transformation_id: str, source_id: str) -> Edge:
        """Create a READS_FROM edge."""
        return self.create_edge(
            source_id=transformation_id,
            target_id=source_id,
            relation=EdgeType.READS_FROM,
        )

    def create_writes_to_edge(self, transformation_id: str, target_id: str) -> Edge:
        """Create a WRITES_TO edge."""
        return self.create_edge(
            source_id=transformation_id,
            target_id=target_id,
            relation=EdgeType.WRITES_TO,
        )

    def create_transforms_edge(
        self,
        source_transformation_id: str,
        target_transformation_id: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Edge:
        """Create a data flow edge between transformations."""
        return self.create_edge(
            source_id=source_transformation_id,
            target_id=target_transformation_id,
            relation=EdgeType.TRANSFORMS,
            properties=properties,
        )

    # Helper methods
    def _categorize_task_type(self, task_type: str) -> str:
        """Categorize task types into standardized subtypes."""
        task_type_lower = task_type.lower()

        if "session" in task_type_lower:
            return "DATA_FLOW"
        elif "command" in task_type_lower:
            return "EXECUTE"
        elif "email" in task_type_lower:
            return "NOTIFY"
        elif "decision" in task_type_lower or "event" in task_type_lower:
            return "CONTROL_FLOW"
        else:
            return "EXECUTE"

    def _categorize_transformation_type(self, transformation_type: str) -> str:
        """Categorize transformation types into standardized subtypes."""
        trans_type_lower = transformation_type.lower()

        if "source qualifier" in trans_type_lower:
            return "SOURCE"
        elif any(keyword in trans_type_lower for keyword in ["expression", "aggregator", "rank"]):
            return "TRANSFORM"
        elif any(keyword in trans_type_lower for keyword in ["joiner", "lookup", "union"]):
            return "JOIN"
        elif "router" in trans_type_lower or "filter" in trans_type_lower:
            return "FILTER"
        elif "sorter" in trans_type_lower:
            return "SORT"
        else:
            return "TRANSFORM"

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about graph construction."""
        return {
            "nodes_created": len(self.context.nodes),
            "edges_created": len(self.context.edges),
            "unique_node_ids": len(self._node_id_cache),
            "duplicates_detected": len(self.context.nodes) - len(self._node_id_cache),
            "context_stats": self.context.get_stats(),
        }
