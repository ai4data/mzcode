"""
Parsing Context for Informatica Parser Refactoring

Centralizes state management for Informatica parsing, replacing the proliferation
of 5+ context dictionaries being passed to every parsing method.

Legacy Issue:
- connections_context: Dict[str, Dict[str, Any]]
- parameters_context: Dict[str, Dict[str, Any]]
- variables_context: Dict[str, Dict[str, Any]]
- parameter_file_context: Dict[str, Dict[str, Any]]
- session_connections: Dict[str, str]
- Plus: file_path, mapping_id, workflow_id passed separately

Refactored Solution:
- Single InformaticaParsingContext object
- Clear ownership and lifecycle
- Easy to extend without breaking signatures
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from ....models.graph import Node, Edge


@dataclass
class InformaticaParsingContext:
    """
    Central context object for Informatica parsing operations.

    Replaces 5+ dictionaries and multiple parameters passed throughout
    the legacy parser.
    """

    # File context
    file_path: str
    file_type: str  # 'workflow' or 'mapping'

    # Workflow/Mapping identification
    workflow_id: Optional[str] = None
    workflow_name: Optional[str] = None
    mapping_id: Optional[str] = None
    mapping_name: Optional[str] = None

    # Graph collections (mutable state)
    nodes: List[Node] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)

    # Context dictionaries (from legacy system)
    connections_context: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    parameters_context: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    variables_context: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    parameter_file_context: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Session-specific state
    session_connections: Dict[str, str] = field(default_factory=dict)
    session_context: Dict[str, Any] = field(default_factory=dict)

    # ID mappings for cross-referencing
    transformation_id_map: Dict[str, str] = field(default_factory=dict)
    instance_id_map: Dict[str, str] = field(default_factory=dict)
    connector_map: Dict[str, List[Dict]] = field(default_factory=dict)

    # Caching
    mapping_cache: Dict[str, Any] = field(default_factory=dict)
    schema_cache: Dict[str, Any] = field(default_factory=dict)

    # Configuration
    enable_schema_introspection: bool = True
    enable_type_mapping: bool = True
    target_platforms: Optional[List[str]] = None

    def add_node(self, node: Node) -> None:
        """Add a node to the graph collection."""
        self.nodes.append(node)

    def add_edge(self, edge: Edge) -> None:
        """Add an edge to the graph collection."""
        self.edges.append(edge)

    def add_nodes(self, nodes: List[Node]) -> None:
        """Add multiple nodes to the graph."""
        self.nodes.extend(nodes)

    def add_edges(self, edges: List[Edge]) -> None:
        """Add multiple edges to the graph."""
        self.edges.extend(edges)

    def register_transformation(self, transformation_name: str, transformation_id: str) -> None:
        """Register a transformation name -> ID mapping."""
        self.transformation_id_map[transformation_name] = transformation_id

    def get_transformation_id(self, transformation_name: str) -> Optional[str]:
        """Get transformation ID by name."""
        return self.transformation_id_map.get(transformation_name)

    def register_instance(self, instance_name: str, instance_id: str) -> None:
        """Register an instance name -> ID mapping."""
        self.instance_id_map[instance_name] = instance_id

    def get_instance_id(self, instance_name: str) -> Optional[str]:
        """Get instance ID by name."""
        return self.instance_id_map.get(instance_name)

    def add_connector(self, connector_key: str, connector_data: Dict) -> None:
        """Add connector data for later processing."""
        if connector_key not in self.connector_map:
            self.connector_map[connector_key] = []
        self.connector_map[connector_key].append(connector_data)

    def get_connectors(self, connector_key: str) -> List[Dict]:
        """Get all connectors for a given key."""
        return self.connector_map.get(connector_key, [])

    def resolve_parameter(self, param_name: str) -> Any:
        """
        Resolve a parameter value from multiple sources.

        Search order:
        1. Parameter file context
        2. Parameters context
        3. Session context
        """
        # Try parameter file first
        if param_name in self.parameter_file_context:
            return self.parameter_file_context[param_name].get("value")

        # Try regular parameters
        if param_name in self.parameters_context:
            return self.parameters_context[param_name].get("value")

        # Try session context
        if param_name in self.session_context:
            return self.session_context.get(param_name)

        return None

    def resolve_variable(self, var_name: str) -> Any:
        """Resolve a variable value."""
        if var_name in self.variables_context:
            return self.variables_context[var_name].get("value")
        return None

    def get_connection_name(self, connection_ref: str) -> Optional[str]:
        """
        Resolve connection reference to connection name.

        Args:
            connection_ref: Connection reference from session

        Returns:
            Connection name or None
        """
        return self.session_connections.get(connection_ref)

    def get_connection_info(self, connection_name: str) -> Optional[Dict[str, Any]]:
        """Get connection information by name."""
        return self.connections_context.get(connection_name)

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about the parsing context."""
        return {
            "nodes": len(self.nodes),
            "edges": len(self.edges),
            "connections": len(self.connections_context),
            "parameters": len(self.parameters_context),
            "variables": len(self.variables_context),
            "transformations": len(self.transformation_id_map),
            "instances": len(self.instance_id_map),
        }

    def __repr__(self) -> str:
        stats = self.get_stats()
        name = self.workflow_name or self.mapping_name or "unknown"
        return (
            f"InformaticaParsingContext(file={name}, type={self.file_type}, "
            f"nodes={stats['nodes']}, edges={stats['edges']})"
        )
