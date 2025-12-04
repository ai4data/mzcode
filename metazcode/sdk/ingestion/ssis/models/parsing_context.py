"""
Parsing Context for SSIS Parser Refactoring

This module provides a centralized context object that replaces the proliferation
of parameters (8-12 parameters) being passed to every parsing method in the legacy
monolithic parser.

Purpose:
- Eliminates parameter proliferation anti-pattern
- Provides single source of truth for parsing state
- Simplifies method signatures from 12 params → 1 param
- Enables easier testing and mocking
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from ....models.graph import Node, Edge


@dataclass
class ParsingContext:
    """
    Central context object for SSIS parsing operations.

    Replaces the multiple parameters passed throughout the legacy parser:
    - nodes: List[Node]
    - edges: List[Edge]
    - connection_id_map: Dict[str, str]
    - param_var_id_map: Dict[str, Dict]
    - task_id: str
    - file_path: str
    - pipeline_id: str
    - package_name: str

    Benefits:
    - Single parameter instead of 8-12
    - Clear ownership of collections
    - Easy to extend without breaking signatures
    - Better testability
    """

    # File context
    file_path: str
    package_name: str
    pipeline_id: str

    # Graph collections (mutable state)
    nodes: List[Node] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)

    # ID mappings for cross-referencing
    connection_id_map: Dict[str, str] = field(default_factory=dict)
    param_var_id_map: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    parameter_id_map: Dict[str, str] = field(default_factory=dict)
    variable_id_map: Dict[str, str] = field(default_factory=dict)

    # Component tracking
    task_id_stack: List[str] = field(default_factory=list)  # For nested components
    conditional_split_mapping: Dict[str, Dict[str, str]] = field(default_factory=dict)

    # Caching
    schema_cache: Dict[str, Any] = field(default_factory=dict)

    # Configuration
    enable_schema_introspection: bool = True
    enable_type_mapping: bool = True
    target_platforms: Optional[List[str]] = None

    @property
    def current_task_id(self) -> Optional[str]:
        """Get the current task ID from the stack, or None if empty."""
        return self.task_id_stack[-1] if self.task_id_stack else None

    def push_task(self, task_id: str) -> None:
        """Push a new task ID onto the stack (for nested parsing)."""
        self.task_id_stack.append(task_id)

    def pop_task(self) -> Optional[str]:
        """Pop the current task ID from the stack."""
        return self.task_id_stack.pop() if self.task_id_stack else None

    def add_node(self, node: Node) -> None:
        """Add a node to the graph collection."""
        self.nodes.append(node)

    def add_edge(self, edge: Edge) -> None:
        """Add an edge to the graph collection."""
        self.edges.append(edge)

    def get_connection_id(self, guid: str) -> Optional[str]:
        """
        Resolve a connection GUID to its canonical node ID.

        Args:
            guid: Connection GUID from SSIS XML

        Returns:
            Canonical node ID or None if not found
        """
        return self.connection_id_map.get(guid)

    def register_connection(self, guid: str, node_id: str) -> None:
        """Register a connection GUID -> node ID mapping."""
        self.connection_id_map[guid] = node_id

    def get_parameter_or_variable(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Resolve a parameter or variable by name.

        Args:
            name: Parameter or variable name

        Returns:
            Parameter/variable metadata dict or None
        """
        return self.param_var_id_map.get(name)

    def register_parameter(self, name: str, node_id: str, metadata: Dict[str, Any]) -> None:
        """Register a parameter for later resolution."""
        self.parameter_id_map[name] = node_id
        self.param_var_id_map[name] = metadata

    def register_variable(self, name: str, node_id: str, metadata: Dict[str, Any]) -> None:
        """Register a variable for later resolution."""
        self.variable_id_map[name] = node_id
        self.param_var_id_map[name] = metadata

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about the parsing context."""
        return {
            "nodes": len(self.nodes),
            "edges": len(self.edges),
            "connections": len(self.connection_id_map),
            "parameters": len(self.parameter_id_map),
            "variables": len(self.variable_id_map),
            "conditional_splits": len(self.conditional_split_mapping),
        }

    def __repr__(self) -> str:
        stats = self.get_stats()
        return (
            f"ParsingContext(package={self.package_name}, "
            f"nodes={stats['nodes']}, edges={stats['edges']}, "
            f"connections={stats['connections']})"
        )
