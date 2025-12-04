"""
Parsing context for PL/SQL files.

Maintains state during parsing including connections, parameters, and discovered objects.
Similar to SSIS and Informatica parsing context models.
"""

from typing import Dict, List, Set, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PlsqlParsingContext:
    """
    Context object that maintains state during PL/SQL parsing.

    This follows the same pattern as SSIS/Informatica parsing contexts,
    providing centralized state management and preventing duplication.
    """

    # File being parsed
    file_path: Path
    file_content: str

    # Oracle connection context from tnsnames.ora, etc.
    connections_context: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Oracle parameters from config files
    parameters_context: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Connection name to node ID mapping for edge creation
    connection_id_map: Dict[str, str] = field(default_factory=dict)

    # Parameter name to node ID mapping for edge creation
    parameter_id_map: Dict[str, str] = field(default_factory=dict)

    # Track created nodes to avoid duplicates at file level
    created_node_ids: Set[str] = field(default_factory=set)

    # Track tables created with CREATE TABLE statements
    created_tables: Set[str] = field(default_factory=set)

    # Track referenced tables for deduplication
    referenced_tables: Set[str] = field(default_factory=set)

    # Track discovered procedures/functions
    discovered_operations: List[Dict[str, Any]] = field(default_factory=list)

    # Current operation being parsed (for nested context)
    current_operation_id: Optional[str] = None
    current_operation_name: Optional[str] = None

    # Type mapping configuration
    enable_type_mapping: bool = True
    target_platforms: List[str] = field(default_factory=lambda: ["sql_server", "postgresql"])

    # Statistics
    parse_stats: Dict[str, int] = field(default_factory=lambda: {
        "procedures_found": 0,
        "functions_found": 0,
        "anonymous_blocks": 0,
        "tables_created": 0,
        "tables_referenced": 0,
        "connections_used": 0,
        "parameters_used": 0,
    })

    def add_operation(self, op_name: str, op_type: str, start_pos: int, end_pos: int) -> None:
        """Register a discovered operation."""
        self.discovered_operations.append({
            "name": op_name,
            "type": op_type,
            "start": start_pos,
            "end": end_pos
        })

        if op_type == "procedure":
            self.parse_stats["procedures_found"] += 1
        elif op_type == "function":
            self.parse_stats["functions_found"] += 1
        else:
            self.parse_stats["anonymous_blocks"] += 1

    def mark_table_created(self, table_name: str) -> None:
        """Mark a table as created to prevent duplicate node creation."""
        self.created_tables.add(table_name)
        self.parse_stats["tables_created"] += 1

    def mark_table_referenced(self, table_name: str) -> None:
        """Mark a table as referenced."""
        self.referenced_tables.add(table_name)
        self.parse_stats["tables_referenced"] += 1

    def is_table_created(self, table_name: str) -> bool:
        """Check if table was created with CREATE TABLE."""
        return table_name in self.created_tables

    def is_node_created(self, node_id: str) -> bool:
        """Check if node was already created."""
        return node_id in self.created_node_ids

    def register_node(self, node_id: str) -> None:
        """Register a created node."""
        self.created_node_ids.add(node_id)

    def get_connection_node_id(self, connection_name: str) -> Optional[str]:
        """Get node ID for a connection name."""
        return self.connection_id_map.get(connection_name)

    def get_parameter_node_id(self, parameter_name: str) -> Optional[str]:
        """Get node ID for a parameter name."""
        return self.parameter_id_map.get(parameter_name)

    def set_current_operation(self, operation_id: str, operation_name: str) -> None:
        """Set the current operation context."""
        self.current_operation_id = operation_id
        self.current_operation_name = operation_name

    def clear_current_operation(self) -> None:
        """Clear the current operation context."""
        self.current_operation_id = None
        self.current_operation_name = None
