"""
Graph builder for PL/SQL parser.

Handles creation of nodes and edges with proper traceability and type mapping.
Follows the same builder pattern as SSIS and Informatica parsers.
"""

import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

from ....models.graph import Node, Edge
from ....models.canonical_types import NodeType, EdgeType
from ....models.traceability import SourceContext
from ..models.parsing_context import PlsqlParsingContext
from ..type_mapping import PLSQLDataTypeMapper, TargetPlatform, detect_column_types_from_sql

logger = logging.getLogger(__name__)


class PlsqlGraphBuilder:
    """
    Builds graph nodes and edges for PL/SQL constructs.

    Responsibilities:
    - Create nodes with proper IDs and types
    - Add traceability metadata
    - Integrate type mapping for tables
    - Create edges with appropriate relationships
    - Handle FQN (Fully Qualified Names) for cross-platform consistency
    """

    def __init__(self, context: PlsqlParsingContext, type_mapper: Optional[PLSQLDataTypeMapper] = None):
        """
        Initialize graph builder with parsing context.

        Args:
            context: The parsing context for this file
            type_mapper: Optional type mapper for data type conversions
        """
        self.context = context
        self.type_mapper = type_mapper or (PLSQLDataTypeMapper() if context.enable_type_mapping else None)
        self.logger = logging.getLogger(__name__)

    def create_pipeline_node(self) -> Node:
        """
        Create a pipeline node for the PL/SQL file.

        Returns:
            Pipeline node representing the file
        """
        file_path = self.context.file_path
        pipeline_id = f"pipeline:{file_path.stem}"

        properties = {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "technology": "ORACLE",
            "pipeline_type": "plsql_file"
        }

        # Add traceability
        properties.update(SourceContext.create_node_traceability(
            source_file_path=str(file_path),
            source_file_type="sql",
            xml_path=f"//file[@name='{file_path.name}']",
            technology="ORACLE"
        ))

        node = Node(
            node_id=pipeline_id,
            node_type=NodeType.PIPELINE.value,
            name=file_path.stem,
            properties=properties
        )

        self.context.register_node(pipeline_id)
        return node

    def create_operation_node(
        self,
        operation_name: str,
        operation_type: str,
        properties: Dict[str, Any],
        line_number: int = 0
    ) -> Node:
        """
        Create an operation node (procedure, function, anonymous block).

        Args:
            operation_name: Name of the operation
            operation_type: Type (procedure, function, anonymous_block)
            properties: Additional properties
            line_number: Line number in source file

        Returns:
            Operation node
        """
        file_path = self.context.file_path
        operation_id = f"pipeline:{file_path.stem}:operation:{operation_name}"

        # Build properties
        node_props = {
            "name": operation_name,
            "operation_type": operation_type,
            "technology": "ORACLE",
            **properties
        }

        # Add traceability
        node_props.update(SourceContext.create_node_traceability(
            source_file_path=str(file_path),
            source_file_type="sql",
            line_number=line_number,
            xml_path=f"//{operation_type}[@name='{operation_name}']",
            technology="ORACLE"
        ))

        # Add platform support flags like SSIS
        node_props["supported_platforms"] = self.context.target_platforms
        node_props["type_mapping_enabled"] = self.context.enable_type_mapping

        node = Node(
            node_id=operation_id,
            node_type=NodeType.OPERATION.value,
            name=operation_name,
            properties=node_props
        )

        self.context.register_node(operation_id)
        return node

    def create_table_node(
        self,
        table_name: str,
        schema: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        create_statement: Optional[str] = None
    ) -> Node:
        """
        Create a DATA_ASSET node for a table.

        Args:
            table_name: Name of the table
            schema: Optional schema name
            properties: Additional properties
            create_statement: Optional CREATE TABLE statement for type mapping

        Returns:
            Table node
        """
        table_id = f"data_asset:table:{table_name}"

        # Check if already created
        if self.context.is_node_created(table_id):
            return None  # Already exists

        node_props = properties or {}
        node_props.update({
            "name": table_name,
            "schema": schema or "public",
            "technology": "ORACLE",
            "asset_type": "table"
        })

        # Create FQN (Fully Qualified Name)
        fqn = self._create_fqn_table(table_name, schema)
        node_props["fqn"] = fqn
        node_props["qualified_name"] = fqn

        # Add traceability
        file_path = self.context.file_path
        node_props.update(SourceContext.create_node_traceability(
            source_file_path=str(file_path),
            source_file_type="sql",
            xml_path=f"//table[@name='{table_name}']",
            technology="ORACLE"
        ))

        # Add type mapping if CREATE TABLE statement provided
        if create_statement and self.type_mapper:
            try:
                column_types = detect_column_types_from_sql(create_statement)
                if column_types:
                    node_props["type_mapping"] = {
                        "columns": column_types,
                        "source_platform": "oracle",
                        "target_platforms": self.context.target_platforms,
                        "mapping_confidence": self._calculate_mapping_confidence(column_types)
                    }
                    node_props["supported_platforms"] = self.context.target_platforms
                    node_props["type_mapping_enabled"] = True
                    self.logger.debug(f"Added type mapping for table {table_name}: {len(column_types)} columns")
            except Exception as e:
                self.logger.debug(f"Could not extract type mapping for table {table_name}: {e}")

        node = Node(
            node_id=table_id,
            node_type=NodeType.DATA_ASSET.value,
            name=table_name,
            properties=node_props
        )

        self.context.register_node(table_id)
        return node

    def create_connection_node(
        self,
        connection_name: str,
        connection_data: Dict[str, Any]
    ) -> Node:
        """
        Create a connection node from Oracle connection context.

        Args:
            connection_name: Name of the connection
            connection_data: Connection properties

        Returns:
            Connection node
        """
        conn_id = f"connection:{connection_name}"

        # Check if already created
        if self.context.is_node_created(conn_id):
            return None

        properties = {
            "technology": "ORACLE",
            "connection_type": connection_data.get("connection_type", "oracle_tns"),
            "host": connection_data.get("host", ""),
            "port": connection_data.get("port", ""),
            "service_name": connection_data.get("service_name", ""),
            "protocol": connection_data.get("protocol", "TCP"),
            "username": connection_data.get("username", ""),
            "connection_string": connection_data.get("connection_string", ""),
        }

        # Add traceability
        config_file = connection_data.get("file_path", "")
        if config_file:
            properties.update(SourceContext.create_node_traceability(
                source_file_path=config_file,
                source_file_type="config",
                xml_path=f"//connection[@name='{connection_name}']",
                technology="ORACLE"
            ))

        node = Node(
            node_id=conn_id,
            node_type=NodeType.CONNECTION.value,
            name=connection_name,
            properties=properties
        )

        self.context.register_node(conn_id)
        self.context.connection_id_map[connection_name] = conn_id
        return node

    def create_parameter_node(
        self,
        parameter_name: str,
        parameter_data: Dict[str, Any]
    ) -> Node:
        """
        Create a parameter node from Oracle parameter context.

        Args:
            parameter_name: Name of the parameter
            parameter_data: Parameter properties

        Returns:
            Parameter node
        """
        param_id = f"parameter:{parameter_name}"

        # Check if already created
        if self.context.is_node_created(param_id):
            return None

        properties = {
            "value": parameter_data.get("value", ""),
            "parameter_type": parameter_data.get("parameter_type", "oracle_define"),
            "technology": "ORACLE",
        }

        # Add traceability
        config_file = parameter_data.get("file_path", "")
        if config_file:
            properties.update(SourceContext.create_node_traceability(
                source_file_path=config_file,
                source_file_type="config",
                xml_path=f"//parameter[@name='{parameter_name}']",
                technology="ORACLE"
            ))

        node = Node(
            node_id=param_id,
            node_type=NodeType.PARAMETER.value,
            name=parameter_name,
            properties=properties
        )

        self.context.register_node(param_id)
        self.context.parameter_id_map[parameter_name] = param_id
        return node

    def create_edge(
        self,
        source_id: str,
        target_id: str,
        relation: EdgeType,
        properties: Optional[Dict[str, Any]] = None
    ) -> Edge:
        """
        Create an edge between two nodes.

        Args:
            source_id: Source node ID
            target_id: Target node ID
            relation: Edge type
            properties: Additional edge properties

        Returns:
            Edge
        """
        edge_props = properties or {}

        # Add source context for traceability
        file_path = self.context.file_path
        edge_props.update(SourceContext.create_node_traceability(
            source_file_path=str(file_path),
            source_file_type="sql",
            technology="ORACLE"
        ))

        return Edge(
            source_id=source_id,
            target_id=target_id,
            relation=relation.value,
            properties=edge_props
        )

    def _create_fqn_table(self, table_name: str, schema: Optional[str] = None) -> str:
        """Create Fully Qualified Name for tables: service.db.schema.table"""
        clean_table = table_name.strip('"').strip("'").strip()

        # If table already contains dots, parse it
        if '.' in clean_table:
            parts = clean_table.split('.')
            if len(parts) == 2:
                schema = parts[0]
                clean_table = parts[1]
            elif len(parts) >= 3:
                return clean_table  # Already fully qualified

        # Default schema if not provided
        if not schema:
            schema = "public"

        return f"oracle.default.{schema}.{clean_table}"

    def _calculate_mapping_confidence(self, column_types: List[Dict[str, Any]]) -> float:
        """Calculate overall mapping confidence based on column type conversion confidence."""
        if not column_types:
            return 0.0

        confidences = [col.get("conversion_confidence", 0.5) for col in column_types]
        return sum(confidences) / len(confidences)
