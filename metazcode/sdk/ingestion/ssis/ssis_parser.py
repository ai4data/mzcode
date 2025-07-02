import os
import re
from lxml import etree
from typing import Dict, List, Tuple, Generator, Any, Optional
import logging
import json

from ...models.canonical_types import NodeType, EdgeType
from ...models.graph import Node, Edge

logger = logging.getLogger(__name__)


class CanonicalSsisParser:
    """
    A robust SSIS parser built with lxml to create a canonical representation of the project.
    It parses a single SSIS package file (.dtsx).
    """

    def __init__(
        self,
        connections_context: Optional[Dict[str, Dict[str, Any]]] = None,
        parameters_context: Optional[Dict[str, Dict[str, Any]]] = None,
        enable_schema_introspection: bool = True,
    ):
        self.ns_map = {
            "DTS": "www.microsoft.com/SqlServer/Dts",
            "SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask",
        }
        self.connections_context = connections_context or {}
        self.parameters_context = parameters_context or {}
        self.enable_schema_introspection = enable_schema_introspection
        self.schema_cache = {}  # Cache for database schema information

    def _categorize_operation_subtype(self, native_type: str) -> str:
        """
        Categorizes SSIS operations into standardized subtypes based on their native type.

        Args:
            native_type: The SSIS native operation type (e.g., 'Microsoft.Pipeline')

        Returns:
            A standardized operation subtype: CONTROL_FLOW, DATA_FLOW, EXECUTE, or SCRIPT
        """
        # Data Flow operations
        if native_type in ["Microsoft.Pipeline"]:
            return "DATA_FLOW"

        # Control Flow containers
        elif native_type in ["STOCK:FORLOOP", "STOCK:FOREACHLOOP", "STOCK:SEQUENCE"]:
            return "CONTROL_FLOW"

        # Execute operations
        elif native_type in ["Microsoft.ExecuteSQLTask", "Microsoft.FileSystemTask"]:
            return "EXECUTE"

        # Script operations
        elif native_type in ["Microsoft.ScriptTask"]:
            return "SCRIPT"

        # Default fallback for unknown types
        else:
            logger.warning(
                f"Unknown operation native_type '{native_type}', defaulting to 'EXECUTE'"
            )
            return "EXECUTE"

    def parse(
        self, file_path: str
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Parses a single .dtsx file and yields the discovered nodes and edges.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            # Remove the BOM if it exists
            if content.startswith("\ufeff"):
                content = content[1:]
            root = etree.fromstring(content.encode("utf-8"))
            yield from self._parse_package(root, file_path)
        except Exception as e:
            logger.error(
                f"ERROR: Could not parse {file_path}. Reason: {e}", exc_info=True
            )
            return

    def _parse_package(
        self, root: etree._Element, file_path: str
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        nodes: List[Node] = []
        edges: List[Edge] = []

        # Parse connection managers first (local connections within .dtsx)
        connection_nodes, connection_id_map = self._parse_connection_managers(
            root, file_path
        )
        nodes.extend(connection_nodes)

        # Add external connections from .conmgr context to the ID map
        external_connection_map = self._build_external_connection_map()
        connection_id_map.update(external_connection_map)

        # Parse package parameters
        parameter_nodes, parameter_id_map = self._parse_package_parameters(
            root, file_path
        )
        nodes.extend(parameter_nodes)

        # Parse package variables (often used in parameter mappings)
        variable_nodes, variable_id_map = self._parse_package_variables(root, file_path)
        nodes.extend(variable_nodes)

        # Combine parameter and variable mappings for reference tracking
        param_var_id_map = {**parameter_id_map, **variable_id_map}

        # Create the pipeline node
        package_name_elem = root.find(
            "DTS:Property[@DTS:Name='ObjectName']", self.ns_map
        )
        package_name = (
            package_name_elem.text
            if package_name_elem is not None
            else os.path.basename(file_path)
        )

        pipeline_id = f"pipeline:{package_name}"
        nodes.append(
            Node(
                node_id=pipeline_id,
                node_type=NodeType.PIPELINE,
                name=package_name,
                properties={"file_path": file_path, "technology": "SSIS"},
            )
        )

        # Parse tasks
        executables_container = root.find("DTS:Executables", self.ns_map)
        if executables_container is None:
            yield nodes, edges
            return

        for task_xml in executables_container.findall("DTS:Executable", self.ns_map):
            task_name = task_xml.get(f"{{{self.ns_map['DTS']}}}ObjectName")
            task_type = task_xml.get(f"{{{self.ns_map['DTS']}}}ExecutableType")

            if not task_name or not task_type:
                continue

            task_id = f"{pipeline_id}:operation:{task_name}"

            # Get operation subtype
            operation_subtype = self._categorize_operation_subtype(task_type)

            nodes.append(
                Node(
                    node_id=task_id,
                    node_type=NodeType.OPERATION,
                    name=task_name,
                    properties={
                        "native_type": task_type,
                        "operation_subtype": operation_subtype,
                        "technology": "SSIS",
                    },
                )
            )
            edges.append(
                Edge(
                    source_id=pipeline_id, target_id=task_id, relation=EdgeType.CONTAINS
                )
            )

            # Parse task details
            object_data_xml = task_xml.find("DTS:ObjectData", self.ns_map)
            if object_data_xml is None:
                continue

            task_type_element = object_data_xml.find("*")
            if task_type_element is None:
                continue

            task_type = task_type_element.tag
            if "pipeline" in task_type.lower():
                # The task_type_element IS the pipeline element
                pipeline_xml = task_type_element
                components_xml = pipeline_xml.find("components")
                if components_xml is None:
                    continue

                for component_xml in components_xml.findall("component"):
                    self._parse_dft_component(
                        component_xml,
                        task_id,
                        nodes,
                        edges,
                        connection_id_map,
                        param_var_id_map,
                    )

            elif "SqlTaskData" in task_type:
                self._parse_execute_sql_task(
                    object_data_xml,
                    task_id,
                    nodes,
                    edges,
                    connection_id_map,
                    param_var_id_map,
                )
            else:
                logger.debug(
                    f"DEBUG: Unhandled task type: '{task_type}' for task: '{task_name}'"
                )

        # Parse precedence constraints (control flow)
        self._parse_precedence_constraints(root, pipeline_id, edges)

        yield nodes, edges

    def _parse_connection_managers(
        self, root: etree._Element, file_path: str
    ) -> Tuple[List[Node], Dict[str, str]]:
        connection_nodes: List[Node] = []
        id_map: Dict[str, str] = {}
        connections_container = root.find("DTS:ConnectionManagers", self.ns_map)
        if connections_container is None:
            return [], {}

        for conn_xml in connections_container.findall(
            "DTS:ConnectionManager", self.ns_map
        ):
            conn_name = conn_xml.get(f"{{{self.ns_map['DTS']}}}ObjectName")
            conn_guid = conn_xml.get(f"{{{self.ns_map['DTS']}}}ID")

            if not conn_name or not conn_guid:
                continue

            conn_id = f"connection:{conn_name}"

            # Start with basic properties
            properties = {
                "file_path": file_path,
                "technology": "SSIS",
                "guid": conn_guid,
            }

            # Enrich with detailed properties from .conmgr files
            conn_guid_clean = conn_guid.strip("{}")
            enrichment_data = None

            # Try to find enrichment data by name or GUID
            if conn_name in self.connections_context:
                enrichment_data = self.connections_context[conn_name]
                logger.debug(
                    f"Found enrichment data for connection '{conn_name}' by name"
                )
            elif conn_guid_clean in self.connections_context:
                enrichment_data = self.connections_context[conn_guid_clean]
                logger.debug(
                    f"Found enrichment data for connection '{conn_name}' by GUID"
                )

            if enrichment_data:
                # Merge enrichment properties
                properties.update(
                    {
                        "server": enrichment_data.get("server", ""),
                        "database": enrichment_data.get("database", ""),
                        "provider": enrichment_data.get("provider", ""),
                        "security": enrichment_data.get("security", ""),
                        "connection_string": enrichment_data.get(
                            "connection_string", ""
                        ),
                        "creation_name": enrichment_data.get("creation_name", ""),
                        "conmgr_file": enrichment_data.get("file_path", ""),
                    }
                )
                logger.debug(
                    f"Enriched connection '{conn_name}' with server='{enrichment_data.get('server')}', database='{enrichment_data.get('database')}'"
                )
            else:
                logger.debug(
                    f"No enrichment data found for connection '{conn_name}' (GUID: {conn_guid_clean})"
                )

            connection_nodes.append(
                Node(
                    node_id=conn_id,
                    node_type=NodeType.CONNECTION,
                    name=conn_name,
                    properties=properties,
                )
            )
            id_map[conn_guid] = conn_id

        return connection_nodes, id_map

    def _build_external_connection_map(self) -> Dict[str, str]:
        """
        Builds a mapping from connection GUIDs to connection node IDs for external .conmgr connections.
        """
        external_map = {}

        for key, conn_data in self.connections_context.items():
            conn_guid = conn_data.get("guid")
            conn_name = conn_data.get("connection_name")

            if conn_guid and conn_name:
                # Map GUID to connection node ID
                conn_id = f"connection:{conn_name}"
                external_map[conn_guid] = conn_id
                logger.debug(
                    f"Mapped external connection GUID '{conn_guid}' to node '{conn_id}'"
                )

        return external_map

    def _parse_dft_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        connection_id_map: Dict[str, str],
        param_var_id_map: Dict[str, str],
    ):
        class_id = component_xml.get("componentClassID", "")
        component_name = component_xml.get("name", "")

        # Extract column lineage for all components
        column_lineage = self._extract_column_lineage(
            component_xml, component_name, task_id
        )

        # Handle different component types
        if "Microsoft.DerivedColumn" in class_id:
            self._parse_derived_column_component(
                component_xml, task_id, nodes, edges, param_var_id_map
            )
        elif "Microsoft.ConditionalSplit" in class_id:
            self._parse_conditional_split_component(
                component_xml, task_id, nodes, edges, param_var_id_map
            )
        elif "Microsoft.Lookup" in class_id:
            self._parse_lookup_component(
                component_xml, task_id, nodes, edges, param_var_id_map
            )
        elif "Microsoft.OLEDBCommand" in class_id:
            self._parse_oledb_command_component(
                component_xml, task_id, nodes, edges, connection_id_map, param_var_id_map
            )
        elif "OLEDBSource" in class_id or "OLEDBDestination" in class_id:
            self._parse_oledb_component(
                component_xml,
                task_id,
                nodes,
                edges,
                connection_id_map,
                param_var_id_map,
            )
        # More transformation types will be added in subsequent phases
        else:
            # Use generic component parser for unsupported types
            self._parse_generic_component(
                component_xml, task_id, nodes, edges, column_lineage
            )

        # Add column lineage to the operation node if we have any column information
        if column_lineage["input_columns"] or column_lineage["output_columns"]:
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                if "column_lineage" not in operation_node.properties:
                    operation_node.properties["column_lineage"] = []
                operation_node.properties["column_lineage"].append(column_lineage)

                logger.debug(
                    f"Added column lineage for component {component_name}: "
                    f"{len(column_lineage['input_columns'])} inputs, "
                    f"{len(column_lineage['output_columns'])} outputs, "
                    f"{len(column_lineage['column_mappings'])} mappings"
                )
        
        # Extract error handling configuration for all components
        self._extract_error_handling_config(component_xml, task_id, nodes)

    def _parse_derived_column_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse Microsoft.DerivedColumn transformation components to extract
        transformation expressions and business logic.
        """
        component_name = component_xml.get("name", "")
        logger.debug(f"Parsing derived column component: {component_name}")

        # Look for output columns with expressions
        outputs_tag = component_xml.find("outputs")
        if outputs_tag is None:
            return

        transformations = []

        for output in outputs_tag.findall("output"):
            # Skip error outputs
            if output.get("isErrorOut") == "true":
                continue

            output_columns = output.find("outputColumns")
            if output_columns is None:
                continue

            for output_column in output_columns.findall("outputColumn"):
                column_name = output_column.get("name", "")
                properties_tag = output_column.find("properties")

                if properties_tag is not None:
                    # Extract the transformation expression
                    expression_prop = properties_tag.find(
                        "property[@name='Expression']"
                    )
                    friendly_expr_prop = properties_tag.find(
                        "property[@name='FriendlyExpression']"
                    )

                    if expression_prop is not None and expression_prop.text:
                        expression = expression_prop.text
                        friendly_expression = (
                            friendly_expr_prop.text
                            if friendly_expr_prop is not None
                            else expression
                        )

                        transformation = {
                            "column_name": column_name,
                            "expression": expression,
                            "friendly_expression": friendly_expression,
                            "data_type": output_column.get("dataType", ""),
                            "length": output_column.get("length", ""),
                        }
                        transformations.append(transformation)

                        logger.debug(
                            f"Found derived column expression: {column_name} = {expression}"
                        )

                        # Parse expression for variable/parameter references
                        self._parse_expression_dependencies(
                            expression, task_id, edges, param_var_id_map
                        )

        # Store transformations in the operation node properties
        if transformations:
            # Find or create the operation node for this component
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                if "transformations" not in operation_node.properties:
                    operation_node.properties["transformations"] = []
                operation_node.properties["transformations"].extend(transformations)

                logger.debug(
                    f"Added {len(transformations)} derived column transformations to {task_id}"
                )
                
                # Store derived column expressions in business logic format
                operation_node.properties["derived_column_expressions"] = {
                    "transformation_count": len(transformations),
                    "expressions": transformations,
                    "component_name": component_xml.get("name", "")
                }

    def _parse_conditional_split_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse Microsoft.ConditionalSplit transformation components to extract
        conditional expressions and routing logic.
        """
        component_name = component_xml.get("name", "")
        logger.debug(f"Parsing conditional split component: {component_name}")

        # Look for output branches with conditions
        outputs_tag = component_xml.find("outputs")
        if outputs_tag is None:
            return

        conditions = []
        default_output = None

        for output in outputs_tag.findall("output"):
            # Skip error outputs
            if output.get("isErrorOut") == "true":
                continue

            output_name = output.get("name", "")
            properties_tag = output.find("properties")

            if properties_tag is not None:
                # Check if this is the default output
                is_default_prop = properties_tag.find("property[@name='IsDefaultOut']")
                if is_default_prop is not None and is_default_prop.text == "true":
                    default_output = {
                        "output_name": output_name,
                        "is_default": True,
                        "description": output.get("description", ""),
                    }
                    continue

                # Extract the conditional expression
                expression_prop = properties_tag.find("property[@name='Expression']")
                friendly_expr_prop = properties_tag.find(
                    "property[@name='FriendlyExpression']"
                )
                evaluation_order_prop = properties_tag.find(
                    "property[@name='EvaluationOrder']"
                )

                if expression_prop is not None and expression_prop.text:
                    expression = expression_prop.text
                    friendly_expression = (
                        friendly_expr_prop.text
                        if friendly_expr_prop is not None
                        else expression
                    )
                    evaluation_order = (
                        int(evaluation_order_prop.text)
                        if evaluation_order_prop is not None
                        else 0
                    )

                    condition = {
                        "output_name": output_name,
                        "expression": expression,
                        "friendly_expression": friendly_expression,
                        "evaluation_order": evaluation_order,
                        "description": output.get("description", ""),
                        "is_default": False,
                    }
                    conditions.append(condition)

                    logger.debug(
                        f"Found conditional split condition [{evaluation_order}]: {output_name} = {friendly_expression}"
                    )

                    # Parse expression for variable/parameter references
                    self._parse_expression_dependencies(
                        expression, task_id, edges, param_var_id_map
                    )

        # Sort conditions by evaluation order
        conditions.sort(key=lambda x: x["evaluation_order"])

        # Add default output to the end if it exists
        if default_output:
            conditions.append(default_output)

        # Store conditions in the operation node properties
        if conditions:
            # Find the operation node for this component
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                if "conditions" not in operation_node.properties:
                    operation_node.properties["conditions"] = []
                operation_node.properties["conditions"].extend(conditions)

                logger.debug(
                    f"Added {len(conditions)} conditional split conditions to {task_id}"
                )

    def _parse_expression_dependencies(
        self,
        expression: str,
        task_id: str,
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse SSIS expressions to identify variable and parameter references.

        Common SSIS expression patterns:
        - Variables: @[User::VariableName] or @VariableName
        - Parameters: $Project::ParameterName or $Package::ParameterName
        - Functions: GETDATE(), LEN(), SUBSTRING(), etc.
        - Column references: [ColumnName] or just ColumnName
        """
        if not expression:
            return

        import re

        # Pattern for variable references: @[User::VariableName] or @[System::VariableName]
        variable_pattern = r"@\[(?:User::|System::)?([^\]]+)\]"
        variable_matches = re.findall(variable_pattern, expression, re.IGNORECASE)

        for var_name in variable_matches:
            # Look for variable in our mapping
            var_id = f"variable:{var_name}"
            if var_id in param_var_id_map.values():
                edges.append(
                    Edge(
                        source_id=task_id,
                        target_id=var_id,
                        relation=EdgeType.USES_VARIABLE,
                    )
                )
                logger.debug(f"Found variable reference in expression: {var_name}")

        # Pattern for parameter references: $Project::ParamName or $Package::ParamName
        param_pattern = r"\$(?:Project::|Package::)([^\s\)]+)"
        param_matches = re.findall(param_pattern, expression, re.IGNORECASE)

        for param_name in param_matches:
            param_id = f"parameter:{param_name}"
            if param_id in param_var_id_map.values():
                edges.append(
                    Edge(
                        source_id=task_id,
                        target_id=param_id,
                        relation=EdgeType.USES_PARAMETER,
                    )
                )
                logger.debug(f"Found parameter reference in expression: {param_name}")

    def _parse_oledb_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        connection_id_map: Dict[str, str],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse OLE DB Source and Destination components (extracted from original method).
        """
        class_id = component_xml.get("componentClassID", "")
        is_source = "OLEDBSource" in class_id
        is_destination = "OLEDBDestination" in class_id

        if not is_source and not is_destination:
            return

        # Parse connection
        conn_guid = None
        connections_tag = component_xml.find("connections")
        if connections_tag is not None:
            connection_tag = connections_tag.find("connection")
            if connection_tag is not None:
                conn_mgr_id_attr = connection_tag.get("connectionManagerID")
                if conn_mgr_id_attr:
                    conn_guid = conn_mgr_id_attr.split(":")[0].strip("{}")

        if conn_guid and conn_guid in connection_id_map:
            edges.append(
                Edge(
                    source_id=task_id,
                    target_id=connection_id_map[conn_guid],
                    relation=EdgeType.USES_CONNECTION,
                )
            )

        # Parse parameter mappings
        properties_tag = component_xml.find("properties")
        if properties_tag is not None:
            param_mapping_prop = properties_tag.find(
                "property[@name='ParameterMapping']"
            )
            if param_mapping_prop is not None and param_mapping_prop.text:
                self._parse_parameter_mapping(
                    param_mapping_prop.text, task_id, edges, param_var_id_map
                )

        # Parse table information
        table_name = None
        if properties_tag is not None:
            prop_names_to_check = ["OpenRowset", "SqlCommand", "TableName"]
            for prop_name in prop_names_to_check:
                prop = properties_tag.find(f"property[@name='{prop_name}']")
                if prop is not None and prop.text:
                    if "SELECT" in prop.text.upper():
                        found_tables = re.findall(
                            r"(?:FROM|JOIN)\s+\[?(\w+)\]?\.\[?(\w+)\]?",
                            prop.text,
                            re.IGNORECASE,
                        )
                        if found_tables:
                            table_name = f"{found_tables[0][0]}.{found_tables[0][1]}"
                            break
                    else:
                        table_name = prop.text
                        break

        if table_name:
            table_name = table_name.strip("[]")
            table_id = f"table:{table_name}"
            if not any(n.node_id == table_id for n in nodes):
                # Enhanced table properties with schema introspection
                table_properties = {
                    "technology": "SSIS",
                    "table_name": table_name
                }
                
                # Add schema introspection if connection context is available
                if conn_guid and conn_guid in self.connections_context:
                    conn_info = self.connections_context[conn_guid]
                    connection_string = conn_info.get("connection_string")
                    if connection_string:
                        schema_info = self._introspect_table_schema(connection_string, table_name)
                        table_properties["schema_details"] = schema_info
                        
                        # Add connection reference
                        table_properties["connection_id"] = conn_guid
                        table_properties["database"] = conn_info.get("database")
                        table_properties["server"] = conn_info.get("server")
                
                nodes.append(
                    Node(
                        node_id=table_id,
                        node_type=NodeType.TABLE,
                        name=table_name,
                        properties=table_properties,
                    )
                )

            relation = EdgeType.READS_FROM if is_source else EdgeType.WRITES_TO
            edges.append(Edge(source_id=task_id, target_id=table_id, relation=relation))

    def _parse_parameter_mapping(
        self,
        mapping_text: str,
        task_id: str,
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parses parameter mapping strings like:
        "Parameter0:Input",{6A1D4288-D951-460C-A98D-17C167544F57};
        """
        if not mapping_text:
            return

        # Extract parameter GUID from mapping text
        import re

        guid_pattern = r"\{([A-F0-9\-]+)\}"
        matches = re.findall(guid_pattern, mapping_text, re.IGNORECASE)

        for guid in matches:
            if guid in param_var_id_map:
                target_id = param_var_id_map[guid]

                # Determine if it's a parameter or variable based on node ID prefix
                if target_id.startswith("parameter:"):
                    relation = EdgeType.USES_PARAMETER
                    edge_type = "USES_PARAMETER"
                elif target_id.startswith("variable:"):
                    relation = EdgeType.USES_VARIABLE
                    edge_type = "USES_VARIABLE"
                else:
                    relation = EdgeType.USES_PARAMETER  # Default fallback
                    edge_type = "USES_PARAMETER"

                edges.append(
                    Edge(
                        source_id=task_id,
                        target_id=target_id,
                        relation=relation,
                    )
                )
                logger.debug(f"Created {edge_type} edge: {task_id} -> {target_id}")
            else:
                logger.debug(f"Parameter/Variable GUID {guid} not found in mapping")

    def _parse_execute_sql_task(
        self,
        object_data_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        connection_id_map: Dict[str, str],
        param_var_id_map: Dict[str, str],
    ):
        sql_task_data_xml = object_data_xml.find("SQLTask:SqlTaskData", self.ns_map)
        if sql_task_data_xml is None:
            return

        # Parse connection
        connection_ref = sql_task_data_xml.get(
            f"{{{self.ns_map['SQLTask']}}}Connection"
        )
        if connection_ref and connection_ref in connection_id_map:
            edges.append(
                Edge(
                    source_id=task_id,
                    target_id=connection_id_map[connection_ref],
                    relation=EdgeType.USES_CONNECTION,
                )
            )

        # Parse SQL statement - this is the embedded script content
        sql_statement = sql_task_data_xml.get(
            f"{{{self.ns_map['SQLTask']}}}SqlStatementSource"
        )
        if sql_statement:
            # Store the embedded SQL in the operation node properties
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                # Enhanced SQL transformation logic extraction
                sql_info = {
                    "sql_query": sql_statement,
                    "connection_ref": connection_ref,
                    "query_type": self._determine_sql_type(sql_statement),
                    "parameters": self._extract_sql_parameters(sql_statement),
                    "affected_tables": self._extract_table_references(sql_statement),
                    "has_placeholders": "?" in sql_statement
                }
                operation_node.properties["sql_transformation"] = sql_info

                logger.debug(
                    f"Added SQL transformation logic to {task_id}: {sql_statement[:100]}..."
                )

            # Check for parameter placeholders (? marks) in SQL
            if "?" in sql_statement:
                logger.debug(
                    f"Found parameterized SQL in task {task_id}: {sql_statement[:100]}..."
                )

                # Look for parameter mapping properties
                param_mapping_prop = sql_task_data_xml.find(
                    "DTS:Property[@DTS:Name='ParameterMapping']", self.ns_map
                )
                if param_mapping_prop is not None and param_mapping_prop.text:
                    self._parse_parameter_mapping(
                        param_mapping_prop.text, task_id, edges, param_var_id_map
                    )

            # Extract table references from SQL for lineage
            found_tables = re.findall(
                r"(?:FROM|JOIN|UPDATE|INTO)\s+\[?(\w+)\]?\.\[?(\w+)\]?",
                sql_statement,
                re.IGNORECASE,
            )
            for schema, table in found_tables:
                table_name = f"{schema}.{table}"
                table_id = f"table:{table_name}"
                if not any(n.node_id == table_id for n in nodes):
                    nodes.append(
                        Node(
                            node_id=table_id,
                            node_type=NodeType.TABLE,
                            name=table_name,
                            properties={"technology": "SSIS"},
                        )
                    )

                relation = (
                    EdgeType.WRITES_TO
                    if "UPDATE" in sql_statement.upper()
                    or "INSERT" in sql_statement.upper()
                    else EdgeType.READS_FROM
                )
                edges.append(
                    Edge(source_id=task_id, target_id=table_id, relation=relation)
                )

            # Parse expression for variable/parameter references
            self._parse_expression_dependencies(
                sql_statement, task_id, edges, param_var_id_map
            )

    def _determine_sql_type(self, sql_statement: str) -> str:
        """Determine the type of SQL operation."""
        sql_upper = sql_statement.upper().strip()
        if sql_upper.startswith('SELECT'):
            return 'SELECT'
        elif sql_upper.startswith('INSERT'):
            return 'INSERT'
        elif sql_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif sql_upper.startswith('DELETE'):
            return 'DELETE'
        elif sql_upper.startswith('EXEC'):
            return 'EXECUTE'
        elif sql_upper.startswith('CREATE'):
            return 'CREATE'
        elif sql_upper.startswith('DROP'):
            return 'DROP'
        else:
            return 'UNKNOWN'

    def _extract_sql_parameters(self, sql_statement: str) -> List[Dict[str, Any]]:
        """Extract parameter placeholders and their positions."""
        parameters = []
        param_count = sql_statement.count('?')
        for i in range(param_count):
            parameters.append({
                "position": i,
                "placeholder": "?",
                "description": f"Parameter {i+1}"
            })
        return parameters

    def _extract_table_references(self, sql_statement: str) -> List[Dict[str, str]]:
        """Extract table references from SQL statement."""
        tables = []
        # Enhanced regex to capture various SQL patterns
        patterns = [
            r"FROM\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?",
            r"JOIN\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?",
            r"UPDATE\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?",
            r"INSERT\s+INTO\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?",
            r"DELETE\s+FROM\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, sql_statement, re.IGNORECASE)
            for schema, table in matches:
                tables.append({
                    "schema": schema,
                    "table": table,
                    "full_name": f"{schema}.{table}"
                })
        
        return tables

    def _parse_oledb_command_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        connection_id_map: Dict[str, str],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse Microsoft.OLEDBCommand components to extract SQL command logic.
        This handles embedded SQL commands within data flows.
        """
        component_name = component_xml.get("name", "")
        logger.debug(f"Parsing OLE DB Command component: {component_name}")
        
        # Extract SQL command from properties
        properties_tag = component_xml.find("properties")
        if properties_tag is not None:
            sql_command_prop = properties_tag.find("property[@name='SqlCommand']")
            if sql_command_prop is not None and sql_command_prop.text:
                sql_command = sql_command_prop.text.strip()
                
                # Find the operation node and add SQL transformation logic
                operation_node = next((n for n in nodes if n.node_id == task_id), None)
                if operation_node:
                    # Extract connection information
                    conn_ref = None
                    connections_tag = component_xml.find("connections")
                    if connections_tag is not None:
                        connection_tag = connections_tag.find("connection")
                        if connection_tag is not None:
                            conn_mgr_id = connection_tag.get("connectionManagerID")
                            if conn_mgr_id:
                                conn_guid = conn_mgr_id.split(":")[0].strip("{}")
                                if conn_guid in connection_id_map:
                                    conn_ref = connection_id_map[conn_guid]
                    
                    # Create SQL transformation info
                    sql_info = {
                        "sql_query": sql_command,
                        "connection_ref": conn_ref,
                        "query_type": self._determine_sql_type(sql_command),
                        "parameters": self._extract_sql_parameters(sql_command),
                        "affected_tables": self._extract_table_references(sql_command),
                        "has_placeholders": "?" in sql_command,
                        "component_type": "OLE DB Command",
                        "component_name": component_name
                    }
                    operation_node.properties["sql_transformation"] = sql_info
                    
                    logger.debug(
                        f"Added OLE DB Command SQL to {task_id}: {sql_command[:100]}..."
                    )
                    
                    # Parse expression for dependencies
                    self._parse_expression_dependencies(
                        sql_command, task_id, edges, param_var_id_map
                    )

    def _extract_error_handling_config(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node]
    ):
        """
        Extract error handling configuration from SSIS components.
        This captures error output redirections, failure handling logic, and retry policies.
        """
        error_config = {
            "has_error_output": False,
            "error_outputs": [],
            "input_error_configs": [],
            "output_error_configs": []
        }
        
        # Check for error outputs
        outputs_tag = component_xml.find("outputs")
        if outputs_tag is not None:
            for output in outputs_tag.findall("output"):
                if output.get("isErrorOut") == "true":
                    error_config["has_error_output"] = True
                    error_output = {
                        "name": output.get("name", ""),
                        "description": output.get("description", ""),
                        "ref_id": output.get("refId", "")
                    }
                    error_config["error_outputs"].append(error_output)
        
        # Check for input error handling configurations
        inputs_tag = component_xml.find("inputs")
        if inputs_tag is not None:
            for input_elem in inputs_tag.findall("input"):
                error_disposition = input_elem.get("errorRowDisposition")
                truncation_disposition = input_elem.get("truncationRowDisposition")
                
                if error_disposition or truncation_disposition:
                    input_error_config = {
                        "input_name": input_elem.get("name", ""),
                        "error_row_disposition": error_disposition,
                        "truncation_row_disposition": truncation_disposition,
                        "error_operation": input_elem.get("errorOrTruncationOperation", "")
                    }
                    error_config["input_error_configs"].append(input_error_config)
        
        # Add error configuration to operation node if any error handling is found
        if (error_config["has_error_output"] or 
            error_config["input_error_configs"] or 
            error_config["output_error_configs"]):
            
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                operation_node.properties["error_handling"] = error_config
                logger.debug(
                    f"Added error handling config to {task_id}: "
                    f"{len(error_config['error_outputs'])} error outputs, "
                    f"{len(error_config['input_error_configs'])} input configs"
                )

    def _parse_precedence_constraints(
        self, root: etree._Element, pipeline_id: str, edges: List[Edge]
    ):
        """
        Parse precedence constraints to capture control flow between tasks.
        Creates PRECEDES edges between operation nodes.
        """
        precedence_constraints = root.findall(
            ".//DTS:PrecedenceConstraint", self.ns_map
        )

        logger.debug(f"Found {len(precedence_constraints)} precedence constraints")

        # Get list of existing task IDs from current edges to validate references
        existing_task_ids = set()
        for edge in edges:
            if edge.relation == EdgeType.CONTAINS and edge.source_id == pipeline_id:
                existing_task_ids.add(edge.target_id)

        for constraint in precedence_constraints:
            from_attr = constraint.get(f"{{{self.ns_map['DTS']}}}From")
            to_attr = constraint.get(f"{{{self.ns_map['DTS']}}}To")

            logger.debug(f"Constraint: From='{from_attr}', To='{to_attr}'")

            if not from_attr or not to_attr:
                continue

            # Extract task names from the format "Package\TaskName"
            from_task = self._extract_task_name_from_ref(from_attr)
            to_task = self._extract_task_name_from_ref(to_attr)

            logger.debug(f"Extracted tasks: From='{from_task}', To='{to_task}'")

            if from_task and to_task:
                from_task_id = f"{pipeline_id}:operation:{from_task}"
                to_task_id = f"{pipeline_id}:operation:{to_task}"

                logger.debug(f"Task IDs: From='{from_task_id}', To='{to_task_id}'")

                # Only create edge if both tasks exist
                if (
                    from_task_id in existing_task_ids
                    and to_task_id in existing_task_ids
                ):
                    edges.append(
                        Edge(
                            source_id=from_task_id,
                            target_id=to_task_id,
                            relation=EdgeType.PRECEDES,
                        )
                    )
                    logger.debug(
                        f"Created PRECEDES edge: {from_task_id} -> {to_task_id}"
                    )
                else:
                    logger.debug(
                        f"Skipping constraint - tasks not found in graph: {from_task_id} or {to_task_id}"
                    )

    def _extract_task_name_from_ref(self, task_ref: str) -> str:
        """
        Extract task name from SSIS task reference format.
        Handles formats like "Package\TaskName" or just "TaskName"
        """
        if "\\" in task_ref:
            return task_ref.split("\\")[-1]
        return task_ref

    def _parse_package_parameters(
        self, root: etree._Element, file_path: str
    ) -> Tuple[List[Node], Dict[str, str]]:
        """
        Parses package parameters from DTS:PackageParameters section.
        Returns parameter nodes and a mapping from parameter GUIDs to node IDs.
        """
        parameter_nodes: List[Node] = []
        id_map: Dict[str, str] = {}

        # Find the PackageParameters container
        parameters_container = root.find("DTS:PackageParameters", self.ns_map)
        if parameters_container is None:
            return [], {}

        for param_xml in parameters_container.findall(
            "DTS:PackageParameter", self.ns_map
        ):
            param_name = param_xml.get(f"{{{self.ns_map['DTS']}}}ObjectName")
            param_guid = param_xml.get(f"{{{self.ns_map['DTS']}}}DTSID")
            param_data_type = param_xml.get(f"{{{self.ns_map['DTS']}}}DataType")
            param_required = param_xml.get(f"{{{self.ns_map['DTS']}}}Required", "False")

            if not param_name or not param_guid:
                continue

            # Extract parameter value
            param_value = ""
            param_value_elem = param_xml.find(
                "DTS:Property[@DTS:Name='ParameterValue']", self.ns_map
            )
            if param_value_elem is not None and param_value_elem.text:
                param_value = param_value_elem.text.strip('"')

            param_id = f"parameter:{param_name}"

            # Build properties
            properties = {
                "file_path": file_path,
                "technology": "SSIS",
                "guid": param_guid.strip("{}"),
                "data_type": param_data_type or "unknown",
                "required": param_required.lower() == "true",
                "value": param_value,
                "scope": "package",
            }

            parameter_nodes.append(
                Node(
                    node_id=param_id,
                    node_type=NodeType.PARAMETER,
                    name=param_name,
                    properties=properties,
                )
            )

            # Map GUID to parameter node ID for reference tracking
            id_map[param_guid.strip("{}")] = param_id

            logger.debug(
                f"Parsed package parameter: {param_name} (Type: {param_data_type}, Required: {param_required})"
            )

        return parameter_nodes, id_map

    def _parse_package_variables(
        self, root: etree._Element, file_path: str
    ) -> Tuple[List[Node], Dict[str, str]]:
        """
        Parses package variables from DTS:Variables section.
        Returns variable nodes and a mapping from variable GUIDs to node IDs.
        """
        variable_nodes: List[Node] = []
        id_map: Dict[str, str] = {}

        # Find the Variables container
        variables_container = root.find("DTS:Variables", self.ns_map)
        if variables_container is None:
            return [], {}

        for var_xml in variables_container.findall("DTS:Variable", self.ns_map):
            var_name = var_xml.get(f"{{{self.ns_map['DTS']}}}ObjectName")
            var_guid = var_xml.get(f"{{{self.ns_map['DTS']}}}DTSID")
            var_namespace = var_xml.get(f"{{{self.ns_map['DTS']}}}Namespace", "User")

            if not var_name or not var_guid:
                continue

            # Extract variable value from DTS:VariableValue element
            var_value = ""
            var_value_elem = var_xml.find("DTS:VariableValue", self.ns_map)
            if var_value_elem is not None and var_value_elem.text:
                var_value = var_value_elem.text

            # Extract data type from VariableValue element
            var_data_type = "unknown"
            if var_value_elem is not None:
                var_data_type = var_value_elem.get(
                    f"{{{self.ns_map['DTS']}}}DataType", "unknown"
                )

            var_id = f"variable:{var_namespace}.{var_name}"

            # Build properties
            properties = {
                "file_path": file_path,
                "technology": "SSIS",
                "guid": var_guid.strip("{}"),
                "data_type": var_data_type,
                "value": var_value,
                "namespace": var_namespace,
                "scope": "package",
            }

            variable_nodes.append(
                Node(
                    node_id=var_id,
                    node_type=NodeType.VARIABLE,
                    name=f"{var_namespace}.{var_name}",
                    properties=properties,
                )
            )

            # Map GUID to variable node ID for reference tracking
            id_map[var_guid.strip("{}")] = var_id

            logger.debug(
                f"Parsed package variable: {var_namespace}.{var_name} (Type: {var_data_type}, Value: {var_value[:50]}...)"
            )

        return variable_nodes, id_map

    def _parse_lookup_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse Microsoft.Lookup transformation components to extract
        join conditions and reference table mappings.
        """
        component_name = component_xml.get("name", "")
        logger.debug(f"Parsing lookup component: {component_name}")

        properties_tag = component_xml.find("properties")
        if properties_tag is None:
            return

        # Extract SQL commands
        sql_command_prop = properties_tag.find("property[@name='SqlCommand']")
        sql_command_param_prop = properties_tag.find(
            "property[@name='SqlCommandParam']"
        )
        parameter_map_prop = properties_tag.find("property[@name='ParameterMap']")
        no_match_behavior_prop = properties_tag.find(
            "property[@name='NoMatchBehavior']"
        )

        lookup_info = {
            "lookup_name": component_name,
            "sql_command": sql_command_prop.text
            if sql_command_prop is not None
            else "",
            "sql_command_param": sql_command_param_prop.text
            if sql_command_param_prop is not None
            else "",
            "parameter_map": parameter_map_prop.text
            if parameter_map_prop is not None
            else "",
            "no_match_behavior": int(no_match_behavior_prop.text)
            if no_match_behavior_prop is not None
            else 0,
            "join_conditions": [],
            "output_columns": [],
        }

        # Extract join conditions from input columns
        inputs_tag = component_xml.find("inputs")
        if inputs_tag is not None:
            for input_elem in inputs_tag.findall("input"):
                input_columns_tag = input_elem.find("inputColumns")
                if input_columns_tag is not None:
                    for input_column in input_columns_tag.findall("inputColumn"):
                        column_name = input_column.get("cachedName", "")
                        column_properties = input_column.find("properties")

                        if column_properties is not None:
                            join_to_ref_prop = column_properties.find(
                                "property[@name='JoinToReferenceColumn']"
                            )
                            if join_to_ref_prop is not None and join_to_ref_prop.text:
                                join_condition = {
                                    "input_column": column_name,
                                    "reference_column": join_to_ref_prop.text,
                                    "data_type": input_column.get("cachedDataType", ""),
                                    "length": input_column.get("cachedLength", ""),
                                }
                                lookup_info["join_conditions"].append(join_condition)

                                logger.debug(
                                    f"Found lookup join: {column_name} -> {join_to_ref_prop.text}"
                                )

        # Extract output columns
        outputs_tag = component_xml.find("outputs")
        if outputs_tag is not None:
            for output in outputs_tag.findall("output"):
                # Skip error outputs and no-match outputs for now
                if output.get(
                    "isErrorOut"
                ) == "true" or "sans correspondance" in output.get("name", ""):
                    continue

                output_columns_tag = output.find("outputColumns")
                if output_columns_tag is not None:
                    for output_column in output_columns_tag.findall("outputColumn"):
                        column_name = output_column.get("name", "")
                        column_properties = output_column.find("properties")

                        if column_properties is not None:
                            copy_from_ref_prop = column_properties.find(
                                "property[@name='CopyFromReferenceColumn']"
                            )
                            if (
                                copy_from_ref_prop is not None
                                and copy_from_ref_prop.text
                            ):
                                output_col = {
                                    "output_column": column_name,
                                    "reference_column": copy_from_ref_prop.text,
                                    "data_type": output_column.get("dataType", ""),
                                }
                                lookup_info["output_columns"].append(output_col)

                                logger.debug(
                                    f"Found lookup output: {copy_from_ref_prop.text} -> {column_name}"
                                )

        # Extract reference table name from SQL command
        if lookup_info["sql_command"]:
            import re

            # Try to extract table name from "select * from [dbo].[TableName]"
            table_match = re.search(
                r"from\s+\[?([^\[\]\s]+)\]?\.\[?([^\[\]\s]+)\]?",
                lookup_info["sql_command"],
                re.IGNORECASE,
            )
            if table_match:
                lookup_info["reference_schema"] = table_match.group(1)
                lookup_info["reference_table"] = table_match.group(2)
            else:
                # Fallback: try simpler pattern
                table_match = re.search(
                    r"from\s+\[?([^\[\]\s]+)\]?",
                    lookup_info["sql_command"],
                    re.IGNORECASE,
                )
                if table_match:
                    lookup_info["reference_table"] = table_match.group(1)

        # Store lookup info in the operation node properties
        if lookup_info["join_conditions"] or lookup_info["output_columns"]:
            # Find the operation node for this component
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                if "lookups" not in operation_node.properties:
                    operation_node.properties["lookups"] = []
                operation_node.properties["lookups"].append(lookup_info)

                logger.debug(
                    f"Added lookup with {len(lookup_info['join_conditions'])} join conditions "
                    f"and {len(lookup_info['output_columns'])} output columns to {task_id}"
                )

    def _extract_column_lineage(
        self,
        component_xml: etree._Element,
        component_name: str,
        task_id: str,
    ) -> Dict[str, Any]:
        """
        Extracts column-level lineage mappings from SSIS components.

        Args:
            component_xml: The component XML element
            component_name: Name of the component
            task_id: Parent task ID

        Returns:
            Dictionary containing column lineage information
        """
        column_lineage = {
            "component_name": component_name,
            "input_columns": [],
            "output_columns": [],
            "column_mappings": [],
        }

        # Extract input columns with their lineage IDs
        inputs_xml = component_xml.find("inputs")
        if inputs_xml is not None:
            for input_xml in inputs_xml.findall("input"):
                input_name = input_xml.get("name", "")
                input_columns_xml = input_xml.find("inputColumns")

                if input_columns_xml is not None:
                    for input_col_xml in input_columns_xml.findall("inputColumn"):
                        col_name = input_col_xml.get("cachedName") or input_col_xml.get(
                            "name"
                        )
                        lineage_id = input_col_xml.get("lineageId")
                        data_type = input_col_xml.get("cachedDataType")
                        length = input_col_xml.get("cachedLength")

                        # Extract output lineage ID from properties
                        output_lineage_id = None
                        properties_xml = input_col_xml.find("properties")
                        if properties_xml is not None:
                            for prop_xml in properties_xml.findall("property"):
                                if prop_xml.get("name") == "OutputColumnLineageID":
                                    output_lineage_id = prop_xml.text
                                    # Remove the #{} wrapper if present
                                    if (
                                        output_lineage_id
                                        and output_lineage_id.startswith("#{")
                                        and output_lineage_id.endswith("}")
                                    ):
                                        output_lineage_id = output_lineage_id[2:-1]
                                    break

                        input_column = {
                            "column_name": col_name,
                            "input_name": input_name,
                            "lineage_id": lineage_id,
                            "output_lineage_id": output_lineage_id,
                            "data_type": data_type,
                            "length": length,
                        }
                        column_lineage["input_columns"].append(input_column)

                        # Create column mapping if we have both input and output lineage
                        if lineage_id and output_lineage_id:
                            column_lineage["column_mappings"].append(
                                {
                                    "source_column": col_name,
                                    "source_lineage_id": lineage_id,
                                    "target_lineage_id": output_lineage_id,
                                    "transformation_type": "pass_through",
                                }
                            )

        # Extract output columns
        outputs_xml = component_xml.find("outputs")
        if outputs_xml is not None:
            for output_xml in outputs_xml.findall("output"):
                output_name = output_xml.get("name", "")
                output_columns_xml = output_xml.find("outputColumns")

                if output_columns_xml is not None:
                    for output_col_xml in output_columns_xml.findall("outputColumn"):
                        col_name = output_col_xml.get("name")
                        lineage_id = output_col_xml.get("lineageId")
                        data_type = output_col_xml.get("dataType")
                        length = output_col_xml.get("length")

                        # Check if this is a derived column with expression
                        expression = None
                        properties_xml = output_col_xml.find("properties")
                        if properties_xml is not None:
                            for prop_xml in properties_xml.findall("property"):
                                if prop_xml.get("name") == "Expression":
                                    expression = prop_xml.text
                                    break

                        output_column = {
                            "column_name": col_name,
                            "output_name": output_name,
                            "lineage_id": lineage_id,
                            "data_type": data_type,
                            "length": length,
                            "expression": expression,
                        }
                        column_lineage["output_columns"].append(output_column)

                        # If there's an expression, this is a transformation
                        if expression:
                            column_lineage["column_mappings"].append(
                                {
                                    "target_column": col_name,
                                    "target_lineage_id": lineage_id,
                                    "expression": expression,
                                    "transformation_type": "derived_column",
                                }
                            )

        return column_lineage

    def _resolve_expression_with_parameters(self, expression: str, param_var_id_map: Dict[str, str]) -> Dict[str, Any]:
        """
        Enhanced expression resolution that tracks parameter usage and provides resolved values.
        
        Args:
            expression: The raw SSIS expression
            param_var_id_map: Mapping of parameter/variable names to their IDs
            
        Returns:
            Dictionary containing expression analysis and resolved values
        """
        if not expression:
            return {
                "raw_expression": expression,
                "uses_parameters": [],
                "uses_variables": [],
                "resolved_expression": expression,
                "is_parameterized": False
            }
            
        import re
        
        result = {
            "raw_expression": expression,
            "uses_parameters": [],
            "uses_variables": [],
            "resolved_expression": expression,
            "is_parameterized": False
        }
        
        # Enhanced parameter pattern matching
        param_patterns = [
            r"\$Project::([\w\d_]+)",  # $Project::ParameterName
            r"\$Package::([\w\d_]+)",  # $Package::ParameterName
            r"@\[\$Project::([^\]]+)\]",  # @[$Project::ParameterName]
            r"@\[\$Package::([^\]]+)\]"   # @[$Package::ParameterName]
        ]
        
        for pattern in param_patterns:
            matches = re.findall(pattern, expression, re.IGNORECASE)
            for match in matches:
                param_name = match.strip()
                if param_name not in result["uses_parameters"]:
                    result["uses_parameters"].append(param_name)
                    result["is_parameterized"] = True
                    
                    # Try to resolve parameter value from context
                    param_key = f"parameter:{param_name}"
                    if param_key in param_var_id_map:
                        param_value = self._get_parameter_value(param_name)
                        if param_value:
                            # Replace parameter reference with actual value in resolved expression
                            param_ref_patterns = [
                                f"\$Project::{param_name}",
                                f"\$Package::{param_name}",
                                f"@[\$Project::{param_name}]",
                                f"@[\$Package::{param_name}]"
                            ]
                            for ref_pattern in param_ref_patterns:
                                result["resolved_expression"] = result["resolved_expression"].replace(
                                    ref_pattern, str(param_value)
                                )
        
        # Enhanced variable pattern matching
        variable_patterns = [
            r"@\[User::([^\]]+)\]",     # @[User::VariableName]
            r"@\[System::([^\]]+)\]",   # @[System::VariableName]
            r"@([\w\d_]+)"              # @VariableName (simple form)
        ]
        
        for pattern in variable_patterns:
            matches = re.findall(pattern, expression, re.IGNORECASE)
            for match in matches:
                var_name = match.strip()
                if var_name not in result["uses_variables"]:
                    result["uses_variables"].append(var_name)
                    result["is_parameterized"] = True
                    
                    # Try to resolve variable value from context
                    var_key = f"variable:{var_name}"
                    if var_key in param_var_id_map:
                        var_value = self._get_variable_value(var_name)
                        if var_value:
                            # Replace variable reference with actual value
                            var_ref_patterns = [
                                f"@[User::{var_name}]",
                                f"@[System::{var_name}]",
                                f"@{var_name}"
                            ]
                            for ref_pattern in var_ref_patterns:
                                result["resolved_expression"] = result["resolved_expression"].replace(
                                    ref_pattern, str(var_value)
                                )
        
        return result
    
    def _get_parameter_value(self, param_name: str) -> Optional[str]:
        """
        Retrieve parameter value from parameters context.
        """
        for param_info in self.parameters_context.values():
            if param_info.get("name") == param_name:
                return param_info.get("value")
        return None
    
    def _get_variable_value(self, var_name: str) -> Optional[str]:
        """
        Retrieve variable value from the parsed variables.
        """
        # This would need to be implemented based on how variables are stored
        # For now, return None as we'd need access to the variable context
        return None
    
    def _introspect_table_schema(self, connection_string: str, table_name: str) -> Dict[str, Any]:
        """
        Introspect database table schema to get detailed metadata.
        
        Args:
            connection_string: Database connection string
            table_name: Name of the table to introspect
            
        Returns:
            Dictionary containing detailed schema information
        """
        # Create cache key
        cache_key = f"{connection_string}::{table_name}"
        if cache_key in self.schema_cache:
            return self.schema_cache[cache_key]
            
        schema_info = {
            "table_name": table_name,
            "schema_introspected": False,
            "columns": [],
            "indexes": [],
            "constraints": [],
            "table_type": "TABLE",
            "row_count_estimate": None,
            "introspection_error": None
        }
        
        if not self.enable_schema_introspection:
            schema_info["introspection_error"] = "Schema introspection disabled"
            self.schema_cache[cache_key] = schema_info
            return schema_info
        
        try:
            # For now, we'll skip actual database connection due to environment limitations
            # In a real implementation, this would connect to the database and introspect
            schema_info["introspection_error"] = "Database introspection not implemented in this demo"
            logger.debug(f"Schema introspection requested for {table_name} but not implemented")
                
        except Exception as e:
            schema_info["introspection_error"] = f"Schema introspection error: {str(e)}"
            logger.debug(f"Unexpected error during schema introspection: {e}")
        
        # Cache the result
        self.schema_cache[cache_key] = schema_info
        return schema_info
    
    def _parse_connection_string(self, connection_string: str) -> Optional[Dict[str, str]]:
        """
        Parse SQL Server connection string into components.
        """
        if not connection_string:
            return None
            
        parts = {}
        for part in connection_string.split(";"):
            if "=" in part:
                key, value = part.split("=", 1)
                parts[key.strip().lower()] = value.strip()
        
        return parts if parts else None

    def _parse_generic_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        column_lineage: Dict[str, Any],
    ):
        """
        Parse generic SSIS components that don't have specialized parsers.
        This ensures we still capture column lineage for all component types.
        """
        component_name = component_xml.get("name", "")
        class_id = component_xml.get("componentClassID", "")

        logger.debug(f"Parsing generic component: {component_name} (type: {class_id})")

        # Log column lineage information for debugging
        if column_lineage["column_mappings"]:
            logger.debug(
                f"Generic component {component_name} has {len(column_lineage['column_mappings'])} column mappings"
            )
