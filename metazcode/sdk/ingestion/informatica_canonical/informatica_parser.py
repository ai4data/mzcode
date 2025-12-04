"""
Canonical Informatica Parser

This module provides a robust Informatica parser built with lxml to create a canonical representation
of Informatica workflows and mappings. It parses Informatica XML export files following the same
architectural patterns as the SSIS parser.
"""

import os
import re
from lxml import etree
from typing import Dict, List, Tuple, Generator, Any, Optional
import logging
import json

from ...models.canonical_types import NodeType, EdgeType
from ...models.graph import Node, Edge
from ...models.traceability import SourceContext
from .type_mapping import InformaticaDataTypeMapper, TargetPlatform
from ..ssis_canonical.sql_semantics import EnhancedSqlParser, SqlSemantics, create_join_edges_from_semantics
from ..informatica.parsers.column_lineage_builder import ColumnLineageBuilder, JoinerLineageBuilder, LookupLineageBuilder
from ..informatica.parsers.error_handling_builder import ErrorHandlingBuilder

logger = logging.getLogger(__name__)


class CanonicalInformaticaParser:
    """
    A robust Informatica parser built with lxml to create a canonical representation of the project.
    It parses Informatica workflow and mapping XML files exported from PowerCenter.
    """

    def __init__(
        self,
        connections_context: Optional[Dict[str, Dict[str, Any]]] = None,
        parameters_context: Optional[Dict[str, Dict[str, Any]]] = None,
        parameter_file_path: Optional[str] = None,
        enable_schema_introspection: bool = True,
        enable_type_mapping: bool = True,
        target_platforms: Optional[List[str]] = None,
    ):
        """
        Initialize the Informatica parser.
        
        Args:
            connections_context: Pre-loaded connection information
            parameters_context: Pre-loaded parameter information  
            parameter_file_path: Path to Informatica parameter file (.par) for dynamic resolution
            enable_schema_introspection: Whether to enable database schema introspection
            enable_type_mapping: Whether to enable data type mapping
            target_platforms: List of target platforms for type mapping
        """
        self.connections_context = connections_context or {}
        self.parameters_context = parameters_context or {}
        
        # REQUIREMENT 2.2: Initialize variables context for workflow/mapping variables
        self.variables_context = {}
        
        # REQUIREMENT 3.2: Load and parse parameter file if provided
        self.parameter_file_context = {}
        if parameter_file_path:
            self.parameter_file_context = self._parse_parameter_file(parameter_file_path)
        self.enable_schema_introspection = enable_schema_introspection
        self.schema_cache = {}  # Cache for database schema information
        
        # Initialize type mapping engine
        self.enable_type_mapping = enable_type_mapping
        self.type_mapper = InformaticaDataTypeMapper() if enable_type_mapping else None
        self.target_platforms = self._parse_target_platforms(target_platforms or ["sql_server", "postgresql"])
        
        # Initialize enhanced SQL parser for migration support
        self.sql_parser = EnhancedSqlParser()
        self._pending_sql_semantics = None
        
        # Session context for mapping session connections
        self.session_connections = {}

        # Cache for parsed mapping files to avoid re-parsing
        self.mapping_cache = {}

        # Connection ID map for creating USES_CONNECTION edges
        self.connection_id_map = {}

        # Initialize enhancement builders (Enhancements #1-5)
        self.column_lineage_builder = ColumnLineageBuilder()
        self.joiner_lineage_builder = JoinerLineageBuilder()
        self.lookup_lineage_builder = LookupLineageBuilder()
        self.error_handling_builder = ErrorHandlingBuilder()

    def _parse_target_platforms(self, platforms: List[str]) -> List[TargetPlatform]:
        """Parse string platform names to TargetPlatform enums."""
        parsed = []
        for platform in platforms:
            try:
                parsed.append(TargetPlatform(platform))
            except ValueError:
                logger.warning(f"Unknown target platform: {platform}")
        return parsed

    def _add_enhancements_to_transformation(
        self,
        properties: Dict[str, Any],
        transformation_element: Optional[etree._Element],
        transformation_type: str,
        instance_name: str,
    ) -> Dict[str, Any]:
        """
        Add all enhancements (column lineage, error handling) to transformation properties.

        This is a helper method that can be called from any transformation parser to add
        the new enhancement features without duplicating code.

        Args:
            properties: Existing properties dictionary
            transformation_element: XML element with transformation definition
            transformation_type: Type of transformation (Expression, Joiner, etc.)
            instance_name: Instance name

        Returns:
            Enhanced properties dictionary
        """
        if transformation_element is None:
            return properties

        # Enhancement #1 & #4: Add column lineage with friendly expressions
        trans_type_lower = transformation_type.lower()

        if "joiner" in trans_type_lower:
            lineage_builder = self.joiner_lineage_builder
        elif "lookup" in trans_type_lower:
            lineage_builder = self.lookup_lineage_builder
        else:
            lineage_builder = self.column_lineage_builder

        column_lineage = lineage_builder.build_lineage(
            transformation_element=transformation_element,
            transformation_type=transformation_type,
            instance_name=instance_name,
        )
        if column_lineage:
            properties["column_lineage"] = column_lineage

        # Enhancement #5: Add error handling metadata
        error_handling = self.error_handling_builder.build_error_handling_metadata(
            transformation_element=transformation_element,
            transformation_type=transformation_type,
            instance_name=instance_name,
            properties=properties,
        )
        if error_handling:
            properties["error_handling"] = error_handling

        return properties


    def _create_uses_connection_edge(
        self,
        operation_id: str,
        properties: Dict[str, Any],
        edges: List[Edge]
    ) -> None:
        """
        Helper method to create USES_CONNECTION edge if operation uses a connection.

        Args:
            operation_id: ID of the operation node
            properties: Operation properties dictionary
            edges: List to append the edge to
        """
        # Check for connection_name in properties (could be from session context or direct)
        connection_name = properties.get("connection_name", "") or properties.get("session_connection", "")

        if connection_name and connection_name in self.connection_id_map:
            uses_conn_edge = Edge(
                source_id=operation_id,
                target_id=self.connection_id_map[connection_name],
                relation=EdgeType.USES_CONNECTION,
                properties={
                    "connection_name": connection_name,
                    "derivation_method": "session_context",
                    "confidence_level": "high"
                }
            )
            edges.append(uses_conn_edge)
            logger.debug(f"Created USES_CONNECTION edge: {operation_id} -> connection:{connection_name}")

    def _categorize_operation_subtype(self, transformation_type: str) -> str:
        """
        Categorizes Informatica operations into standardized subtypes based on their type.

        Args:
            transformation_type: The Informatica transformation type

        Returns:
            A standardized operation subtype: CONTROL_FLOW, DATA_FLOW, EXECUTE, or SCRIPT
        """
        # Data Flow transformations
        data_flow_types = {
            "Source Qualifier", "Target Definition", "Expression", "Filter", 
            "Aggregator", "Sorter", "Joiner", "Lookup", "Router", "Union",
            "Sequence Generator", "Update Strategy", "Normalizer", "Rank",
            "Transaction Control", "Stored Procedure"
        }
        
        if transformation_type in data_flow_types:
            return "DATA_FLOW"
        
        # Control flow operations (workflow level)
        elif transformation_type in ["Session", "Worklet", "Assignment", "Command", "Timer", "Event-Wait", "Start", "Decision"]:
            return "CONTROL_FLOW"
        
        # Execute operations
        elif transformation_type in ["Command", "Email"]:
            return "EXECUTE"
        
        # Custom and other transformations
        elif transformation_type in ["Custom Transformation"]:
            return "DATA_FLOW"
            
        # Default fallback
        else:
            logger.warning(f"Unknown transformation type '{transformation_type}', defaulting to 'DATA_FLOW'")
            return "DATA_FLOW"

    def parse(
        self, 
        workflow_file_path: str,
        mapping_file_path: Optional[str] = None
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Parses Informatica workflow and mapping files and yields the discovered nodes and edges.
        
        Args:
            workflow_file_path: Path to the workflow XML file
            mapping_file_path: Path to the mapping XML file (if separate)
        """
        try:
            # Parse workflow file
            workflow_root = self._parse_xml_file(workflow_file_path)
            
            # Parse mapping file if provided, otherwise look for mappings in workflow file
            mapping_root = None
            if mapping_file_path and os.path.exists(mapping_file_path):
                mapping_root = self._parse_xml_file(mapping_file_path)
            else:
                # Try to find mapping file based on workflow file name
                inferred_mapping_path = self._infer_mapping_path(workflow_file_path)
                if inferred_mapping_path and os.path.exists(inferred_mapping_path):
                    mapping_root = self._parse_xml_file(inferred_mapping_path)
                    mapping_file_path = inferred_mapping_path
            
            yield from self._parse_informatica_project(
                workflow_root, workflow_file_path, mapping_root, mapping_file_path
            )
            
        except Exception as e:
            logger.error(
                f"ERROR: Could not parse {workflow_file_path}. Reason: {e}", exc_info=True
            )
            return

    def _parse_xml_file(self, file_path: str) -> etree._Element:
        """Parse an Informatica XML file with proper encoding handling."""
        try:
            with open(file_path, "r", encoding="Windows-1252") as f:
                content = f.read()
        except UnicodeDecodeError:
            # Fallback to iso-8859-1 if Windows-1252 fails
            with open(file_path, "r", encoding="iso-8859-1") as f:
                content = f.read()
        
        # Remove BOM if present
        if content.startswith('\ufeff'):
            content = content[1:]
            
        return etree.fromstring(content.encode("utf-8"))

    def _infer_mapping_path(self, workflow_file_path: str) -> Optional[str]:
        """
        Infer the mapping file path from the workflow file path.
        
        Typical patterns:
        - wf_m_q1.XML -> m_q1.XML
        - WorkFlow_ExploreInformatica.XML -> Mapping_ExploreInformatica.XML
        """
        directory = os.path.dirname(workflow_file_path)
        filename = os.path.basename(workflow_file_path)
        
        # Pattern 1: wf_m_*.XML -> m_*.XML
        if filename.startswith("wf_m_"):
            mapping_filename = filename[3:]  # Remove "wf_" prefix
            return os.path.join(directory, mapping_filename)
        
        # Pattern 2: WorkFlow_*.XML -> Mapping_*.XML
        elif filename.startswith("WorkFlow_"):
            mapping_filename = filename.replace("WorkFlow_", "Mapping_")
            return os.path.join(directory, mapping_filename)
        
        return None

    def _parse_informatica_project(
        self,
        workflow_root: etree._Element,
        workflow_file_path: str,
        mapping_root: Optional[etree._Element],
        mapping_file_path: Optional[str]
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Parse the complete Informatica project consisting of workflow and mapping files.
        """
        nodes = []
        edges = []

        # FIRST: Extract and create connection nodes from workflow
        connection_nodes, connection_id_map = self._parse_connections_from_workflow(
            workflow_root, workflow_file_path
        )
        nodes.extend(connection_nodes)

        # Store connection map for later use when creating edges
        self.connection_id_map = connection_id_map

        # Parse workflows to extract session information
        workflow_nodes, workflow_edges = self._parse_workflows(workflow_root, workflow_file_path)
        nodes.extend(workflow_nodes)
        edges.extend(workflow_edges)

        # Parse mappings if available
        if mapping_root is not None:
            mapping_nodes, mapping_edges = self._parse_mappings(mapping_root, mapping_file_path)
            nodes.extend(mapping_nodes)
            edges.extend(mapping_edges)

        yield nodes, edges

    def _parse_workflows(
        self, 
        root: etree._Element, 
        file_path: str
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse workflow definitions from the root element.
        """
        nodes = []
        edges = []
        
        # Find all workflow definitions
        workflows = root.xpath(".//WORKFLOW")
        
        for workflow in workflows:
            workflow_nodes, workflow_edges = self._parse_workflow(workflow, file_path)
            nodes.extend(workflow_nodes)
            edges.extend(workflow_edges)
        
        return nodes, edges

    def _parse_connections_from_workflow(
        self,
        workflow_root: etree._Element,
        file_path: str
    ) -> Tuple[List[Node], Dict[str, str]]:
        """
        Extract and create connection nodes from workflow CONNECTIONREFERENCE elements.

        Args:
            workflow_root: Root element of workflow XML
            file_path: Path to workflow file

        Returns:
            Tuple of (connection_nodes, connection_id_map)
        """
        connection_nodes = []
        connection_id_map = {}
        connection_names = set()

        # Find all CONNECTIONREFERENCE elements in the workflow
        conn_refs = workflow_root.xpath(".//CONNECTIONREFERENCE")

        for conn_ref in conn_refs:
            connection_name = conn_ref.get("CONNECTIONNAME", "")
            connection_type = conn_ref.get("CONNECTIONTYPE", "")
            connection_subtype = conn_ref.get("CONNECTIONSUBTYPE", "")

            if not connection_name or connection_name in connection_names:
                continue

            connection_names.add(connection_name)
            conn_id = f"connection:{connection_name}"

            # Build connection properties
            properties = {
                "technology": "Informatica",
                "connection_type": connection_type,
                "connection_subtype": connection_subtype,
                **SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="xml",
                    xml_path=f"//CONNECTIONREFERENCE[@CONNECTIONNAME='{connection_name}']"
                )
            }

            # Enrich with connection context if available
            if connection_name in self.connections_context:
                conn_data = self.connections_context[connection_name]
                properties.update({
                    "server": conn_data.get("server", ""),
                    "database": conn_data.get("database", ""),
                    "username": conn_data.get("username", ""),
                    "port": conn_data.get("port", ""),
                    "file_path": conn_data.get("file_path", ""),
                })

            # Create connection node
            connection_nodes.append(
                Node(
                    node_id=conn_id,
                    node_type=NodeType.CONNECTION,
                    name=connection_name,
                    properties=properties,
                )
            )

            # Map connection name to node ID
            connection_id_map[connection_name] = conn_id

        logger.info(f"Created {len(connection_nodes)} connection nodes from workflow")

        return connection_nodes, connection_id_map

    def _parse_workflow(
        self, 
        workflow: etree._Element, 
        file_path: str
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse a single workflow definition.
        """
        nodes = []
        edges = []
        
        workflow_name = workflow.get("NAME", "UnknownWorkflow")
        workflow_id = f"workflow:{workflow_name}"
        
        # Create workflow node
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            source_file_type="xml",
            xml_path=f"//WORKFLOW[@NAME='{workflow_name}']",
            line_number=workflow.sourceline
            ,
            technology="Informatica"
        )
        
        workflow_node = Node(
            node_id=workflow_id,
            node_type=NodeType.PIPELINE.value,
            name=workflow_name,
            properties={
                "name": workflow_name,
                "description": workflow.get("DESCRIPTION", ""),
                "is_valid": workflow.get("ISVALID", "YES") == "YES",
                "version_number": workflow.get("VERSIONNUMBER", "1"),
                "source_context": source_context,
                "informatica_type": "workflow"
            }
        )
        nodes.append(workflow_node)
        
        # Parse task instances within the workflow
        task_instances = workflow.xpath(".//TASKINSTANCE")
        for task_instance in task_instances:
            task_nodes, task_edges = self._parse_task_instance(
                task_instance, workflow_id, file_path
            )
            nodes.extend(task_nodes)
            edges.extend(task_edges)
        
        # REQUIREMENT 2.2: Parse workflow variables
        workflow_variable_nodes, workflow_variable_edges = self._parse_variables(
            workflow, workflow_id, file_path, "workflow"
        )
        nodes.extend(workflow_variable_nodes)
        edges.extend(workflow_variable_edges)
        
        # Parse workflow links (execution order)
        workflow_links = workflow.xpath(".//WORKFLOWLINK")
        for link in workflow_links:
            link_edges = self._parse_workflow_link(link, workflow_id, file_path)
            edges.extend(link_edges)
        
        # Parse sessions for connection information AND create EXECUTES edges to mappings
        sessions = workflow.xpath(".//SESSION")
        for session in sessions:
            # Extract connection information for later use
            self._extract_session_connections(session, file_path)
            
            # Create EXECUTES edges from session tasks to mappings
            session_name = session.get("NAME", "")
            mapping_name = session.get("MAPPINGNAME", "")
            
            if session_name and mapping_name:
                # Find the corresponding task instance for this session
                session_task_id = f"{workflow_id}:task:{session_name}"
                mapping_pipeline_id = f"mapping:{mapping_name}"
                
                # Create EXECUTES edge from session task to mapping
                executes_edge = Edge(
                    source_id=session_task_id,
                    target_id=mapping_pipeline_id,
                    relation=EdgeType.DEPENDS_ON.value,  # Using DEPENDS_ON as closest semantic
                    properties={
                        "relationship": "session_executes_mapping",
                        "mapping_name": mapping_name,
                        "session_name": session_name,
                        "derivation_method": "xml_metadata",
                        "confidence_level": "high",
                        "source_context": SourceContext.create_node_traceability(
                            source_file_path=file_path,
                            source_file_type="xml", 
                            xml_path=f"//SESSION[@NAME='{session_name}'][@MAPPINGNAME='{mapping_name}']",
                            technology="Informatica"
                        )
                    }
                )
                edges.append(executes_edge)
                logger.info(f"Created EXECUTES edge: {session_task_id} -> {mapping_pipeline_id}")
        
        return nodes, edges

    def _parse_task_instance(
        self, 
        task_instance: etree._Element, 
        workflow_id: str, 
        file_path: str
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse a task instance within a workflow.
        """
        nodes = []
        edges = []
        
        task_name = task_instance.get("NAME", "UnknownTask")
        # Use TASKTYPE attribute for workflow task instances
        task_type = task_instance.get("TASKTYPE") or task_instance.get("TYPE", "UnknownType")
        task_id = f"{workflow_id}:task:{task_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            source_file_type="xml",
            xml_path=f"//TASKINSTANCE[@NAME='{task_name}']",
            line_number=task_instance.sourceline
            ,
            technology="Informatica"
        )
        
        # Determine node type based on task type
        node_type = NodeType.OPERATION
        operation_subtype = self._categorize_operation_subtype(task_type)
        
        # Prepare base properties
        task_properties = {
            "name": task_name,
            "task_type": task_type,
            "operation_subtype": operation_subtype,
            "description": task_instance.get("DESCRIPTION", ""),
            "is_reusable": task_instance.get("REUSABLE", "NO") == "YES",
            "source_context": source_context,
            "informatica_type": "task_instance"
        }
        
        # REQUIREMENT 2.4: Add performance attributes to Session task nodes
        if task_type == "Session":
            session_context = self.session_connections.get(task_name, {})
            performance_attributes = session_context.get("performance_attributes", {})
            
            if performance_attributes:
                # Add performance attributes to Session node properties
                task_properties["performance_attributes"] = performance_attributes
                task_properties["has_performance_tuning"] = True
                
                # Categorize performance attributes for quick reference
                perf_categories = {}
                for attr_name, attr_info in performance_attributes.items():
                    category = attr_info.get("category", "general_performance")
                    if category not in perf_categories:
                        perf_categories[category] = []
                    perf_categories[category].append({
                        "attribute": attr_name,
                        "value": attr_info.get("value", ""),
                        "affects_performance": attr_info.get("affects_performance", False)
                    })
                
                task_properties["performance_categories"] = perf_categories
                task_properties["performance_baseline_available"] = len(performance_attributes) > 0
                
                logger.debug(f"Added {len(performance_attributes)} performance attributes to Session {task_name}")
            else:
                task_properties["has_performance_tuning"] = False
                task_properties["performance_baseline_available"] = False
        
        task_node = Node(
            node_id=task_id,
            node_type=node_type.value,
            name=task_name,
            properties=task_properties
        )
        nodes.append(task_node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=workflow_id,
            target_id=task_id,
            relation=EdgeType.CONTAINS.value,
            properties={
                "source_context": source_context
            }
        )
        edges.append(containment_edge)
        
        # REQUIREMENT 3.3: Recursive parsing for worklets
        if task_type == "Worklet":
            worklet_nodes, worklet_edges = self._parse_worklet_contents(
                task_instance, task_id, file_path
            )
            nodes.extend(worklet_nodes)
            edges.extend(worklet_edges)
        
        return nodes, edges

    def _parse_worklet_contents(
        self,
        worklet_task: etree._Element,
        parent_task_id: str,
        file_path: str
    ) -> Tuple[List[Node], List[Edge]]:
        """
        REQUIREMENT 3.3: Recursively parse the contents of a worklet.
        
        A worklet is a reusable sub-workflow that contains its own set of tasks, sessions, and logic.
        This method expands the worklet to show all internal components.
        
        Args:
            worklet_task: The worklet task instance XML element
            parent_task_id: ID of the parent worklet task
            file_path: Path to the XML file containing the worklet
            
        Returns:
            Tuple of nodes and edges representing the worklet's internal structure
        """
        nodes = []
        edges = []
        
        worklet_name = worklet_task.get("NAME", "")
        worklet_id = f"{parent_task_id}:worklet"
        
        logger.info(f"Recursively parsing worklet: {worklet_name}")
        
        # Find the corresponding WORKLET definition in the XML
        # Worklets can be defined in the same file or referenced externally
        root = worklet_task.getroottree().getroot()
        worklet_definitions = root.xpath(f".//WORKLET[@NAME='{worklet_name}']")
        
        if not worklet_definitions:
            # Try alternative xpath patterns
            worklet_definitions = root.xpath(f".//WORKLET") 
            worklet_definitions = [w for w in worklet_definitions if w.get("NAME") == worklet_name]
        
        if not worklet_definitions:
            logger.warning(f"Could not find WORKLET definition for: {worklet_name}")
            return nodes, edges
        
        worklet_def = worklet_definitions[0]
        logger.debug(f"Found worklet definition for: {worklet_name}")
        
        # Create a sub-pipeline node for the worklet contents
        worklet_pipeline_id = f"{parent_task_id}:worklet_pipeline:{worklet_name}"
        worklet_pipeline_node = Node(
            node_id=worklet_pipeline_id,
            node_type=NodeType.PIPELINE.value,
            name=f"Worklet_{worklet_name}",
            properties={
                "name": f"Worklet_{worklet_name}",
                "worklet_name": worklet_name,
                "parent_task": parent_task_id,
                "is_worklet_pipeline": True,
                "source_context": SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="xml",
                    xml_path=f"//WORKLET[@NAME='{worklet_name}']",
                    technology="Informatica"
                )
            }
        )
        nodes.append(worklet_pipeline_node)
        
        # Create containment edge from parent worklet task to worklet pipeline
        worklet_containment_edge = Edge(
            source_id=parent_task_id,
            target_id=worklet_pipeline_id,
            relation=EdgeType.CONTAINS.value,
            properties={
                "relationship": "worklet_contains_pipeline",
                "worklet_name": worklet_name
            }
        )
        edges.append(worklet_containment_edge)
        
        # REQUIREMENT 3.3: Recursively parse task instances within the worklet
        task_instances = worklet_def.xpath(".//TASKINSTANCE")
        for task_instance in task_instances:
            nested_task_name = task_instance.get("NAME", "")
            nested_task_type = task_instance.get("TASKTYPE", "")
            
            if not nested_task_name:
                continue
            
            # Create hierarchical task ID to reflect worklet nesting
            nested_task_id = f"{worklet_pipeline_id}:task:{nested_task_name}"
            
            logger.debug(f"Parsing nested task in worklet {worklet_name}: {nested_task_name} ({nested_task_type})")
            
            # Create the nested task node
            nested_source_context = SourceContext.create_node_traceability(
                source_file_path=file_path,
                source_file_type="xml",
                xml_path=f"//WORKLET[@NAME='{worklet_name}']//TASKINSTANCE[@NAME='{nested_task_name}']",
                line_number=task_instance.sourceline,
                technology="Informatica"
            )
            
            nested_task_node = Node(
                node_id=nested_task_id,
                node_type=NodeType.OPERATION.value,
                name=nested_task_name,
                properties={
                    "name": nested_task_name,
                    "task_type": nested_task_type,
                    "operation_subtype": self._categorize_operation_subtype(nested_task_type),
                    "description": task_instance.get("DESCRIPTION", ""),
                    "parent_worklet": worklet_name,
                    "parent_worklet_task": parent_task_id,
                    "worklet_context": True,
                    "source_context": nested_source_context,
                    "informatica_type": "worklet_task_instance"
                }
            )
            nodes.append(nested_task_node)
            
            # Create containment edge from worklet pipeline to nested task
            nested_containment_edge = Edge(
                source_id=worklet_pipeline_id,
                target_id=nested_task_id,
                relation=EdgeType.CONTAINS.value,
                properties={
                    "source_context": nested_source_context,
                    "worklet_context": True
                }
            )
            edges.append(nested_containment_edge)
            
            # Handle nested worklets (recursive case)
            if nested_task_type == "Worklet":
                nested_worklet_nodes, nested_worklet_edges = self._parse_worklet_contents(
                    task_instance, nested_task_id, file_path
                )
                nodes.extend(nested_worklet_nodes)
                edges.extend(nested_worklet_edges)
                logger.debug(f"Recursively parsed nested worklet: {nested_task_name}")
        
        # Parse workflow links within the worklet
        worklet_links = worklet_def.xpath(".//WORKFLOWLINK")
        for link in worklet_links:
            from_task = link.get("FROMTASK", "")
            to_task = link.get("TOTASK", "")
            
            if from_task and to_task:
                from_task_id = f"{worklet_pipeline_id}:task:{from_task}"
                to_task_id = f"{worklet_pipeline_id}:task:{to_task}"
                
                worklet_link_edge = Edge(
                    source_id=from_task_id,
                    target_id=to_task_id,
                    relation=EdgeType.DEPENDS_ON.value,
                    properties={
                        "link_type": "worklet_execution_order",
                        "condition": link.get("CONDITION", ""),
                        "worklet_context": True,
                        "parent_worklet": worklet_name
                    }
                )
                edges.append(worklet_link_edge)
                logger.debug(f"Created worklet link: {from_task} -> {to_task} in {worklet_name}")
        
        logger.info(f"Completed recursive parsing of worklet {worklet_name}: "
                   f"{len([n for n in nodes if n.node_type == NodeType.OPERATION.value])} tasks, "
                   f"{len([e for e in edges if e.relation == EdgeType.DEPENDS_ON.value])} links")
        
        return nodes, edges

    def _parse_workflow_link(
        self, 
        link: etree._Element, 
        workflow_id: str, 
        file_path: str
    ) -> List[Edge]:
        """
        Parse workflow links that define execution order between tasks.
        """
        edges = []
        
        from_task = link.get("FROMTASK", "")
        to_task = link.get("TOTASK", "")
        
        if from_task and to_task:
            from_id = f"{workflow_id}:task:{from_task}"
            to_id = f"{workflow_id}:task:{to_task}"
            
            source_context = SourceContext.create_node_traceability(
                source_file_path=file_path,
                source_file_type="xml",
                xml_path=f"//WORKFLOWLINK[@FROMTASK='{from_task}'][@TOTASK='{to_task}']",
                line_number=link.sourceline
                ,
            technology="Informatica"
        )
            
            link_edge = Edge(
                source_id=from_id,
                target_id=to_id,
                relation=EdgeType.DEPENDS_ON.value,
                properties={
                    "link_type": "workflow_execution_order",
                    "condition": link.get("CONDITION", ""),
                    "source_context": source_context
                }
            )
            edges.append(link_edge)
        
        return edges

    def _extract_session_connections(self, session: etree._Element, file_path: str):
        """
        REQUIREMENT 3.1: Extract comprehensive session-level overrides for connections, SQL, and transformations.
        This method now captures all session-level configuration that overrides static mapping definitions.
        """
        session_name = session.get("NAME", "")
        mapping_name = session.get("MAPPINGNAME", "")
        
        session_context = {
            "session_name": session_name,
            "connections": {},
            "sql_overrides": {},
            "lookup_overrides": {},
            "transformation_overrides": {},
            "general_attributes": {},
            # REQUIREMENT 2.4: Performance tuning attributes
            "performance_attributes": {}
        }
        
        # Extract connection references from session extensions
        session_extensions = session.xpath(".//SESSIONEXTENSION")
        for ext in session_extensions:
            # Get SINSTANCENAME from the SESSIONEXTENSION element (not from CONNECTIONREFERENCE)
            instance_name = ext.get("SINSTANCENAME", "")

            conn_refs = ext.xpath(".//CONNECTIONREFERENCE")
            for conn_ref in conn_refs:
                connection_name = conn_ref.get("CONNECTIONNAME", "")
                if instance_name and connection_name:
                    session_context["connections"][instance_name] = connection_name
                    logger.debug(f"Session {session_name}: Connection mapping {instance_name} -> {connection_name}")
        
        # REQUIREMENT 3.1: Extract comprehensive session-level attribute overrides
        attributes = session.xpath(".//ATTRIBUTE")
        for attr in attributes:
            attr_name = attr.get("NAME", "")
            attr_value = attr.get("VALUE", "")
            transformation_name = attr.get("TRANSFORMATIONNAME", "")
            
            if not attr_name:
                continue
                
            # SQL Query overrides (most common override)
            if attr_name == "Sql Query" and transformation_name:
                if transformation_name not in session_context["sql_overrides"]:
                    session_context["sql_overrides"][transformation_name] = {}
                session_context["sql_overrides"][transformation_name]["sql_query"] = attr_value
                logger.debug(f"Session {session_name}: SQL override for {transformation_name}")
            
            # Lookup SQL overrides
            elif attr_name == "Lookup Sql Override" and transformation_name:
                if transformation_name not in session_context["lookup_overrides"]:
                    session_context["lookup_overrides"][transformation_name] = {}
                session_context["lookup_overrides"][transformation_name]["lookup_sql"] = attr_value
                logger.debug(f"Session {session_name}: Lookup SQL override for {transformation_name}")
            
            # Connection Name overrides (for transformations that can switch connections)
            elif attr_name == "Connection Name" and transformation_name:
                if transformation_name not in session_context["transformation_overrides"]:
                    session_context["transformation_overrides"][transformation_name] = {}
                session_context["transformation_overrides"][transformation_name]["connection_name"] = attr_value
                logger.debug(f"Session {session_name}: Connection override for {transformation_name} -> {attr_value}")
            
            # Source/Target file overrides
            elif attr_name in ["Source filename", "Target filename", "Output filename"] and transformation_name:
                if transformation_name not in session_context["transformation_overrides"]:
                    session_context["transformation_overrides"][transformation_name] = {}
                session_context["transformation_overrides"][transformation_name][attr_name.lower().replace(" ", "_")] = attr_value
                logger.debug(f"Session {session_name}: File override for {transformation_name}: {attr_name} -> {attr_value}")
            
            # Other transformation-specific overrides
            elif transformation_name and attr_value:
                if transformation_name not in session_context["transformation_overrides"]:
                    session_context["transformation_overrides"][transformation_name] = {}
                session_context["transformation_overrides"][transformation_name][attr_name] = attr_value
            
            # General session attributes (not transformation-specific)
            elif not transformation_name and attr_value:
                session_context["general_attributes"][attr_name] = attr_value
                
                # REQUIREMENT 2.4: Identify and categorize performance tuning attributes
                if self._is_performance_attribute(attr_name):
                    session_context["performance_attributes"][attr_name] = {
                        "value": attr_value,
                        "category": self._categorize_performance_attribute(attr_name),
                        "affects_performance": True
                    }
                    logger.debug(f"Session {session_name}: Performance attribute {attr_name} = {attr_value}")
        
        # Store comprehensive session context mapping
        if mapping_name:
            self.session_connections[mapping_name] = session_context
            logger.info(f"Extracted session context for {mapping_name}: "
                       f"{len(session_context['connections'])} connections, "
                       f"{len(session_context['sql_overrides'])} SQL overrides, "
                       f"{len(session_context['lookup_overrides'])} lookup overrides, "
                       f"{len(session_context['transformation_overrides'])} transformation overrides, "
                       f"{len(session_context['performance_attributes'])} performance attributes")

    def _apply_session_overrides(
        self,
        transformation_name: str,
        instance_name: str,
        base_properties: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        REQUIREMENT 3.1: Apply session-level overrides to transformation properties.
        This ensures the graph reflects actual runtime behavior, not just static mapping definitions.
        
        Args:
            transformation_name: Name of the transformation (e.g., "SQ_CUSTOMERS")
            instance_name: Name of the instance for connection lookups
            base_properties: Properties extracted from the mapping definition
            session_context: Session context with overrides
            
        Returns:
            Enhanced properties with session overrides applied
        """
        enhanced_properties = base_properties.copy()
        
        # Apply SQL query overrides (most critical)
        if transformation_name in session_context.get("sql_overrides", {}):
            sql_override = session_context["sql_overrides"][transformation_name].get("sql_query", "")
            if sql_override:
                # REQUIREMENT 3.2: Resolve parameters in session SQL override
                workflow_name = session_context.get("workflow_name", "")
                session_name = session_context.get("session_name", "")
                resolution_result = self._resolve_parameter_value(sql_override, workflow_name, session_name)
                
                # Store both original and override values for complete traceability
                enhanced_properties["mapping_sql_query"] = base_properties.get("sql_query", "")
                enhanced_properties["session_sql_query"] = sql_override  # Raw override
                enhanced_properties["resolved_sql_query"] = resolution_result["resolved_expression"]
                enhanced_properties["sql_query"] = resolution_result["resolved_expression"]  # Resolved as effective value
                enhanced_properties["sql_override_applied"] = True
                enhanced_properties["sql_parameter_resolution"] = resolution_result
                logger.debug(f"Applied SQL override for {transformation_name} with parameter resolution")
        
        # Apply lookup SQL overrides
        if transformation_name in session_context.get("lookup_overrides", {}):
            lookup_override = session_context["lookup_overrides"][transformation_name].get("lookup_sql", "")
            if lookup_override:
                # REQUIREMENT 3.2: Resolve parameters in lookup SQL override
                workflow_name = session_context.get("workflow_name", "")
                session_name = session_context.get("session_name", "")
                resolution_result = self._resolve_parameter_value(lookup_override, workflow_name, session_name)
                
                enhanced_properties["mapping_lookup_sql"] = base_properties.get("lookup_sql", "")
                enhanced_properties["session_lookup_sql"] = lookup_override  # Raw override
                enhanced_properties["resolved_lookup_sql"] = resolution_result["resolved_expression"]
                enhanced_properties["lookup_sql"] = resolution_result["resolved_expression"]  # Resolved as effective value
                enhanced_properties["lookup_override_applied"] = True
                enhanced_properties["lookup_parameter_resolution"] = resolution_result
                logger.debug(f"Applied lookup SQL override for {transformation_name} with parameter resolution")
        
        # Apply connection overrides
        session_connection = session_context.get("connections", {}).get(instance_name, "")
        if session_connection:
            enhanced_properties["mapping_connection"] = base_properties.get("connection_name", "")
            enhanced_properties["session_connection"] = session_connection
            enhanced_properties["connection_name"] = session_connection
            enhanced_properties["connection_override_applied"] = True
        
        # Apply other transformation-specific overrides with parameter resolution
        if transformation_name in session_context.get("transformation_overrides", {}):
            overrides = session_context["transformation_overrides"][transformation_name]
            workflow_name = session_context.get("workflow_name", "")
            session_name = session_context.get("session_name", "")
            
            for attr_name, attr_value in overrides.items():
                # REQUIREMENT 3.2: Resolve parameters in override values
                resolution_result = self._resolve_parameter_value(attr_value, workflow_name, session_name)
                
                # Store original value if it exists
                if attr_name in base_properties:
                    enhanced_properties[f"mapping_{attr_name}"] = base_properties[attr_name]
                enhanced_properties[f"session_{attr_name}"] = attr_value  # Raw override
                enhanced_properties[f"resolved_{attr_name}"] = resolution_result["resolved_expression"]
                enhanced_properties[attr_name] = resolution_result["resolved_expression"]  # Resolved as effective value
                enhanced_properties[f"{attr_name}_override_applied"] = True
                
                # Store parameter resolution info if parameters were found
                if resolution_result["has_parameters"]:
                    enhanced_properties[f"{attr_name}_parameter_resolution"] = resolution_result
        
        # Add session metadata for traceability
        enhanced_properties["session_name"] = session_context.get("session_name", "")
        enhanced_properties["has_session_overrides"] = any([
            enhanced_properties.get("sql_override_applied", False),
            enhanced_properties.get("lookup_override_applied", False),
            enhanced_properties.get("connection_override_applied", False),
            len(session_context.get("transformation_overrides", {}).get(transformation_name, {})) > 0
        ])
        
        return enhanced_properties

    def _parse_parameter_file(self, parameter_file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        REQUIREMENT 3.2: Parse Informatica parameter file (.par) format.
        
        Parameter file format example:
        [Folder.WF:workflow_name.ST:session_name]
        $$param1=value1
        $$param2=value2
        
        [Global]
        $$GlobalParam=GlobalValue
        
        Args:
            parameter_file_path: Path to the .par file
            
        Returns:
            Dictionary with parameter context organized by section
        """
        parameter_context = {}
        
        try:
            with open(parameter_file_path, 'r', encoding='utf-8') as f:
                current_section = "Global"  # Default section
                parameter_context[current_section] = {}
                
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('#') or line.startswith(';'):
                        continue
                    
                    # Check for section headers [Folder.WF:workflow.ST:session]
                    if line.startswith('[') and line.endswith(']'):
                        current_section = line[1:-1]  # Remove brackets
                        parameter_context[current_section] = {}
                        logger.debug(f"Found parameter section: {current_section}")
                        continue
                    
                    # Parse parameter assignments $$param=value
                    if '=' in line and line.startswith('$$'):
                        param_name, param_value = line.split('=', 1)
                        param_name = param_name.strip()
                        param_value = param_value.strip()
                        
                        # Remove quotes if present
                        if param_value.startswith('"') and param_value.endswith('"'):
                            param_value = param_value[1:-1]
                        elif param_value.startswith("'") and param_value.endswith("'"):
                            param_value = param_value[1:-1]
                        
                        parameter_context[current_section][param_name] = {
                            "value": param_value,
                            "line_number": line_num,
                            "section": current_section
                        }
                        
                        logger.debug(f"Parsed parameter: {param_name} = {param_value} (section: {current_section})")
            
            logger.info(f"Parsed parameter file: {len(parameter_context)} sections, "
                       f"{sum(len(params) for params in parameter_context.values())} total parameters")
            
        except Exception as e:
            logger.error(f"Failed to parse parameter file {parameter_file_path}: {e}")
            parameter_context = {}
        
        return parameter_context

    def _resolve_parameter_value(
        self, 
        expression: str, 
        workflow_name: str = "", 
        session_name: str = ""
    ) -> Dict[str, Any]:
        """
        REQUIREMENT 3.2: Resolve parameter references in expressions.
        
        Args:
            expression: Expression that may contain parameter references ($$param)
            workflow_name: Name of current workflow for context-specific resolution
            session_name: Name of current session for context-specific resolution
            
        Returns:
            Dictionary with resolved information
        """
        if not expression or not isinstance(expression, str):
            return {
                "original_expression": expression,
                "resolved_expression": expression,
                "has_parameters": False,
                "parameters_found": []
            }
        
        import re
        
        result = {
            "original_expression": expression,
            "resolved_expression": expression,
            "has_parameters": False,
            "parameters_found": [],
            "resolution_context": []
        }
        
        # Find all parameter references $$ParameterName
        parameter_pattern = r'\$\$([A-Za-z_][A-Za-z0-9_]*)'
        parameters = re.findall(parameter_pattern, expression)
        
        if not parameters:
            return result
        
        result["has_parameters"] = True
        result["parameters_found"] = parameters
        
        # Try to resolve each parameter
        for param_name in parameters:
            param_ref = f"$${param_name}"
            resolved_value = None
            resolution_source = None
            
            # REQUIREMENT 2.2: Enhanced resolution priority to include workflow/mapping variables:
            # 1. Workflow/mapping variables (stored during parsing)
            # 2. Session-specific: [Folder.WF:workflow.ST:session]
            # 3. Workflow-specific: [Folder.WF:workflow]  
            # 4. Global: [Global]
            # 5. Default parameters_context
            
            # First, try to resolve as workflow/mapping variable
            variable_resolved = False
            if hasattr(self, 'variables_context') and param_name in self.variables_context:
                var_info = self.variables_context[param_name]
                resolved_value = var_info.get("default_value", param_ref)
                resolution_source = f"{var_info.get('scope', 'unknown')}_variable"
                variable_resolved = True
                logger.debug(f"Resolved variable {param_ref} = {resolved_value} (source: {resolution_source})")
            
            # If not resolved as variable, try parameter resolution
            if not variable_resolved:
                resolution_contexts = []
                if workflow_name and session_name:
                    resolution_contexts.append(f"Folder.WF:{workflow_name}.ST:{session_name}")
                if workflow_name:
                    resolution_contexts.append(f"Folder.WF:{workflow_name}")
                resolution_contexts.extend(["Global", ""])
                
                for context in resolution_contexts:
                    context_params = self.parameter_file_context.get(context, {})
                    if param_ref in context_params:
                        resolved_value = context_params[param_ref]["value"]
                        resolution_source = context or "Global"
                        break
                
                # Fallback to legacy parameters_context
                if resolved_value is None and param_name in self.parameters_context:
                    param_info = self.parameters_context[param_name]
                    resolved_value = param_info.get("value", param_ref)
                    resolution_source = "parameters_context"
            
            # Apply resolution
            if resolved_value is not None:
                result["resolved_expression"] = result["resolved_expression"].replace(
                    param_ref, str(resolved_value)
                )
                result["resolution_context"].append({
                    "parameter": param_name,
                    "original_ref": param_ref,
                    "resolved_value": resolved_value,
                    "source": resolution_source
                })
                logger.debug(f"Resolved parameter {param_ref} = {resolved_value} (source: {resolution_source})")
            else:
                logger.warning(f"Could not resolve parameter: {param_ref}")
                result["resolution_context"].append({
                    "parameter": param_name,
                    "original_ref": param_ref,
                    "resolved_value": None,
                    "source": "unresolved"
                })
        
        return result

    def _parse_update_strategy_expression(self, expression: str) -> Dict[str, Any]:
        """
        REQUIREMENT 3.4: Parse Update Strategy expression to understand DML operations.
        
        Args:
            expression: Update Strategy expression (e.g., "IIF(ISNULL(CUST_ID), DD_INSERT, DD_UPDATE)")
            
        Returns:
            Dictionary with parsed update strategy logic
        """
        if not expression:
            return {
                "operations": [],
                "has_conditions": False,
                "complexity": "simple",
                "expression_analysis": "empty"
            }
        
        import re
        
        result = {
            "original_expression": expression,
            "operations": [],
            "has_conditions": False,
            "complexity": "simple",
            "conditions": [],
            "expression_analysis": "parsed"
        }
        
        # Find all DD_ operations (Informatica Update Strategy flags)
        dd_operations = re.findall(r'DD_(INSERT|UPDATE|DELETE|REJECT)', expression, re.IGNORECASE)
        result["operations"] = list(set(op.lower() for op in dd_operations))
        
        # Determine if there are conditional expressions
        if any(func in expression.upper() for func in ["IIF(", "DECODE(", "CASE ", "WHEN "]):
            result["has_conditions"] = True
            result["complexity"] = "conditional"
        
        # Extract condition logic patterns
        iif_patterns = re.findall(r'IIF\s*\((.*?),\s*(DD_\w+),\s*(DD_\w+)\)', expression, re.IGNORECASE)
        for condition, true_op, false_op in iif_patterns:
            result["conditions"].append({
                "condition": condition.strip(),
                "true_operation": true_op.upper(),
                "false_operation": false_op.upper(),
                "pattern_type": "IIF"
            })
        
        # Determine complexity based on analysis
        if len(result["operations"]) > 2:
            result["complexity"] = "complex"
        elif result["has_conditions"] and len(result["operations"]) > 1:
            result["complexity"] = "conditional"
        
        logger.debug(f"Parsed Update Strategy expression: {len(result['operations'])} operations, "
                    f"conditional={result['has_conditions']}, complexity={result['complexity']}")
        
        return result

    def _parse_mappings(
        self, 
        root: etree._Element, 
        file_path: str
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse mapping definitions from the root element.
        """
        nodes = []
        edges = []
        
        # REQUIREMENT 2.1: Store root element for access by mapplet parser
        self._current_root = root
        
        # Parse sources, targets, and transformations first (definitions)
        source_definitions = self._parse_source_definitions(root, file_path)
        target_definitions = self._parse_target_definitions(root, file_path)
        transformation_definitions = self._parse_transformation_definitions(root, file_path)
        
        # Add source and target DATA_ASSET nodes to the main nodes list
        nodes.extend(source_definitions.values())
        nodes.extend(target_definitions.values())
        
        # Find all mapping definitions
        mappings = root.xpath(".//MAPPING")
        
        for mapping in mappings:
            mapping_nodes, mapping_edges = self._parse_mapping(
                mapping, file_path, source_definitions, target_definitions, transformation_definitions
            )
            nodes.extend(mapping_nodes)
            edges.extend(mapping_edges)
        
        return nodes, edges

    def _parse_source_definitions(self, root: etree._Element, file_path: str) -> Dict[str, Node]:
        """Parse source definitions and return them as Node objects."""
        sources = {}
        source_elements = root.xpath(".//SOURCE")
        
        for source in source_elements:
            source_name = source.get("NAME", "")
            if source_name:
                source_id = f"data_asset:source:{source_name}"
                
                # Create source context for traceability
                source_context = SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="xml",
                    xml_path=f"//SOURCE[@NAME='{source_name}']",
                    line_number=source.sourceline
                    ,
            technology="Informatica"
        )
                
                # Parse field information
                fields = self._parse_source_fields(source)
                
                # Create DATA_ASSET node for source
                source_node = Node(
                    node_id=source_id,
                    node_type=NodeType.DATA_ASSET.value,
                    name=source_name,
                    properties={
                        "name": source_name,
                        "database_type": source.get("DATABASETYPE", ""),
                        "description": source.get("DESCRIPTION", ""),
                        "owner_name": source.get("OWNERNAME", ""),
                        "fields": fields,
                        "source_context": source_context,
                        "informatica_type": "source",
                        "asset_type": "table"
                    }
                )
                
                sources[source_name] = source_node
        
        return sources

    def _parse_target_definitions(self, root: etree._Element, file_path: str) -> Dict[str, Node]:
        """Parse target definitions and return them as Node objects."""
        targets = {}
        target_elements = root.xpath(".//TARGET")
        
        for target in target_elements:
            target_name = target.get("NAME", "")
            if target_name:
                target_id = f"data_asset:target:{target_name}"
                
                # Create source context for traceability
                source_context = SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="xml",
                    xml_path=f"//TARGET[@NAME='{target_name}']",
                    line_number=target.sourceline
                    ,
            technology="Informatica"
        )
                
                # Parse field information
                fields = self._parse_target_fields(target)
                
                # Create DATA_ASSET node for target
                target_node = Node(
                    node_id=target_id,
                    node_type=NodeType.DATA_ASSET.value,
                    name=target_name,
                    properties={
                        "name": target_name,
                        "database_type": target.get("DATABASETYPE", ""),
                        "description": target.get("DESCRIPTION", ""),
                        "fields": fields,
                        "source_context": source_context,
                        "informatica_type": "target",
                        "asset_type": "table"
                    }
                )
                
                targets[target_name] = target_node
        
        return targets

    def _parse_transformation_definitions(self, root: etree._Element, file_path: str) -> Dict[str, Dict]:
        """Parse transformation definitions and return them as a lookup dictionary."""
        transformations = {}
        transformation_elements = root.xpath(".//TRANSFORMATION")
        
        for transformation in transformation_elements:
            transformation_name = transformation.get("NAME", "")
            if transformation_name:
                transformations[transformation_name] = {
                    "element": transformation,
                    "name": transformation_name,
                    "type": transformation.get("TYPE", ""),
                    "description": transformation.get("DESCRIPTION", ""),
                    "is_reusable": transformation.get("REUSABLE", "NO") == "YES"
                }
        
        return transformations

    def _parse_source_fields(self, source: etree._Element) -> List[Dict]:
        """Parse fields from a source definition."""
        fields = []
        source_fields = source.xpath(".//SOURCEFIELD")
        
        for field in source_fields:
            # Use Informatica type mapper to enrich field properties if available
            if self.type_mapper:
                enriched_field_properties = self.type_mapper.enrich_column_properties(
                    informatica_type=field.get("DATATYPE", ""),
                    length=field.get("LENGTH"),
                    precision=field.get("PRECISION"),
                    scale=field.get("SCALE"),
                    nullable=field.get("NULLABLE", "NULL") == "NULL",
                    target_platforms=self.target_platforms
                )
                # Add other metadata not covered by the mapper
                enriched_field_properties['name'] = field.get("NAME", "")
                enriched_field_properties['key_type'] = field.get("KEYTYPE", "NOT A KEY")
                enriched_field_properties['field_number'] = self._safe_int(field.get("FIELDNUMBER"))
                
                fields.append(enriched_field_properties)
            else:
                # Fallback to basic field info if type mapper is not available
                field_info = {
                    "name": field.get("NAME", ""),
                    "datatype": field.get("DATATYPE", ""),
                    "precision": self._safe_int(field.get("PRECISION")),
                    "scale": self._safe_int(field.get("SCALE")),
                    "length": self._safe_int(field.get("LENGTH")),
                    "nullable": field.get("NULLABLE", "NULL") == "NULL",
                    "key_type": field.get("KEYTYPE", "NOT A KEY"),
                    "field_number": self._safe_int(field.get("FIELDNUMBER"))
                }
                fields.append(field_info)
        
        return sorted(fields, key=lambda x: x.get("field_number", 0) or 0)

    def _parse_target_fields(self, target: etree._Element) -> List[Dict]:
        """Parse fields from a target definition."""
        fields = []
        target_fields = target.xpath(".//TARGETFIELD")
        
        for field in target_fields:
            # Use Informatica type mapper to enrich field properties if available
            if self.type_mapper:
                enriched_field_properties = self.type_mapper.enrich_column_properties(
                    informatica_type=field.get("DATATYPE", ""),
                    length=field.get("LENGTH"),
                    precision=field.get("PRECISION"),
                    scale=field.get("SCALE"),
                    nullable=field.get("NULLABLE", "NULL") == "NULL",
                    target_platforms=self.target_platforms
                )
                # Add other metadata not covered by the mapper
                enriched_field_properties['name'] = field.get("NAME", "")
                enriched_field_properties['key_type'] = field.get("KEYTYPE", "NOT A KEY")
                enriched_field_properties['field_number'] = self._safe_int(field.get("FIELDNUMBER"))
                
                fields.append(enriched_field_properties)
            else:
                # Fallback to basic field info if type mapper is not available
                field_info = {
                    "name": field.get("NAME", ""),
                    "datatype": field.get("DATATYPE", ""),
                    "precision": self._safe_int(field.get("PRECISION")),
                    "scale": self._safe_int(field.get("SCALE")),
                    "nullable": field.get("NULLABLE", "NULL") == "NULL",
                    "key_type": field.get("KEYTYPE", "NOT A KEY"),
                    "field_number": self._safe_int(field.get("FIELDNUMBER"))
                }
                fields.append(field_info)
        
        return sorted(fields, key=lambda x: x.get("field_number", 0) or 0)

    def _safe_int(self, value: Optional[str]) -> Optional[int]:
        """Safely convert string to int, return None if conversion fails."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _parse_mapping(
        self,
        mapping: etree._Element,
        file_path: str,
        source_definitions: Dict[str, Node],
        target_definitions: Dict[str, Node],
        transformation_definitions: Dict[str, Dict]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse a single mapping definition.
        """
        nodes = []
        edges = []
        
        mapping_name = mapping.get("NAME", "UnknownMapping")
        mapping_id = f"mapping:{mapping_name}"
        
        # Create mapping node
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            source_file_type="xml",
            xml_path=f"//MAPPING[@NAME='{mapping_name}']",
            line_number=mapping.sourceline
            ,
            technology="Informatica"
        )
        
        mapping_node = Node(
            node_id=mapping_id,
            node_type=NodeType.PIPELINE.value,
            name=mapping_name,
            properties={
                "name": mapping_name,
                "description": mapping.get("DESCRIPTION", ""),
                "is_valid": mapping.get("ISVALID", "YES") == "YES",
                "version_number": mapping.get("VERSIONNUMBER", "1"),
                "source_context": source_context,
                "informatica_type": "mapping"
            }
        )
        nodes.append(mapping_node)
        
        # Get session connection context for this mapping
        session_context = self.session_connections.get(mapping_name, {})
        
        # Parse transformation instances within the mapping
        instances = mapping.xpath(".//INSTANCE")
        instance_nodes = {}  # Keep track of instance nodes for connector parsing
        
        for instance in instances:
            instance_nodes_list, instance_edges = self._parse_transformation_instance(
                instance, mapping_id, file_path, transformation_definitions, session_context
            )
            nodes.extend(instance_nodes_list)
            edges.extend(instance_edges)
            
            # Store instance nodes for later connector parsing
            if instance_nodes_list:
                # Use same logic as _get_effective_instance_name - check both NAME and INSTANCENAME
                instance_name = instance.get("NAME", "") or instance.get("INSTANCENAME", "")
                if instance_name:
                    instance_nodes[instance_name] = instance_nodes_list[0]
                    logger.debug(f"Registered instance node: {instance_name} -> {instance_nodes_list[0].node_id}")
        
        # Parse connectors (data flow connections)
        connectors = mapping.xpath(".//CONNECTOR")
        for connector in connectors:
            connector_edges = self._parse_connector(
                connector, mapping_id, file_path, instance_nodes
            )
            edges.extend(connector_edges)
        
        # REQUIREMENT 2.2: Parse mapping variables
        mapping_variable_nodes, mapping_variable_edges = self._parse_variables(
            mapping, mapping_id, file_path, "mapping"
        )
        nodes.extend(mapping_variable_nodes)
        edges.extend(mapping_variable_edges)
        
        # REQUIREMENT 2.1: Post-processing step for unconnected lookup resolution
        unconnected_lookup_edges = self._resolve_unconnected_lookups(
            nodes, mapping_id, file_path
        )
        edges.extend(unconnected_lookup_edges)
        
        # REQUIREMENT 2.2: Post-processing step for variable reference resolution
        variable_dependency_edges = self._resolve_variable_references(
            nodes, mapping_id, file_path
        )
        edges.extend(variable_dependency_edges)
        
        # REQUIREMENT 2.3: Parse target load order groups
        target_load_order_edges = self._parse_target_load_order(
            mapping, mapping_id, file_path, nodes
        )
        edges.extend(target_load_order_edges)

        # Aggregate field mappings from connectors
        edges = self._aggregate_field_mappings(edges)

        return nodes, edges

    def _parse_transformation_instance(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_definitions: Dict[str, Dict],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse a transformation instance and dispatch to specific transformation parsers.
        """
        instance_name = instance.get("INSTANCENAME") or instance.get("NAME", "UnknownInstance")
        transformation_name = instance.get("TRANSFORMATION_NAME") or instance.get("TRANSFORMATIONNAME", "")
        transformation_type = instance.get("TRANSFORMATION_TYPE") or instance.get("TRANSFORMATIONTYPE", "")
        
        # Get transformation definition if available
        transformation_def = transformation_definitions.get(transformation_name, {})
        if not transformation_def:
            # For built-in transformations like Source Qualifier, use instance info
            # CRITICAL: Include the element so transformation parsers can access TABLEATTRIBUTE data
            transformation_def = {
                "element": instance,  # Use the passed element (TRANSFORMATION or INSTANCE)
                "name": transformation_name,
                "type": transformation_type,
                "description": "",
                "is_reusable": False
            }
        
        # Dispatch to specific transformation parser based on type
        return self._dispatch_transformation_parser(
            instance, mapping_id, file_path, transformation_def, session_context
        )

    def _get_effective_instance_name(self, instance: etree._Element) -> str:
        """
        Get effective instance name, using fallbacks for empty values.
        This handles both INSTANCENAME and NAME attributes and provides fallbacks.
        """
        # Check both INSTANCENAME and NAME attributes (Informatica uses both)
        instance_name = instance.get("INSTANCENAME", "") or instance.get("NAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "") or instance.get("TRANSFORMATION_NAME", "")
        return instance_name or transformation_name or "UnknownInstance"
    
    def _dispatch_transformation_parser(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Dispatch to the appropriate transformation parser based on transformation type.
        This follows the same pattern as the SSIS parser's component dispatcher.
        
        CRITICAL: Use TRANSFORMATION_TYPE from the INSTANCE XML tag, not transformation_def.
        This is the key to proper Informatica parsing.
        """
        # FIXED: Read TRANSFORMATION_TYPE directly from the instance XML element
        transformation_type = instance.get("TRANSFORMATION_TYPE", "").lower()
        
        # Handle both variants (TRANSFORMATION_TYPE vs TRANSFORMATIONTYPE)
        if not transformation_type:
            transformation_type = instance.get("TRANSFORMATIONTYPE", "").lower()
        
        logger.debug(f"Dispatching transformation instance: {instance.get('INSTANCENAME', 'Unknown')} "
                    f"with type: '{transformation_type}'")
        
        # Map transformation types to parser methods - using exact XML values
        parser_map = {
            "source qualifier": self._parse_source_qualifier_transformation,
            "target definition": self._parse_target_transformation,
            "source definition": self._parse_source_definition_transformation,
            "expression": self._parse_expression_transformation,
            "filter": self._parse_filter_transformation,
            "aggregator": self._parse_aggregator_transformation,
            "sorter": self._parse_sorter_transformation,
            "joiner": self._parse_joiner_transformation,
            "lookup": self._parse_lookup_transformation,
            "router": self._parse_router_transformation,
            "union": self._parse_union_transformation,
            "sequence generator": self._parse_sequence_generator_transformation,
            "update strategy": self._parse_update_strategy_transformation,
            "normalizer": self._parse_normalizer_transformation,
            "rank": self._parse_rank_transformation,
            # REQUIREMENT 2.1: Add support for Mapplet transformations
            "mapplet": self._parse_mapplet_transformation,
            # REQUIREMENT 2.3: Add support for Stored Procedure transformations
            "stored procedure": self._parse_stored_procedure_transformation,
            # Additional transformation types found in real-world XML
            "custom transformation": self._parse_generic_transformation,
            "lookup procedure": self._parse_lookup_transformation,
            "sequence": self._parse_sequence_generator_transformation
        }
        
        parser_method = parser_map.get(transformation_type)
        if parser_method:
            return parser_method(instance, mapping_id, file_path, transformation_def, session_context)
        else:
            logger.warning(f"No specific parser for transformation type '{transformation_type}', "
                          f"using generic parser")
            # Generic transformation parser for unknown types
            return self._parse_generic_transformation(
                instance, mapping_id, file_path, transformation_def, session_context
            )

    def _parse_connector(
        self,
        connector: etree._Element,
        mapping_id: str,
        file_path: str,
        instance_nodes: Dict[str, Node]
    ) -> List[Edge]:
        """
        Parse connectors that define data flow between transformation instances.
        
        CRITICAL: This implements the complete data lineage including:
        1. WRITES_TO edges for Target Definition instances
        2. Column-level lineage via FROMFIELD/TOFIELD
        3. Proper handling of Source Definition to Source Qualifier flows
        """
        edges = []
        
        from_instance = connector.get("FROMINSTANCE", "")
        to_instance = connector.get("TOINSTANCE", "")
        from_instancetype = connector.get("FROMINSTANCETYPE", "")
        to_instancetype = connector.get("TOINSTANCETYPE", "")
        
        # Extract column-level lineage
        from_field = connector.get("FROMFIELD", "")
        to_field = connector.get("TOFIELD", "")
        
        if from_instance and to_instance:
            source_context = SourceContext.create_node_traceability(
                source_file_path=file_path,
                line_number=connector.sourceline or 0,
                source_file_type="xml",
                xml_path=f"//CONNECTOR[@FROMINSTANCE='{from_instance}'][@TOINSTANCE='{to_instance}']",
                technology="Informatica"
            )
            
            # CRITICAL: Handle Target Definition instances - create WRITES_TO to DATA_ASSET
            if to_instancetype == "Target Definition":
                # Find the source operation node
                from_node = instance_nodes.get(from_instance)
                if from_node:
                    # Create WRITES_TO edge to the actual DATA_ASSET node for the target
                    target_data_asset_id = f"data_asset:target:{to_instance}"
                    
                    # REQUIREMENT 2.2: Correlate Update Strategy Logic with Target Instance DML Options
                    edge_properties = {
                        "connector_type": "data_flow",
                        "from_instance": from_instance,
                        "to_instance": to_instance,
                        "from_instance_type": from_instancetype,
                        "to_instance_type": to_instancetype,
                        "from_field": from_field,
                        "to_field": to_field,
                        "column_lineage": f"{from_field} -> {to_field}" if from_field and to_field else "",
                        "derivation_method": "xml_metadata",
                        "confidence_level": "high",
                        "source_context": source_context
                    }
                    
                    # Check if source is an Update Strategy transformation and correlate with target DML options
                    if (from_node.properties.get("transformation_type") == "Update Strategy" and 
                        "update_strategy_logic" in from_node.properties):
                        
                        # Get Update Strategy DD_ operations
                        strategy_operations = from_node.properties.get("dml_operations_supported", [])
                        
                        # Extract target instance DML settings (would need to be enhanced to parse target definition)
                        target_dml_options = self._extract_target_dml_options(to_instance, mapping_id, file_path)
                        
                        # Correlate Update Strategy operations with target settings
                        effective_operations = self._correlate_update_strategy_with_target(
                            strategy_operations, target_dml_options
                        )
                        
                        # Add correlation results to edge properties
                        edge_properties.update({
                            "has_update_strategy_correlation": True,
                            "strategy_operations": strategy_operations,
                            "target_dml_options": target_dml_options,
                            "effective_operations": effective_operations.get("allowed", []),
                            "rejected_operations": effective_operations.get("rejected", []),
                            "dml_correlation_summary": effective_operations.get("summary", "")
                        })
                        
                        logger.debug(f"Correlated Update Strategy with Target DML: {from_instance} -> {to_instance}")
                        logger.debug(f"Effective operations: {effective_operations.get('allowed', [])}")
                    
                    connector_edge = Edge(
                        source_id=from_node.node_id,
                        target_id=target_data_asset_id,
                        relation=EdgeType.WRITES_TO.value,
                        properties=edge_properties
                    )
                    edges.append(connector_edge)
                    logger.debug(f"Created WRITES_TO edge: {from_node.node_id} -> {target_data_asset_id} "
                                f"({from_field} -> {to_field})")
                else:
                    logger.debug(f"Source node not found for WRITES_TO: {from_instance}. "
                               f"Available nodes: {list(instance_nodes.keys())[:5]}...")
            
            # Handle Source Definition to Source Qualifier flow  
            elif from_instancetype == "Source Definition" and to_instancetype == "Source Qualifier":
                # This is handled by the Source Qualifier's ASSOCIATED_SOURCE_INSTANCE logic
                # We create a data flow edge between them but the READS_FROM edge to DATA_ASSET
                # is created by the Source Qualifier parser
                from_node = instance_nodes.get(from_instance) 
                to_node = instance_nodes.get(to_instance)
                
                if from_node and to_node:
                    connector_edge = Edge(
                        source_id=from_node.node_id,
                        target_id=to_node.node_id,
                        relation=EdgeType.DEPENDS_ON.value,
                        properties={
                            "connector_type": "data_flow",
                            "from_instance": from_instance,
                            "to_instance": to_instance,
                            "from_instance_type": from_instancetype,
                            "to_instance_type": to_instancetype,
                            "from_field": from_field,
                            "to_field": to_field,
                            "column_lineage": f"{from_field} -> {to_field}" if from_field and to_field else "",
                            "source_context": source_context
                        }
                    )
                    # Note: We don't add this edge as it would be redundant with the READS_FROM 
                    # edge created by the Source Qualifier
                    logger.debug(f"Skipping Source Definition -> Source Qualifier edge "
                                f"(handled by READS_FROM): {from_instance} -> {to_instance}")
            
            # Handle transformation-to-transformation data flow
            else:
                from_node = instance_nodes.get(from_instance)
                to_node = instance_nodes.get(to_instance)
                
                if from_node and to_node:
                    # Determine edge type based on instance types
                    if from_instancetype.startswith("Source") and to_instancetype == "Source Qualifier":
                        edge_type = EdgeType.DEPENDS_ON  # Data flow
                    elif to_instancetype.startswith("Target"):
                        edge_type = EdgeType.WRITES_TO
                    elif from_instancetype.startswith("Source"):
                        edge_type = EdgeType.READS_FROM
                    else:
                        edge_type = EdgeType.DEPENDS_ON  # Transformation data flow
                    
                    connector_edge = Edge(
                        source_id=from_node.node_id,
                        target_id=to_node.node_id,
                        relation=edge_type.value,
                        properties={
                            "connector_type": "data_flow",
                            "from_instance": from_instance,
                            "to_instance": to_instance,
                            "from_instance_type": from_instancetype,
                            "to_instance_type": to_instancetype,
                            "from_field": from_field,
                            "to_field": to_field,
                            "column_lineage": f"{from_field} -> {to_field}" if from_field and to_field else "",
                            "derivation_method": "xml_metadata",
                            "confidence_level": "high",
                            "source_context": source_context
                        }
                    )
                    edges.append(connector_edge)
                    logger.debug(f"Created {edge_type.value} edge: {from_node.node_id} -> {to_node.node_id} "
                                f"({from_field} -> {to_field})")
                else:
                    logger.debug(f"Missing nodes for connector: {from_instance} -> {to_instance}. "
                               f"Available: {list(instance_nodes.keys())[:3]}...")
        else:
            logger.warning(f"Incomplete connector: FROMINSTANCE='{from_instance}' "
                          f"TOINSTANCE='{to_instance}'")

        return edges

    def _aggregate_field_mappings(self, edges: List[Edge]) -> List[Edge]:
        """
        Aggregate field mappings for edges between the same source and target.

        For connectors that have FROMFIELD/TOFIELD information, we want to combine
        multiple edges (one per field) into a single edge with a field_mappings array.

        Example:
        Input: 3 edges from TransformA to TransformB, each with one field mapping
        Output: 1 edge from TransformA to TransformB with 3 field mappings

        Args:
            edges: List of edges from connector parsing

        Returns:
            List of edges with aggregated field mappings
        """
        from collections import defaultdict

        # Group edges by (source_id, target_id, relation)
        edge_groups = defaultdict(list)
        non_connector_edges = []

        # Debug counters
        connector_count = 0
        with_fields_count = 0

        for edge in edges:
            # Check if it's a connector edge
            is_connector = edge.properties.get("connector_type") == "data_flow"
            has_from_field = bool(edge.properties.get("from_field"))
            has_to_field = bool(edge.properties.get("to_field"))

            if is_connector:
                connector_count += 1

            # Only aggregate connector edges that have field information
            if is_connector and has_from_field and has_to_field:
                with_fields_count += 1
                key = (edge.source_id, edge.target_id, edge.relation)
                edge_groups[key].append(edge)
            else:
                # Keep non-connector edges as-is
                non_connector_edges.append(edge)

        logger.debug(f"Aggregation input: {len(edges)} total edges, {connector_count} connector edges, "
                    f"{with_fields_count} with field mappings")

        # Aggregate field mappings for grouped edges
        aggregated_edges = []

        for (source_id, target_id, relation), edge_list in edge_groups.items():
            if len(edge_list) == 1:
                # Only one field mapping, keep the edge as-is
                aggregated_edges.append(edge_list[0])
            else:
                # Multiple field mappings, aggregate them
                field_mappings = []
                base_edge = edge_list[0]

                for edge in edge_list:
                    from_field = edge.properties.get("from_field", "")
                    to_field = edge.properties.get("to_field", "")

                    if from_field and to_field:
                        field_mappings.append({
                            "from_field": from_field,
                            "to_field": to_field,
                            "transformation": "passthrough"  # Default, can be enhanced
                        })

                # Create aggregated edge with field_mappings array
                aggregated_properties = base_edge.properties.copy()

                # Remove individual field properties
                aggregated_properties.pop("from_field", None)
                aggregated_properties.pop("to_field", None)
                aggregated_properties.pop("column_lineage", None)

                # Add field_mappings array
                aggregated_properties["field_mappings"] = field_mappings
                aggregated_properties["field_count"] = len(field_mappings)

                aggregated_edge = Edge(
                    source_id=source_id,
                    target_id=target_id,
                    relation=relation,
                    properties=aggregated_properties
                )
                aggregated_edges.append(aggregated_edge)

                logger.debug(f"Aggregated {len(field_mappings)} field mappings: "
                           f"{source_id} -> {target_id}")

        # Combine aggregated edges with non-connector edges
        return aggregated_edges + non_connector_edges

    def _extract_sql_semantics(self, sql_or_expression: str, context_name: str = "") -> Dict[str, Any]:
        """
        Extract SQL semantics from SQL queries or Informatica expressions.
        
        This method handles both:
        1. Raw SQL queries (Source Qualifiers)
        2. Informatica expressions that contain SQL-like logic
        """
        if not sql_or_expression or not isinstance(sql_or_expression, str):
            return {
                "sql_semantics": None,
                "has_sql": False
            }
        
        # Decode HTML entities from XML (e.g., &gt; -> >, &lt; -> <, &amp; -> &)
        import html
        decoded_expression = html.unescape(sql_or_expression)
        
        # Check if this looks like a SQL query (SELECT, INSERT, UPDATE, DELETE)
        sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE"]
        is_sql_query = any(keyword in decoded_expression.upper() for keyword in sql_keywords)
        
        if is_sql_query:
            # Parse as SQL query
            sql_semantics = self.sql_parser.parse_sql_semantics(decoded_expression)
            return {
                "sql_semantics": sql_semantics.to_dict(),
                "has_sql": True,
                "sql_type": "query"
            }
        else:
            # REQUIREMENT 2.4: Enhanced Informatica expression analysis
            # Parse as Informatica expression with semantic fingerprinting
            
            # REQUIREMENT 2.4: Enhanced function and port recognition with comprehensive Informatica function list
            import re
            
            # Comprehensive list of known Informatica functions for accurate filtering
            KNOWN_INFORMATICA_FUNCTIONS = {
                # Conditional functions
                'IIF', 'DECODE', 'CASE',
                # String functions
                'SUBSTR', 'LPAD', 'RPAD', 'LTRIM', 'RTRIM', 'TRIM', 'UPPER', 'LOWER', 'INITCAP',
                'LENGTH', 'INSTR', 'REPLACE', 'TRANSLATE', 'CONCAT', 'CHR', 'ASCII',
                # Date functions
                'TO_DATE', 'TO_CHAR', 'SYSDATE', 'ADD_TO_DATE', 'TRUNC', 'ROUND', 'EXTRACT',
                'DATE_DIFF', 'DATE_COMPARE', 'GET_DATE_PART', 'SET_DATE_PART',
                # Null handling functions
                'ISNULL', 'NULLIF', 'NVL', 'NVL2',
                # Mathematical functions
                'ABS', 'CEIL', 'FLOOR', 'MOD', 'POWER', 'ROUND', 'SQRT', 'TRUNC',
                'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'LOG', 'LN', 'EXP',
                # Conversion functions
                'TO_DECIMAL', 'TO_FLOAT', 'TO_INTEGER', 'TO_BIGINT', 'TO_BINARY',
                # Aggregate functions (when used in expressions)
                'SUM', 'COUNT', 'MIN', 'MAX', 'AVG', 'STDDEV', 'VARIANCE',
                # Special functions
                'ERROR', 'ABORT', 'REG_EXTRACT', 'REG_REPLACE', 'REG_MATCH',
                'METAPHONE', 'SOUNDEX'
            }
            
            # Extract function calls using regex
            function_pattern = r'([A-Z_]+)\s*\('
            function_matches = re.findall(function_pattern, decoded_expression.upper())
            functions_used = list(set(function_matches))  # Remove duplicates
            
            # Extract potential port references
            port_pattern = r'\b([A-Za-z_][A-Za-z0-9_]*)\b'
            potential_ports = re.findall(port_pattern, decoded_expression)
            
            # Comprehensive keyword list for filtering
            sql_keywords = {
                'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'NULL', 'IS', 
                'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IN', 'LIKE', 'BETWEEN',
                'TRUE', 'FALSE', 'YES', 'NO', 'ON', 'OFF'
            }
            
            # Enhanced port reference filtering
            port_references = []
            for port in potential_ports:
                port_upper = port.upper()
                # Skip if it's a known function, SQL keyword, or looks like a constant
                if (port_upper not in sql_keywords and 
                    port_upper not in KNOWN_INFORMATICA_FUNCTIONS and
                    port_upper not in functions_used and
                    not port_upper.isdigit() and  # Skip numbers
                    len(port) > 1):  # Skip single characters
                    port_references.append(port)
            
            port_references = list(set(port_references))  # Remove duplicates
            
            # Enhanced semantic complexity metrics
            nested_iif_count = len(re.findall(r'\bIIF\b', decoded_expression.upper()))
            nested_case_count = len(re.findall(r'\bCASE\b', decoded_expression.upper()))
            
            expression_complexity = {
                "function_count": len(functions_used),
                "port_reference_count": len(port_references),
                "expression_length": len(decoded_expression),
                "nested_iif_count": nested_iif_count,  # Count of IIF statements
                "nested_case_count": nested_case_count,  # Count of CASE statements
                "total_nested_conditionals": nested_iif_count + nested_case_count,
                "has_conditional_logic": bool(re.search(r'\bIIF\b|\bCASE\b', decoded_expression.upper())),
                "has_null_handling": bool(re.search(r'\bISNULL\b|\bNULLIF\b|\bNVL\b', decoded_expression.upper())),
                "has_string_manipulation": bool(re.search(r'\bSUBSTR\b|\bLPAD\b|\bRPAD\b|\bTRIM\b|\bREPLACE\b', decoded_expression.upper())),
                "has_date_functions": bool(re.search(r'\bTO_DATE\b|\bDATE\b|\bTIMESTAMP\b|\bSYSDATE\b', decoded_expression.upper())),
                "has_mathematical_functions": bool(re.search(r'\bABS\b|\bROUND\b|\bCEIL\b|\bFLOOR\b|\bSQRT\b', decoded_expression.upper())),
                "has_regex_functions": bool(re.search(r'\bREG_EXTRACT\b|\bREG_REPLACE\b|\bREG_MATCH\b', decoded_expression.upper())),
                "complexity_score": self._calculate_expression_complexity_score(decoded_expression, functions_used)
            }
            
            return {
                "sql_semantics": {
                    "original_query": decoded_expression,  # Store decoded version
                    "original_xml": sql_or_expression,     # Store original XML for reference
                    "tables": [],
                    "joins": [],
                    "columns": port_references,  # Port references as columns
                    "where_clause": None,
                    "functions_used": functions_used,  # REQUIREMENT 2.4: Function fingerprint
                    "port_references": port_references,  # REQUIREMENT 2.4: Port references
                    "expression_complexity": expression_complexity,  # REQUIREMENT 2.4: Complexity metrics
                    "migration_metadata": {
                        "table_count": 0,
                        "join_count": 0,
                        "column_count": len(port_references),
                        "has_aliases": False,
                        "has_joins": False,
                        "join_types": [],
                        "expression_type": "informatica_expression",
                        "semantic_fingerprint": {
                            "functions": functions_used,
                            "ports": port_references,
                            "complexity": expression_complexity
                        },
                        "context": context_name,
                        "has_decoded_content": decoded_expression != sql_or_expression
                    }
                },
                "has_sql": bool(decoded_expression.strip()),  # True if has content after decoding
                "sql_type": "expression"
            }

    def _calculate_expression_complexity_score(self, expression: str, functions_used: List[str]) -> int:
        """
        Calculate a complexity score for Informatica expressions.
        
        REQUIREMENT 2.4: Enhanced complexity metrics for semantic fingerprinting
        """
        score = 0
        expr_upper = expression.upper()
        
        # Base complexity from expression length
        score += len(expression) // 10  # 1 point per 10 characters
        
        # Function complexity scoring
        for func in functions_used:
            if func in ['IIF', 'DECODE', 'CASE']:
                score += 3  # Conditional logic is complex
            elif func in ['SUBSTR', 'REPLACE', 'REG_EXTRACT', 'REG_REPLACE']:
                score += 2  # String manipulation is moderately complex
            else:
                score += 1  # Basic functions
        
        # Nested structure complexity
        import re
        
        # Count nested parentheses levels
        max_nesting = 0
        current_nesting = 0
        for char in expression:
            if char == '(':
                current_nesting += 1
                max_nesting = max(max_nesting, current_nesting)
            elif char == ')':
                current_nesting -= 1
        score += max_nesting * 2
        
        # Nested IIF complexity (exponential complexity)
        iif_count = len(re.findall(r'\bIIF\b', expr_upper))
        if iif_count > 1:
            score += (iif_count - 1) * 3  # Additional complexity for nested IIFs
        
        # String literal and constant complexity
        string_literals = len(re.findall(r"'[^']*'", expression))
        score += string_literals
        
        return score

    # Specific transformation parsers following SSIS parser patterns

    def _parse_source_qualifier_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse Source Qualifier transformation instance.
        
        CRITICAL: This is where we implement the READS_FROM lineage by extracting
        the ASSOCIATED_SOURCE_INSTANCE from the instance XML tag.
        """
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "") or instance.get("NAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "") or instance.get("TRANSFORMATION_NAME", "")
        instance_id = f"{mapping_id}:source_qualifier:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # CRITICAL FIX: Extract associated source from INSTANCE XML tag, not transformation definition
        associated_source = ""
        associated_source_elements = instance.xpath(".//ASSOCIATED_SOURCE_INSTANCE")
        if associated_source_elements:
            associated_source = associated_source_elements[0].get("NAME", "")
            logger.info(f"Found ASSOCIATED_SOURCE_INSTANCE: {associated_source} "
                       f"for Source Qualifier: {instance_name}")
        
        # Get transformation definition for SQL query and other attributes
        transformation_element = transformation_def.get("element")
        sql_query = ""
        
        if transformation_element is not None:
            # Extract SQL query from transformation definition
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                if attr.get("NAME") == "Sql Query":
                    sql_query = attr.get("VALUE", "")
                    break
        
        # REQUIREMENT 3.1: Apply session-level overrides to base properties
        base_properties = {
            "name": instance_name,
            "transformation_name": transformation_name,
            "transformation_type": "Source Qualifier",
            "operation_subtype": self._categorize_operation_subtype("Source Qualifier"),
            "sql_query": sql_query,
            "associated_source": associated_source,
            "source_context": source_context,
            "informatica_type": "source_qualifier"
        }
        
        # Apply session overrides - this is CRITICAL for accurate migration
        enhanced_properties = self._apply_session_overrides(
            transformation_name=transformation_name,
            instance_name=instance_name,
            base_properties=base_properties,
            session_context=session_context
        )
        
        # Parse SQL semantics using the EFFECTIVE SQL query (after session overrides)
        effective_sql = enhanced_properties.get("sql_query", sql_query)
        sql_semantics = self.sql_parser.parse_sql_semantics(effective_sql)
        enhanced_properties["sql_semantics"] = sql_semantics.to_dict()
        
        # Log if session overrides were applied
        if enhanced_properties.get("has_session_overrides", False):
            logger.info(f"Applied session overrides to Source Qualifier {transformation_name}: "
                       f"SQL={'sql_override_applied' in enhanced_properties}, "
                       f"Connection={'connection_override_applied' in enhanced_properties}")
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties=enhanced_properties
        )
        nodes.append(node)

        # Create USES_CONNECTION edge if connection is specified
        self._create_uses_connection_edge(instance_id, enhanced_properties, edges)

        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)

        # CRITICAL: Create READS_FROM edge to the DATA_ASSET source node
        if associated_source:
            # Reference the actual DATA_ASSET node created for this source
            source_data_asset_id = f"data_asset:source:{associated_source}"
            reads_from_edge = Edge(
                source_id=instance_id,
                target_id=source_data_asset_id,
                relation=EdgeType.READS_FROM.value,
                properties={
                    "relationship": "source_qualifier_reads_from_source",
                    "associated_source_instance": associated_source,
                    "derivation_method": "xml_metadata",
                    "confidence_level": "high",
                    "source_context": source_context
                }
            )
            edges.append(reads_from_edge)
            logger.info(f"Created READS_FROM edge: {instance_id} -> {source_data_asset_id}")
        else:
            logger.warning(f"No ASSOCIATED_SOURCE_INSTANCE found for Source Qualifier: {instance_name}")
        
        return nodes, edges

    def _parse_source_definition_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse Source Definition instance.
        
        Source Definition instances don't create operation nodes - they are represented
        as DATA_ASSET nodes created in _parse_source_definitions. This is a passthrough.
        """
        # Source definitions are already handled as DATA_ASSET nodes
        # No additional operation nodes needed for source definition instances
        return [], []

    def _parse_target_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Handles Target Definition instances.

        CRITICAL FIX: Target instances can have different names from their definitions
        (e.g., definition="SRTTRANS", instance="SRTTRANS1"). Connectors reference
        the instance name, so we must create DATA_ASSET nodes for each instance.
        """
        nodes = []
        edges = []

        # Get instance name (what connectors will reference)
        instance_name = instance.get("INSTANCENAME") or instance.get("NAME", "")
        if not instance_name:
            return nodes, edges

        # Get the transformation definition name (the TARGET definition)
        transformation_name = instance.get("TRANSFORMATIONNAME") or instance.get("NAME", "")

        # Create source context for traceability
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@NAME='{instance_name}'][@TRANSFORMATION_TYPE='Target Definition']",
            technology="Informatica"
        )

        # Build properties - get metadata from transformation definition if available
        properties = {
            "name": instance_name,
            "transformation_name": transformation_name,
            "informatica_type": "target",
            "asset_type": "table",
            "source_context": source_context
        }

        # Enrich with target definition metadata if available
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            properties["database_type"] = transformation_element.get("DATABASETYPE", "")
            properties["description"] = transformation_element.get("DESCRIPTION", "")

            # Parse field information from TARGET definition
            fields = self._parse_target_fields(transformation_element)
            if fields:
                properties["fields"] = fields

        # Check session context for connection override
        connection_name = session_context.get("connections", {}).get(instance_name, "")
        if connection_name:
            properties["session_connection"] = connection_name
            properties["has_session_overrides"] = True
            logger.debug(f"Target instance {instance_name} uses session connection: {connection_name}")

        # Create DATA_ASSET node for this target instance
        target_id = f"data_asset:target:{instance_name}"
        target_node = Node(
            node_id=target_id,
            node_type=NodeType.DATA_ASSET.value,
            name=instance_name,
            properties=properties
        )
        nodes.append(target_node)

        logger.debug(f"Created target instance DATA_ASSET: {target_id}")

        return nodes, edges

    def _parse_expression_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Expression transformation instance."""
        nodes = []
        edges = []
        
        # Handle both TRANSFORMATION and INSTANCE elements
        instance_name = instance.get("INSTANCENAME") or instance.get("NAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME") or instance.get("NAME", "")
        instance_id = f"{mapping_id}:expression:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract expressions and lookups from transformation definition
        expressions = {}
        unconnected_lookups = []
        variable_references = []  # REQUIREMENT 2.2: Store variable references found in expressions
        combined_expressions = []  # For SQL semantics analysis
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse transformation fields for expressions
            transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
            for field in transform_fields:
                field_name = field.get("NAME", "")
                expression = field.get("EXPRESSION", "")
                if expression:
                    expressions[field_name] = expression
                    combined_expressions.append(expression)
                    
                    # Check for unconnected lookup calls in expression
                    lookup_pattern = r':LKP\.(\w+)\('
                    lookup_matches = re.findall(lookup_pattern, expression)
                    for lookup_name in lookup_matches:
                        if lookup_name not in unconnected_lookups:
                            unconnected_lookups.append(lookup_name)
                    
                    # REQUIREMENT 2.2: Check for variable references in expression
                    variable_pattern = r'\$\$([A-Za-z_][A-Za-z0-9_]*)'
                    variable_matches = re.findall(variable_pattern, expression)
                    for variable_name in variable_matches:
                        if variable_name not in variable_references:
                            variable_references.append(variable_name)
        
        # Extract SQL semantics from expressions
        combined_expression_text = " | ".join(combined_expressions) if combined_expressions else ""
        sql_semantics_result = self._extract_sql_semantics(
            combined_expression_text,
            f"Expression transformation: {instance_name}"
        )

        # Build enhanced properties
        properties = {
            "name": instance_name,
            "transformation_name": transformation_name,
            "transformation_type": "Expression",
            "operation_subtype": self._categorize_operation_subtype("Expression"),
            "expressions": expressions,
            "unconnected_lookups": unconnected_lookups,
            "variable_references": variable_references,  # REQUIREMENT 2.2: Variable references found in expressions
            "sql_semantics": sql_semantics_result.get("sql_semantics"),
            "has_sql": sql_semantics_result.get("has_sql", False),
            "sql_type": sql_semantics_result.get("sql_type", "expression"),
            "source_context": source_context,
            "informatica_type": "expression"
        }

        # Enhancement #1: Add column lineage
        if transformation_element is not None:
            column_lineage = self.column_lineage_builder.build_lineage(
                transformation_element=transformation_element,
                transformation_type="Expression",
                instance_name=instance_name,
            )
            if column_lineage:
                properties["column_lineage"] = column_lineage

        # Enhancement #5: Add error handling metadata
        if transformation_element is not None:
            error_handling = self.error_handling_builder.build_error_handling_metadata(
                transformation_element=transformation_element,
                transformation_type="Expression",
                instance_name=instance_name,
                properties=properties,
            )
            if error_handling:
                properties["error_handling"] = error_handling

        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties=properties
        )
        nodes.append(node)

        # Create USES_CONNECTION edge if connection is specified
        self._create_uses_connection_edge(instance_id, properties, edges)

        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)

        # Create edges for unconnected lookups (only if lookup nodes exist or will be created)
        for lookup_name in unconnected_lookups:
            lookup_id = f"{mapping_id}:lookup:{lookup_name}"
            # Note: We store the unconnected lookup reference in properties for now
            # The actual edge will be created later if the lookup transformation is found
            logger.debug(f"Found unconnected lookup reference: {lookup_name} in expression transformation {instance_name}")
            # Store this information for potential later edge creation during post-processing
        
        return nodes, edges

    def _parse_joiner_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Joiner transformation instance."""
        nodes = []
        edges = []
        
        # Handle both TRANSFORMATION and INSTANCE elements
        instance_name = instance.get("INSTANCENAME") or instance.get("NAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME") or instance.get("NAME", "")
        instance_id = f"{mapping_id}:joiner:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract join information from transformation definition
        join_condition = ""
        join_type = "Normal Join"  # Default
        master_source = ""
        detail_source = ""
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse table attributes for join properties
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                if attr_name == "Join Condition":
                    join_condition = attr_value
                elif attr_name == "Join Type":
                    join_type = attr_value
            
            # Parse transformation fields to identify master and detail
            transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
            for field in transform_fields:
                port_type = field.get("PORTTYPE", "")
                if port_type == "INPUT" and "MASTER" in port_type:
                    master_source = field.get("NAME", "")
                elif port_type == "INPUT" and "DETAIL" in port_type:
                    detail_source = field.get("NAME", "")
        
        # Extract SQL semantics from join condition
        sql_semantics_result = self._extract_sql_semantics(
            join_condition,
            f"Joiner transformation: {instance_name} ({join_type})"
        )

        # Build enhanced properties
        properties = {
            "name": instance_name,
            "transformation_name": transformation_name,
            "transformation_type": "Joiner",
            "operation_subtype": self._categorize_operation_subtype("Joiner"),
            "join_condition": join_condition,
            "join_type": join_type,
            "master_source": master_source,
            "detail_source": detail_source,
            "sql_semantics": sql_semantics_result.get("sql_semantics"),
            "has_sql": sql_semantics_result.get("has_sql", False),
            "sql_type": sql_semantics_result.get("sql_type", "expression"),
            "source_context": source_context,
            "informatica_type": "joiner"
        }

        # Enhancement #1: Add column lineage (with joiner-specific builder)
        if transformation_element is not None:
            column_lineage = self.joiner_lineage_builder.build_lineage(
                transformation_element=transformation_element,
                transformation_type="Joiner",
                instance_name=instance_name,
            )
            if column_lineage:
                properties["column_lineage"] = column_lineage

        # Enhancement #5: Add error handling metadata
        if transformation_element is not None:
            error_handling = self.error_handling_builder.build_error_handling_metadata(
                transformation_element=transformation_element,
                transformation_type="Joiner",
                instance_name=instance_name,
                properties=properties,
            )
            if error_handling:
                properties["error_handling"] = error_handling

        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties=properties
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges

    def _parse_router_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Router transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:router:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract router groups and conditions
        groups = {}
        default_group = None
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            group_elements = transformation_element.xpath(".//GROUP")
            for group in group_elements:
                group_name = group.get("NAME", "")
                group_type = group.get("TYPE", "")
                group_expression = group.get("EXPRESSION", "")
                
                if group_type == "INPUT":
                    continue  # Skip input group
                elif group_type == "OUTPUT/DEFAULT":
                    default_group = group_name
                else:
                    groups[group_name] = {
                        "expression": group_expression,
                        "type": group_type
                    }
        
        # Extract SQL semantics from router group expressions
        all_router_expressions = [group["expression"] for group in groups.values() if group["expression"]]
        combined_expressions = " | ".join(all_router_expressions) if all_router_expressions else ""
        sql_semantics_result = self._extract_sql_semantics(
            combined_expressions, 
            f"Router transformation: {instance_name}"
        )
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Router",
                "operation_subtype": self._categorize_operation_subtype("Router"),
                "groups": groups,
                "default_group": default_group,
                "sql_semantics": sql_semantics_result.get("sql_semantics"),
                "has_sql": sql_semantics_result.get("has_sql", False),
                "sql_type": sql_semantics_result.get("sql_type", "expression"),
                "source_context": source_context,
                "informatica_type": "router"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges

    def _parse_lookup_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Lookup transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:lookup:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract lookup properties including caching settings (REQUIREMENT 3.4)
        lookup_source = ""
        lookup_condition = ""
        is_connected = True  # Default assumption
        lookup_caching_enabled = False
        lookup_cache_type = "static"  # static, dynamic
        lookup_policy_on_multiple_match = "use_first_match"
        persistent_lookup_cache = False
        lookup_cache_size = ""
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse table attributes for lookup properties
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                
                # Basic lookup properties
                if attr_name == "Lookup Source Database":
                    lookup_source = attr_value
                elif attr_name == "Lookup Condition":
                    lookup_condition = attr_value
                    
                # REQUIREMENT 3.4: Caching properties for migration
                elif attr_name == "Lookup caching enabled":
                    lookup_caching_enabled = attr_value.lower() in ["true", "yes", "1"]
                elif attr_name == "Dynamic Lookup Cache":
                    lookup_cache_type = "dynamic" if attr_value.lower() in ["true", "yes", "1"] else "static"
                elif attr_name == "Lookup policy on multiple match":
                    lookup_policy_on_multiple_match = attr_value
                elif attr_name == "Persistent Lookup cache":
                    persistent_lookup_cache = attr_value.lower() in ["true", "yes", "1"]
                elif attr_name == "Cache Size":
                    lookup_cache_size = attr_value
        
        # REQUIREMENT 3.1: Apply session-level overrides to base properties
        base_properties = {
            "name": instance_name,
            "transformation_name": transformation_name,
            "transformation_type": "Lookup",
            "operation_subtype": self._categorize_operation_subtype("Lookup"),
            "lookup_source": lookup_source,
            "lookup_condition": lookup_condition,  # Will be overridden by lookup_sql if present
            "lookup_sql": lookup_condition,  # Alias for lookup condition
            "is_connected": is_connected,
            
            # REQUIREMENT 3.4: Caching properties for accurate migration
            "lookup_caching_enabled": lookup_caching_enabled,
            "lookup_cache_type": lookup_cache_type,
            "lookup_policy_on_multiple_match": lookup_policy_on_multiple_match,
            "persistent_lookup_cache": persistent_lookup_cache,
            "lookup_cache_size": lookup_cache_size,
            
            "source_context": source_context,
            "informatica_type": "lookup"
        }
        
        # Apply session overrides - critical for lookup SQL and connection overrides
        enhanced_properties = self._apply_session_overrides(
            transformation_name=transformation_name,
            instance_name=instance_name,
            base_properties=base_properties,
            session_context=session_context
        )
        
        # Extract SQL semantics using the EFFECTIVE lookup condition (after session overrides)
        effective_lookup_sql = enhanced_properties.get("lookup_sql", lookup_condition)
        sql_semantics_result = self._extract_sql_semantics(
            effective_lookup_sql, 
            f"Lookup transformation: {instance_name}"
        )
        enhanced_properties["sql_semantics"] = sql_semantics_result.get("sql_semantics")
        enhanced_properties["has_sql"] = sql_semantics_result.get("has_sql", False)
        enhanced_properties["sql_type"] = sql_semantics_result.get("sql_type", "expression")
        
        # Get EFFECTIVE connection information (after session overrides)
        effective_connection_name = enhanced_properties.get("connection_name", "")
        connection_details = self.connections_context.get(effective_connection_name, {})
        enhanced_properties.update({
            "connection_details": connection_details,
            "server": connection_details.get('server'),
            "database": connection_details.get('database'),
            "provider": connection_details.get('provider')
        })
        
        # Log if session overrides were applied
        if enhanced_properties.get("has_session_overrides", False):
            logger.info(f"Applied session overrides to Lookup {transformation_name}: "
                       f"LookupSQL={'lookup_override_applied' in enhanced_properties}, "
                       f"Connection={'connection_override_applied' in enhanced_properties}")
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties=enhanced_properties
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges

    def _parse_generic_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse generic transformation instance for unknown types."""
        nodes = []
        edges = []
        
        effective_name = self._get_effective_instance_name(instance)
        transformation_name = instance.get("TRANSFORMATIONNAME", "") or instance.get("TRANSFORMATION_NAME", "")
        transformation_type = transformation_def.get("type", "Unknown")
        instance_id = f"{mapping_id}:transformation:{effective_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{effective_name}']",
            technology="Informatica"
        )
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=effective_name,
            properties={
                "name": effective_name,
                "transformation_name": transformation_name,
                "transformation_type": transformation_type,
                "operation_subtype": self._categorize_operation_subtype(transformation_type),
                "description": transformation_def.get("description", ""),
                "is_reusable": transformation_def.get("is_reusable", False),
                "source_context": source_context,
                "informatica_type": "generic_transformation"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges

    def _parse_mapplet_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse Mapplet transformation instances with recursive expansion of internal logic.
        
        REQUIREMENT 2.1: Implement Recursive Parsing for Mapplets
        - Creates a container node for the mapplet
        - Recursively parses all internal transformations and connectors
        - Handles Mapplet Input and Mapplet Output transformations for interface mapping
        """
        nodes: List[Node] = []
        edges: List[Edge] = []
        
        instance_name = instance.get("INSTANCENAME", "Unknown")
        transformation_name = transformation_def.get("NAME", instance_name)
        
        logger.debug(f"Parsing Mapplet transformation: {instance_name} -> {transformation_name}")
        
        # Create the main mapplet container node
        mapplet_id = f"{mapping_id}:mapplet:{instance_name}"
        
        operation_properties = {
            "transformation_type": "mapplet",
            "instance_name": instance_name,
            "transformation_name": transformation_name,
            "technology": "Informatica",
            "mapplet_container": True,
            **SourceContext.create_node_traceability(
                source_file_path=file_path,
                source_file_type="informatica_xml",
                xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']",
                parent_package=os.path.basename(file_path).replace('.xml', '')
            )
        }
        
        mapplet_node = Node(
            node_id=mapplet_id,
            node_type=NodeType.OPERATION,
            name=f"Mapplet: {instance_name}",
            properties=operation_properties
        )
        nodes.append(mapplet_node)
        
        # Create containment edge from mapping to mapplet
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=mapplet_id,
            relation=EdgeType.CONTAINS,
            properties=SourceContext.create_edge_traceability(
                source_file_path=file_path,
                derivation_method="mapplet_instance_parsing",
                xml_location=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            )
        )
        edges.append(containment_edge)
        
        # REQUIREMENT 2.1: Find the actual <MAPPLET> definition from the root XML
        mapplet_definition_element = None
        if hasattr(self, '_current_root') and self._current_root is not None:
            # Look for the MAPPLET definition in the root XML
            mapplet_definitions = self._current_root.xpath(f".//MAPPLET[@NAME='{transformation_name}']")
            if mapplet_definitions:
                mapplet_definition_element = mapplet_definitions[0]
                logger.debug(f"Found mapplet definition for '{transformation_name}' in root XML")
            else:
                logger.warning(f"Mapplet definition '{transformation_name}' not found in root XML")
        
        # Now recursively parse the mapplet contents using the actual definition element
        if mapplet_definition_element is not None:
            mapplet_nodes, mapplet_edges = self._parse_mapplet_contents(
                mapplet_definition_element, mapplet_id, mapping_id, file_path, session_context
            )
        else:
            logger.warning(f"Cannot parse mapplet contents for '{transformation_name}' - definition not found")
            mapplet_nodes, mapplet_edges = [], []
        
        nodes.extend(mapplet_nodes)
        edges.extend(mapplet_edges)
        
        return nodes, edges

    def _parse_mapplet_contents(
        self,
        mapplet_element: etree._Element,
        mapplet_id: str,
        parent_mapping_id: str,
        file_path: str,
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Recursively parse all transformations and connectors inside a mapplet definition.
        
        REQUIREMENT 2.1: Correct and Complete Mapplet Parsing Logic
        - Operates like a mini-_parse_mapping function
        - Finds all <INSTANCE> tags within the <MAPPLET> definition
        - Recursively calls _dispatch_transformation_parser for internal transformations
        - Parses all <CONNECTOR> tags within the <MAPPLET> definition
        """
        nodes: List[Node] = []
        edges: List[Edge] = []
        
        mapplet_name = mapplet_element.get("NAME", "Unknown")
        logger.debug(f"Parsing mapplet contents for: {mapplet_name}")
        
        # Get transformation definitions for recursive parsing
        # We need to use the transformation definitions that were parsed earlier
        transformation_definitions = {}
        if hasattr(self, '_current_root') and self._current_root is not None:
            transformation_definitions = self._parse_transformation_definitions(self._current_root, file_path)
        
        # Parse transformation instances within the mapplet (like in _parse_mapping)
        instances = mapplet_element.xpath(".//INSTANCE")
        instance_nodes = {}  # Keep track of instance nodes for connector parsing
        
        logger.debug(f"Found {len(instances)} transformation instances in mapplet {mapplet_name}")
        
        for instance in instances:
            # Recursively call the main transformation parser dispatcher
            instance_nodes_list, instance_edges = self._parse_transformation_instance(
                instance, mapplet_id, file_path, transformation_definitions, session_context
            )
            nodes.extend(instance_nodes_list)
            edges.extend(instance_edges)
            
            # Store the first node for each instance (for connector parsing)
            if instance_nodes_list:
                instance_name = instance.get("INSTANCENAME", "")
                if instance_name:
                    instance_nodes[instance_name] = instance_nodes_list[0]
        
        # Parse connectors (data flow connections) within the mapplet
        connectors = mapplet_element.xpath(".//CONNECTOR")
        logger.debug(f"Found {len(connectors)} connectors in mapplet {mapplet_name}")
        
        for connector in connectors:
            connector_edges = self._parse_connector(
                connector, mapplet_id, file_path, instance_nodes
            )
            edges.extend(connector_edges)
        
        # Handle special mapplet interface transformations
        mapplet_inputs = mapplet_element.xpath(".//INSTANCE[contains(@TRANSFORMATIONTYPE, 'INPUT')]")
        mapplet_outputs = mapplet_element.xpath(".//INSTANCE[contains(@TRANSFORMATIONTYPE, 'OUTPUT')]")
        
        logger.debug(f"Found {len(mapplet_inputs)} input and {len(mapplet_outputs)} output interface transformations")
        
        # The input/output transformations have already been parsed above,
        # but we can add special properties to mark them as interface points
        for interface_instance in mapplet_inputs + mapplet_outputs:
            instance_name = interface_instance.get("INSTANCENAME", "")
            if instance_name in instance_nodes:
                interface_node = instance_nodes[instance_name]
                interface_node.properties["is_mapplet_interface"] = True
                interface_node.properties["interface_type"] = "input" if interface_instance in mapplet_inputs else "output"
        
        logger.info(f"Parsed {len(nodes)} internal components and {len(edges)} edges from mapplet {mapplet_name}")
        return nodes, edges

    def _extract_target_dml_options(self, target_instance: str, mapping_id: str, file_path: str) -> Dict[str, Any]:
        """
        Extract DML options from target instance definition.
        
        REQUIREMENT 2.2: Complete Target DML Option Parsing
        - Finds the <TARGET> definition element corresponding to target_instance name
        - Parses <TABLEATTRIBUTE> tags for true/false DML values
        """
        # Default DML options (fallback if parsing fails)
        dml_options = {
            "insert": True,
            "update_as_update": True,
            "update_as_insert": False,
            "delete": False,
            "truncate_table": False,
            "target_found": False,
            "parsing_successful": False
        }
        
        # Access the root XML to find target definition
        if not hasattr(self, '_current_root') or self._current_root is None:
            logger.warning(f"Cannot extract target DML options for {target_instance}: root XML not available")
            return dml_options
        
        try:
            # Find the TARGET definition that matches the target_instance name
            # Target instances can reference target definitions by transformation name
            target_definitions = self._current_root.xpath(f".//TARGET[@NAME='{target_instance}']")
            
            if not target_definitions:
                # Try looking for target by transformation name in case it's referenced differently
                target_definitions = self._current_root.xpath(f".//TRANSFORMATION[@NAME='{target_instance}'][@TYPE='Target Definition']")
            
            if not target_definitions:
                logger.warning(f"Target definition '{target_instance}' not found in XML")
                return dml_options
            
            target_element = target_definitions[0]
            dml_options["target_found"] = True
            logger.debug(f"Found target definition for '{target_instance}'")
            
            # Parse TABLEATTRIBUTE elements for DML settings
            table_attributes = target_element.xpath(".//TABLEATTRIBUTE")
            
            for attr in table_attributes:
                attr_name = attr.get("NAME", "").lower()
                attr_value = attr.get("VALUE", "NO").upper()
                is_enabled = attr_value == "YES"
                
                # Map Informatica attribute names to our DML options
                if attr_name in ["insert"]:
                    dml_options["insert"] = is_enabled
                elif attr_name in ["update", "update as update"]:
                    dml_options["update_as_update"] = is_enabled
                elif attr_name in ["update as insert"]:
                    dml_options["update_as_insert"] = is_enabled
                elif attr_name in ["delete"]:
                    dml_options["delete"] = is_enabled
                elif attr_name in ["truncate table", "truncate"]:
                    dml_options["truncate_table"] = is_enabled
                
                logger.debug(f"Parsed target attribute: {attr_name} = {attr_value} ({is_enabled})")
            
            dml_options["parsing_successful"] = True
            logger.debug(f"Successfully extracted target DML options for {target_instance}: {dml_options}")
            
        except Exception as e:
            logger.error(f"Error extracting target DML options for {target_instance}: {e}")
            # Return default options on error
        
        return dml_options
    
    def _correlate_update_strategy_with_target(
        self, 
        strategy_operations: List[str], 
        target_dml_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Correlate Update Strategy DD_ operations with target DML capabilities.
        
        REQUIREMENT 2.2: Determine effective DML operations by correlating
        Update Strategy expressions with target instance settings.
        """
        allowed_operations = []
        rejected_operations = []
        correlation_details = []
        
        # Map Update Strategy DD_ operations to target DML settings
        operation_mapping = {
            "DD_INSERT": "insert",
            "DD_UPDATE": "update_as_update", 
            "DD_DELETE": "delete",
            "DD_REJECT": None  # Always allowed as it means skip row
        }
        
        for strategy_op in strategy_operations:
            target_setting = operation_mapping.get(strategy_op)
            
            if strategy_op == "DD_REJECT":
                allowed_operations.append("reject")
                correlation_details.append(f"{strategy_op} -> reject (always allowed)")
            elif target_setting and target_dml_options.get(target_setting, False):
                # Strategy operation is allowed by target
                operation_name = strategy_op.replace("DD_", "").lower()
                allowed_operations.append(operation_name)
                correlation_details.append(f"{strategy_op} -> {operation_name} (allowed by target)")
            elif target_setting:
                # Strategy operation is blocked by target settings
                operation_name = strategy_op.replace("DD_", "").lower()
                rejected_operations.append(operation_name)
                correlation_details.append(f"{strategy_op} -> {operation_name} (blocked by target)")
            else:
                # Unknown operation
                correlation_details.append(f"{strategy_op} -> unknown operation")
        
        # Generate summary
        summary = f"Allows: {allowed_operations}; Rejects: {rejected_operations}"
        
        return {
            "allowed": allowed_operations,
            "rejected": rejected_operations,
            "correlation_details": correlation_details,
            "summary": summary
        }

    def _parse_stored_procedure_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse Stored Procedure transformation instance with dedicated logic.
        
        REQUIREMENT 2.3: Implement Dedicated Stored Procedure Transformation Parser
        - Extracts Stored Procedure Name, Connection Information, Call Text
        - Captures Execution Order (Source Pre-load, Target Post-load, etc.)
        - Models execution order as prominent property for control flow
        """
        nodes: List[Node] = []
        edges: List[Edge] = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:stored_procedure:{instance_name}"
        
        logger.debug(f"Parsing Stored Procedure transformation: {instance_name} -> {transformation_name}")
        
        # Initialize stored procedure properties
        stored_proc_properties = {
            "stored_procedure_name": "",
            "connection_name": "",
            "connection_information": "",
            "call_text": "",
            "execution_order": "Normal",  # Default
            "custom_exec_statement": "",
            "output_ports": [],
            "input_ports": []
        }
        
        # Extract properties from transformation definition
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse TABLEATTRIBUTE elements for stored procedure configuration
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                
                # Extract key stored procedure properties
                if attr_name == "Stored Procedure Name":
                    stored_proc_properties["stored_procedure_name"] = attr_value
                elif attr_name == "Connection Name":
                    stored_proc_properties["connection_name"] = attr_value
                elif attr_name == "Connection Information":
                    stored_proc_properties["connection_information"] = attr_value
                elif attr_name == "Call Text" or attr_name == "Exec Statement":
                    stored_proc_properties["call_text"] = attr_value
                elif attr_name == "Execution Order" or attr_name == "Execute On":
                    stored_proc_properties["execution_order"] = attr_value
                elif attr_name == "Custom Exec Statement":
                    stored_proc_properties["custom_exec_statement"] = attr_value
            
            # Parse GROUP elements for input/output port information
            groups = transformation_element.xpath(".//GROUP")
            for group in groups:
                group_name = group.get("NAME", "")
                group_type = group.get("TYPE", "")
                
                if group_type in ["INPUT", "OUTPUT"]:
                    ports = []
                    port_elements = group.xpath(".//TRANSFORMFIELD")
                    for port in port_elements:
                        port_info = {
                            "name": port.get("NAME", ""),
                            "datatype": port.get("DATATYPE", ""),
                            "precision": port.get("PRECISION", ""),
                            "scale": port.get("SCALE", ""),
                            "expression": port.get("EXPRESSION", "")
                        }
                        ports.append(port_info)
                    
                    if group_type == "INPUT":
                        stored_proc_properties["input_ports"] = ports
                    else:
                        stored_proc_properties["output_ports"] = ports
        
        # Categorize execution order for control flow modeling
        execution_categories = {
            "Source Pre-load": "PRE_SOURCE",
            "Source Post-load": "POST_SOURCE", 
            "Target Pre-load": "PRE_TARGET",
            "Target Post-load": "POST_TARGET",
            "Normal": "NORMAL",
            "Pre-session": "PRE_SESSION",
            "Post-session": "POST_SESSION"
        }
        
        execution_category = execution_categories.get(
            stored_proc_properties["execution_order"], 
            "NORMAL"
        )
        
        # Create the stored procedure operation node
        operation_properties = {
            "name": instance_name,
            "transformation_name": transformation_name,
            "transformation_type": "Stored Procedure",
            "operation_subtype": "STORED_PROCEDURE",
            "technology": "Informatica",
            
            # REQUIREMENT 2.3: Stored procedure specific properties
            **stored_proc_properties,
            
            # CRITICAL: Execution order as prominent property for control flow
            "execution_category": execution_category,
            "controls_execution_flow": execution_category != "NORMAL",
            "execution_priority": self._get_execution_priority(execution_category),
            
            **SourceContext.create_node_traceability(
                source_file_path=file_path,
                source_file_type="informatica_xml",
                xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']",
                parent_package=os.path.basename(file_path).replace('.xml', '')
            )
        }
        
        stored_proc_node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION,
            name=f"StoredProc: {instance_name}",
            properties=operation_properties
        )
        nodes.append(stored_proc_node)
        
        # Create containment edge from mapping to stored procedure
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS,
            properties=SourceContext.create_edge_traceability(
                source_file_path=file_path,
                derivation_method="stored_procedure_parsing",
                xml_location=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            )
        )
        edges.append(containment_edge)
        
        logger.debug(f"Parsed Stored Procedure: {instance_name} ({stored_proc_properties['stored_procedure_name']}) - "
                    f"Execution: {stored_proc_properties['execution_order']} ({execution_category})")
        
        return nodes, edges
    
    def _get_execution_priority(self, execution_category: str) -> int:
        """
        Get numeric execution priority for control flow ordering.
        Lower numbers execute earlier.
        """
        priority_map = {
            "PRE_SESSION": 1,
            "PRE_SOURCE": 2,
            "NORMAL": 5,
            "POST_SOURCE": 8,
            "PRE_TARGET": 9,
            "POST_TARGET": 10,
            "POST_SESSION": 15
        }
        return priority_map.get(execution_category, 5)

    def _is_performance_attribute(self, attr_name: str) -> bool:
        """
        Identify if an attribute name represents a performance tuning setting.
        
        REQUIREMENT 2.4: Model Session-Level Performance Tuning Attributes
        """
        performance_attr_patterns = [
            # Buffer and memory settings
            "dtm buffer size", "default buffer block size", "buffer block size",
            "index cache size", "data cache size", "session buffer",
            
            # Transaction and commit settings
            "commit interval", "commit type", "transaction", "rollback",
            "auto commit", "commit on end of file",
            
            # Parallelism and threading
            "degree of parallelism", "maximum parallelism", "parallel processing",
            "reader thread", "writer thread", "transformation thread",
            
            # Performance optimization
            "optimization", "pushdown optimization", "sorter cache size",
            "aggregator cache size", "joiner cache size", "lookup cache size",
            "distinct cache size",
            
            # Session-level performance controls
            "collect performance data", "session recovery", "session timeout",
            "pre post sql timeout", "constraint based load ordering",
            
            # Database-specific performance settings
            "bulk mode", "bulk loading", "array size", "fetch size",
            "pre sql", "post sql", "truncate table",
            
            # Resource management
            "maximum memory", "minimum memory", "swap space",
            "session log", "session statistics"
        ]
        
        attr_lower = attr_name.lower()
        return any(pattern in attr_lower for pattern in performance_attr_patterns)
    
    def _categorize_performance_attribute(self, attr_name: str) -> str:
        """
        Categorize performance attributes by their functional area.
        
        REQUIREMENT 2.4: Organize performance settings for migration analysis.
        """
        attr_lower = attr_name.lower()
        
        # Memory and buffer management
        if any(term in attr_lower for term in ["buffer", "cache", "memory"]):
            return "memory_buffer"
        
        # Transaction and commit control
        elif any(term in attr_lower for term in ["commit", "transaction", "rollback"]):
            return "transaction_commit"
        
        # Parallelism and threading
        elif any(term in attr_lower for term in ["parallel", "thread", "degree"]):
            return "parallelism_threading"
        
        # SQL optimization and pushdown
        elif any(term in attr_lower for term in ["optimization", "pushdown", "pre sql", "post sql"]):
            return "sql_optimization"
        
        # Database connectivity and I/O
        elif any(term in attr_lower for term in ["bulk", "array size", "fetch"]):
            return "database_io"
        
        # Session management and logging
        elif any(term in attr_lower for term in ["session", "log", "timeout"]):
            return "session_management"
        
        # Recovery and error handling
        elif any(term in attr_lower for term in ["recovery", "error", "constraint"]):
            return "recovery_error_handling"
        
        else:
            return "general_performance"

    def _resolve_unconnected_lookups(
        self, 
        mapping_nodes: List[Node], 
        mapping_id: str, 
        file_path: str
    ) -> List[Edge]:
        """
        Post-processing step to create dependency edges for unconnected lookups.
        
        REQUIREMENT 2.1: Implement Unconnected Lookup Resolution
        - Iterates through Expression transformation nodes
        - Creates CALLS edges to Lookup nodes for unconnected lookup references
        - Ensures data lineage captures unconnected lookup dependencies
        """
        unconnected_lookup_edges = []
        
        # Build a dictionary of available lookup nodes in this mapping
        lookup_nodes = {}
        expression_nodes = []
        
        for node in mapping_nodes:
            if (node.properties.get("transformation_type") == "Lookup" and 
                node.node_id.startswith(mapping_id)):
                # Extract lookup name from node ID (e.g., mapping:m_TEST:lookup:LKP_CUSTOMER -> LKP_CUSTOMER)
                lookup_name = node.node_id.split(":lookup:")[-1]
                lookup_nodes[lookup_name] = node
                logger.debug(f"Available lookup for resolution: {lookup_name} -> {node.node_id}")
            
            elif (node.properties.get("transformation_type") == "Expression" and 
                  "unconnected_lookups" in node.properties):
                expression_nodes.append(node)
        
        # Process each Expression transformation with unconnected lookups
        for expr_node in expression_nodes:
            unconnected_lookups = expr_node.properties.get("unconnected_lookups", [])
            
            for lookup_name in unconnected_lookups:
                if lookup_name in lookup_nodes:
                    # Create CALLS edge from Expression to Lookup
                    lookup_node = lookup_nodes[lookup_name]
                    
                    call_edge = Edge(
                        source_id=expr_node.node_id,
                        target_id=lookup_node.node_id,
                        relation=EdgeType.DEPENDS_ON,  # Using DEPENDS_ON as it's more semantic than CALLS
                        properties={
                            "dependency_type": "unconnected_lookup_call",
                            "lookup_name": lookup_name,
                            "call_pattern": f":LKP.{lookup_name}(...)",
                            "derivation_method": "unconnected_lookup_resolution",
                            **SourceContext.create_edge_traceability(
                                source_file_path=file_path,
                                derivation_method="post_processing_lookup_resolution",
                                xml_location=f"Expression -> Lookup dependency resolution"
                            )
                        }
                    )
                    
                    unconnected_lookup_edges.append(call_edge)
                    logger.debug(f"Created unconnected lookup edge: {expr_node.node_id} DEPENDS_ON {lookup_node.node_id} ({lookup_name})")
                else:
                    # Lookup not found in current mapping - this could be a cross-mapping reference
                    logger.warning(f"Unconnected lookup '{lookup_name}' referenced in {expr_node.node_id} but lookup node not found in mapping {mapping_id}")
        
        logger.debug(f"Resolved {len(unconnected_lookup_edges)} unconnected lookup dependencies in mapping {mapping_id}")
        return unconnected_lookup_edges

    def _parse_variables(
        self,
        context: etree._Element, 
        context_id: str, 
        file_path: str,
        scope: str
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse workflow and mapping variables.
        
        REQUIREMENT 2.2: Parse and Model Workflow and Mapping Variables
        - Searches for <VARIABLE> tags within <WORKFLOW> and <MAPPING> elements
        - Creates VARIABLE nodes with name, datatype, and precision
        - Uses proper node ID scoping (e.g., workflow:my_wf:variable:$$LastUpdateDate)
        
        Args:
            context: The XML element to search for variables (workflow or mapping)
            context_id: The workflow or mapping ID for scoping
            file_path: Source file path for traceability
            scope: Either "workflow" or "mapping"
            
        Returns:
            Tuple of (variable_nodes, variable_edges)
        """
        nodes = []
        edges = []
        
        # Search for all VARIABLE elements within the context
        variables = context.xpath(".//VARIABLE")
        
        if not variables:
            logger.debug(f"No variables found in {scope} {context_id}")
            return nodes, edges
        
        logger.debug(f"Found {len(variables)} variables in {scope} {context_id}")
        
        for variable in variables:
            try:
                # Extract variable properties
                variable_name = variable.get("NAME", "")
                if not variable_name:
                    logger.warning(f"Variable without NAME attribute found in {scope} {context_id}")
                    continue
                
                # Build node ID with proper scoping
                variable_id = f"{context_id}:variable:{variable_name}"
                
                # Extract additional properties
                datatype = variable.get("DATATYPE", "")
                precision = variable.get("PRECISION", "")
                default_value = variable.get("DEFAULTVALUE", "")
                is_user_defined = variable.get("ISUSERDEFINEDHIERARCHY", "NO") == "YES"
                is_persistent = variable.get("ISPERSISTENT", "NO") == "YES"
                
                # Create source context for traceability
                source_context = SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="informatica_xml",
                    line_number=getattr(variable, 'sourceline', None),
                    xml_path=f"//VARIABLE[@NAME='{variable_name}']",
                    technology="Informatica"
                )
                
                # Create variable node
                variable_node = Node(
                    node_id=variable_id,
                    node_type=NodeType.VARIABLE,
                    name=variable_name,
                    properties={
                        "name": variable_name,
                        "datatype": datatype,
                        "precision": precision,
                        "default_value": default_value,
                        "is_user_defined": is_user_defined,
                        "is_persistent": is_persistent,
                        "scope": scope,
                        "context_id": context_id,
                        "source_context": source_context
                    }
                )
                
                nodes.append(variable_node)
                logger.debug(f"Created variable node: {variable_id}")
                
                # REQUIREMENT 2.2: Store variable in context for expression resolution
                self.variables_context[variable_name] = {
                    "name": variable_name,
                    "datatype": datatype,
                    "precision": precision,
                    "default_value": default_value,
                    "is_persistent": is_persistent,
                    "scope": scope,
                    "context_id": context_id,
                    "node_id": variable_id
                }
                
                # Create containment edge from context (workflow/mapping) to variable
                containment_edge = Edge(
                    source_id=context_id,
                    target_id=variable_id,
                    relation=EdgeType.CONTAINS,
                    properties={
                        "relationship": f"{scope}_variable_containment"
                    },
                    source_context=SourceContext.create_node_traceability(
                        source_file_path=file_path,
                        source_file_type="informatica_xml",
                        line_number=getattr(variable, 'sourceline', None),
                        xml_path=f"//VARIABLE[@NAME='{variable_name}']",
                        technology="Informatica"
                    )
                )
                
                edges.append(containment_edge)
                
            except Exception as e:
                logger.error(f"Error parsing variable in {scope} {context_id}: {e}")
                continue
        
        logger.info(f"Successfully parsed {len(nodes)} variables from {scope} {context_id}")
        return nodes, edges

    def _resolve_variable_references(
        self,
        mapping_nodes: List[Node],
        mapping_id: str,
        file_path: str
    ) -> List[Edge]:
        """
        Post-processing step to create dependency edges for variable references.
        
        REQUIREMENT 2.2: Resolve Variable References in Expressions
        - Iterates through Expression transformation nodes with variable references
        - Creates DEPENDS_ON edges from Expression to Variable nodes
        - Ensures data lineage captures variable dependencies
        
        Args:
            mapping_nodes: All nodes parsed from the mapping
            mapping_id: ID of the mapping for scoping
            file_path: Source file path for traceability
            
        Returns:
            List of dependency edges for variable references
        """
        variable_dependency_edges = []
        
        # Separate nodes by type for efficient lookup
        variable_nodes = {}
        expression_nodes = []
        
        for node in mapping_nodes:
            if node.node_type == NodeType.VARIABLE.value:
                variable_name = node.properties.get("name", "")
                if variable_name:
                    variable_nodes[variable_name] = node
                    logger.debug(f"Available variable for resolution: {variable_name} -> {node.node_id}")
            
            elif (node.properties.get("transformation_type") == "Expression" and 
                  "variable_references" in node.properties):
                expression_nodes.append(node)
        
        # Process each Expression transformation with variable references
        for expr_node in expression_nodes:
            variable_references = expr_node.properties.get("variable_references", [])
            
            for variable_name in variable_references:
                if variable_name in variable_nodes:
                    # Create DEPENDS_ON edge from Expression to Variable
                    variable_node = variable_nodes[variable_name]
                    
                    dependency_edge = Edge(
                        source_id=expr_node.node_id,
                        target_id=variable_node.node_id,
                        relation=EdgeType.DEPENDS_ON,
                        properties={
                            "relationship": "variable_reference",
                            "variable_name": variable_name,
                            "expression_context": "transformation_expression"
                        },
                        source_info=SourceContext.create_node_traceability(
                            file_path=file_path,
                            line_number=None,
                            element_name="VARIABLE_REFERENCE",
                            element_attributes={'VARIABLE_NAME': variable_name}
                        )
                    )
                    
                    variable_dependency_edges.append(dependency_edge)
                    logger.debug(f"Created variable dependency edge: {expr_node.node_id} DEPENDS_ON {variable_node.node_id} ({variable_name})")
                else:
                    # Variable not found in current mapping - could be workflow variable or cross-mapping reference
                    logger.warning(f"Variable '{variable_name}' referenced in {expr_node.node_id} but variable node not found in mapping {mapping_id}")
        
        logger.debug(f"Resolved {len(variable_dependency_edges)} variable dependencies in mapping {mapping_id}")
        return variable_dependency_edges

    def _parse_target_load_order(
        self,
        mapping: etree._Element,
        mapping_id: str,
        file_path: str,
        mapping_nodes: List[Node]
    ) -> List[Edge]:
        """
        Parse target load order groups to model intra-mapping load dependencies.
        
        REQUIREMENT 2.3: Model Target Load Order Groups
        - Searches for <TARGETLOADORDER> element within mapping
        - Creates DEPENDS_ON edges between consecutive target instances
        - Ensures data integrity dependencies are maintained in migrated solution
        
        Args:
            mapping: The XML mapping element
            mapping_id: ID of the mapping for scoping
            file_path: Source file path for traceability
            mapping_nodes: All nodes parsed from the mapping
            
        Returns:
            List of target load order dependency edges
        """
        target_load_order_edges = []
        
        # Search for TARGETLOADORDER element
        target_load_orders = mapping.xpath(".//TARGETLOADORDER")
        
        if not target_load_orders:
            logger.debug(f"No target load order found in mapping {mapping_id}")
            return target_load_order_edges
        
        # Create a lookup of target nodes by instance name for quick access
        target_nodes = {}
        for node in mapping_nodes:
            if node.properties.get("transformation_type") == "Target":
                instance_name = node.properties.get("name", "")
                if instance_name:
                    target_nodes[instance_name] = node
                    logger.debug(f"Available target for load order: {instance_name} -> {node.node_id}")
        
        # Process each target load order definition
        for target_load_order in target_load_orders:
            target_instances = target_load_order.xpath(".//TARGETINSTANCE")
            
            if len(target_instances) < 2:
                logger.debug(f"Target load order has fewer than 2 targets, skipping dependency creation")
                continue
            
            logger.debug(f"Found target load order with {len(target_instances)} targets in mapping {mapping_id}")
            
            # Create dependency edges between consecutive targets
            for i in range(len(target_instances) - 1):
                current_target = target_instances[i]
                next_target = target_instances[i + 1]
                
                current_name = current_target.get("NAME", "")
                next_name = next_target.get("NAME", "")
                
                if not current_name or not next_name:
                    logger.warning(f"Target instance without NAME found in load order, skipping")
                    continue
                
                # Find corresponding target nodes
                current_node = target_nodes.get(current_name)
                next_node = target_nodes.get(next_name)
                
                if current_node and next_node:
                    # Create DEPENDS_ON edge: next target depends on current target completing first
                    dependency_edge = Edge(
                        source_id=next_node.node_id,  # Next target depends on...
                        target_id=current_node.node_id,  # Current target completing first
                        relation=EdgeType.DEPENDS_ON,
                        properties={
                            "relationship": "target_load_order",
                            "dependency_type": "intra_mapping_load_sequence",
                            "current_target": current_name,
                            "next_target": next_name,
                            "sequence_position": i + 1,
                            "total_targets": len(target_instances)
                        },
                        source_info=SourceContext.create_node_traceability(
                            file_path=file_path,
                            line_number=getattr(target_load_order, 'sourceline', None),
                            element_name="TARGETLOADORDER",
                            element_attributes={
                                'CURRENT_TARGET': current_name,
                                'NEXT_TARGET': next_name
                            }
                        )
                    )
                    
                    target_load_order_edges.append(dependency_edge)
                    logger.debug(f"Created target load order edge: {next_node.node_id} DEPENDS_ON {current_node.node_id} (load sequence)")
                else:
                    if not current_node:
                        logger.warning(f"Target instance '{current_name}' referenced in load order but target node not found in mapping {mapping_id}")
                    if not next_node:
                        logger.warning(f"Target instance '{next_name}' referenced in load order but target node not found in mapping {mapping_id}")
        
        logger.debug(f"Created {len(target_load_order_edges)} target load order dependencies in mapping {mapping_id}")
        return target_load_order_edges

    # Placeholder methods for other transformation types
    def _parse_filter_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Filter transformation instance."""
        nodes = []
        edges = []
        
        # Handle both TRANSFORMATION and INSTANCE elements
        instance_name = instance.get("INSTANCENAME") or instance.get("NAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME") or instance.get("NAME", "")
        instance_id = f"{mapping_id}:filter:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract filter condition from transformation definition
        filter_condition = ""
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                if attr.get("NAME") == "Filter Condition":
                    filter_condition = attr.get("VALUE", "")
                    break
        
        # Extract SQL semantics from filter condition
        sql_semantics_result = self._extract_sql_semantics(
            filter_condition, 
            f"Filter transformation: {instance_name}"
        )
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Filter",
                "operation_subtype": self._categorize_operation_subtype("Filter"),
                "filter_condition": filter_condition,
                "sql_semantics": sql_semantics_result.get("sql_semantics"),
                "has_sql": sql_semantics_result.get("has_sql", False),
                "sql_type": sql_semantics_result.get("sql_type", "expression"),
                "source_context": source_context,
                "informatica_type": "filter"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges
    
    def _parse_aggregator_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Aggregator transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:aggregator:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract aggregation functions and group by fields
        aggregate_functions = []
        group_by_fields = []
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse transformation fields to identify aggregations and group by
            transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
            for field in transform_fields:
                field_name = field.get("NAME", "")
                port_type = field.get("PORTTYPE", "")
                expression = field.get("EXPRESSION", "")
                
                if port_type == "OUTPUT" and any(func in expression.upper() for func in ["SUM", "COUNT", "AVG", "MIN", "MAX"]):
                    aggregate_functions.append({
                        "field_name": field_name,
                        "expression": expression
                    })
                elif port_type == "GROUP BY":
                    group_by_fields.append(field_name)
        
        # Extract SQL semantics from aggregate expressions
        all_expressions = [agg["expression"] for agg in aggregate_functions] + group_by_fields
        combined_expressions = " | ".join(all_expressions) if all_expressions else ""
        sql_semantics_result = self._extract_sql_semantics(
            combined_expressions, 
            f"Aggregator transformation: {instance_name}"
        )
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Aggregator",
                "operation_subtype": self._categorize_operation_subtype("Aggregator"),
                "aggregate_functions": aggregate_functions,
                "group_by_fields": group_by_fields,
                "sql_semantics": sql_semantics_result.get("sql_semantics"),
                "has_sql": sql_semantics_result.get("has_sql", False),
                "sql_type": sql_semantics_result.get("sql_type", "expression"),
                "source_context": source_context,
                "informatica_type": "aggregator"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges
    
    def _parse_sorter_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Sorter transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:sorter:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract sorting properties from transformation definition
        sort_keys = []
        case_sensitive = False
        distinct = False
        sort_origin = "data"
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse table attributes for sorter properties
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                if attr_name == "Case Sensitive":
                    case_sensitive = attr_value.upper() == "YES"
                elif attr_name == "Distinct":
                    distinct = attr_value.upper() == "YES" 
                elif attr_name == "Sort Origin":
                    sort_origin = attr_value.lower()
            
            # Parse transformation fields to identify sort keys
            transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
            for field in transform_fields:
                field_name = field.get("NAME", "")
                sort_order = field.get("SORTORDER", "")
                if sort_order:
                    sort_keys.append({
                        "field_name": field_name,
                        "sort_order": sort_order,
                        "sort_direction": "ASC" if sort_order == "ASCENDING" else "DESC"
                    })
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Sorter",
                "operation_subtype": self._categorize_operation_subtype("Sorter"),
                "sort_keys": sort_keys,
                "case_sensitive": case_sensitive,
                "distinct": distinct,
                "sort_origin": sort_origin,
                "source_context": source_context,
                "informatica_type": "sorter"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges
    
    def _parse_union_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Union transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:union:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract union properties from transformation definition
        union_groups = []
        union_all = False
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse table attributes for union properties
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                if attr_name == "Union All":
                    union_all = attr_value.upper() == "YES"
            
            # Parse transformation fields to identify union groups
            transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
            for field in transform_fields:
                field_name = field.get("NAME", "")
                group_id = field.get("GROUP", "")
                field_expression = field.get("EXPRESSION", "")
                if group_id:
                    union_groups.append({
                        "field_name": field_name,
                        "group_id": group_id,
                        "expression": field_expression
                    })
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Union",
                "operation_subtype": self._categorize_operation_subtype("Union"),
                "union_groups": union_groups,
                "union_all": union_all,
                "source_context": source_context,
                "informatica_type": "union"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges
    
    def _parse_sequence_generator_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Sequence Generator transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:sequence_generator:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract sequence generator properties
        start_value = ""
        increment_by = ""
        max_value = ""
        cycle = False
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                if attr_name == "Start Value":
                    start_value = attr_value
                elif attr_name == "Increment By":
                    increment_by = attr_value
                elif attr_name == "Maximum Value":
                    max_value = attr_value
                elif attr_name == "Cycle":
                    cycle = attr_value.upper() == "YES"
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Sequence Generator",
                "operation_subtype": self._categorize_operation_subtype("Sequence Generator"),
                "start_value": start_value,
                "increment_by": increment_by,
                "max_value": max_value,
                "cycle": cycle,
                "source_context": source_context,
                "informatica_type": "sequence_generator"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges
    
    def _parse_update_strategy_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Update Strategy transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:update_strategy:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract update strategy properties from transformation definition
        update_strategy_expression = ""
        forward_rejected_rows = False
        treat_source_rows_as = "insert"
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse table attributes for update strategy properties
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                if attr_name == "Update Strategy Expression":
                    update_strategy_expression = attr_value
                elif attr_name == "Forward Rejected Rows":
                    forward_rejected_rows = attr_value.upper() == "YES"
                elif attr_name == "Treat Source Rows As":
                    treat_source_rows_as = attr_value.lower()
        
        # REQUIREMENT 3.4: Parse and correlate Update Strategy expression with target operations
        update_strategy_logic = self._parse_update_strategy_expression(update_strategy_expression)
        
        # Extract session-level "Treat source rows as" setting if available
        session_treat_source_as = session_context.get("general_attributes", {}).get("Treat Source Rows As", treat_source_rows_as)
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Update Strategy",
                "operation_subtype": self._categorize_operation_subtype("Update Strategy"),
                "update_strategy_expression": update_strategy_expression,
                "forward_rejected_rows": forward_rejected_rows,
                "treat_source_rows_as": treat_source_rows_as,
                "session_treat_source_rows_as": session_treat_source_as,
                
                # REQUIREMENT 3.4: Enhanced Update Strategy semantics for migration
                "update_strategy_logic": update_strategy_logic,
                "dml_operations_supported": update_strategy_logic.get("operations", []),
                "has_conditional_logic": update_strategy_logic.get("has_conditions", False),
                "update_strategy_complexity": update_strategy_logic.get("complexity", "simple"),
                
                "source_context": source_context,
                "informatica_type": "update_strategy"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges
    
    def _parse_normalizer_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Normalizer transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:normalizer:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract normalizer properties from transformation definition
        normalize_columns = []
        occurs_clause = ""
        reset_level = ""
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse table attributes for normalizer properties
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                if attr_name == "Occurs Clause":
                    occurs_clause = attr_value
                elif attr_name == "Reset Level":
                    reset_level = attr_value
            
            # Parse transformation fields to identify normalizable columns
            transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
            for field in transform_fields:
                field_name = field.get("NAME", "")
                field_type = field.get("FIELDTYPE", "")
                occurs = field.get("OCCURS", "")
                if field_type == "NORMALIZER" or occurs:
                    normalize_columns.append({
                        "field_name": field_name,
                        "field_type": field_type,
                        "occurs": occurs
                    })
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Normalizer",
                "operation_subtype": self._categorize_operation_subtype("Normalizer"),
                "normalize_columns": normalize_columns,
                "occurs_clause": occurs_clause,
                "reset_level": reset_level,
                "source_context": source_context,
                "informatica_type": "normalizer"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges
    
    def _parse_rank_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Rank transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:rank:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Extract ranking properties
        rank_field = ""
        rank_type = ""
        top_bottom = ""
        group_by_fields = []
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                if attr_name == "Rank Field":
                    rank_field = attr_value
                elif attr_name == "Rank Type":
                    rank_type = attr_value
                elif attr_name == "Top/Bottom":
                    top_bottom = attr_value
            
            # Parse transformation fields to identify group by
            transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
            for field in transform_fields:
                port_type = field.get("PORTTYPE", "")
                if port_type == "GROUP BY":
                    group_by_fields.append(field.get("NAME", ""))
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Rank",
                "operation_subtype": self._categorize_operation_subtype("Rank"),
                "rank_field": rank_field,
                "rank_type": rank_type,
                "top_bottom": top_bottom,
                "group_by_fields": group_by_fields,
                "source_context": source_context,
                "informatica_type": "rank"
            }
        )
        nodes.append(node)
        
        # Create containment edge
        containment_edge = Edge(
            source_id=mapping_id,
            target_id=instance_id,
            relation=EdgeType.CONTAINS.value,
            properties={"source_context": source_context}
        )
        edges.append(containment_edge)
        
        return nodes, edges