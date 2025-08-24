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
from ..ssis.sql_semantics import EnhancedSqlParser, SqlSemantics, create_join_edges_from_semantics

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
        enable_schema_introspection: bool = True,
        enable_type_mapping: bool = True,
        target_platforms: Optional[List[str]] = None,
    ):
        """
        Initialize the Informatica parser.
        
        Args:
            connections_context: Pre-loaded connection information
            parameters_context: Pre-loaded parameter information
            enable_schema_introspection: Whether to enable database schema introspection
            enable_type_mapping: Whether to enable data type mapping
            target_platforms: List of target platforms for type mapping
        """
        self.connections_context = connections_context or {}
        self.parameters_context = parameters_context or {}
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

    def _parse_target_platforms(self, platforms: List[str]) -> List[TargetPlatform]:
        """Parse string platform names to TargetPlatform enums."""
        parsed = []
        for platform in platforms:
            try:
                parsed.append(TargetPlatform(platform))
            except ValueError:
                logger.warning(f"Unknown target platform: {platform}")
        return parsed


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
        elif transformation_type in ["Session", "Worklet", "Assignment", "Command", "Timer", "Event-Wait"]:
            return "CONTROL_FLOW"
        
        # Execute operations
        elif transformation_type in ["Command", "Email"]:
            return "EXECUTE"
            
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
        
        # Parse workflows first to extract session information
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
        
        # Parse workflow links (execution order)
        workflow_links = workflow.xpath(".//WORKFLOWLINK")
        for link in workflow_links:
            link_edges = self._parse_workflow_link(link, workflow_id, file_path)
            edges.extend(link_edges)
        
        # Parse sessions for connection information
        sessions = workflow.xpath(".//SESSION")
        for session in sessions:
            self._extract_session_connections(session, file_path)
        
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
        task_type = task_instance.get("TYPE", "UnknownType")
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
        
        task_node = Node(
            node_id=task_id,
            node_type=node_type.value,
            name=task_name,
            properties={
                "name": task_name,
                "task_type": task_type,
                "operation_subtype": operation_subtype,
                "description": task_instance.get("DESCRIPTION", ""),
                "is_reusable": task_instance.get("REUSABLE", "NO") == "YES",
                "source_context": source_context,
                "informatica_type": "task_instance"
            }
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
        Extract connection information from session definitions for later use in mapping parsing.
        """
        session_name = session.get("NAME", "")
        mapping_name = session.get("MAPPINGNAME", "")
        
        connections = {}
        
        # Extract connection references from session extensions
        session_extensions = session.xpath(".//SESSIONEXTENSION")
        for ext in session_extensions:
            conn_refs = ext.xpath(".//CONNECTIONREFERENCE")
            for conn_ref in conn_refs:
                instance_name = conn_ref.get("INSTANCENAME", "")
                connection_name = conn_ref.get("CONNECTIONNAME", "")
                if instance_name and connection_name:
                    connections[instance_name] = connection_name
        
        # Store session connection mapping
        if mapping_name:
            self.session_connections[mapping_name] = {
                "session_name": session_name,
                "connections": connections
            }

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
                instance_name = instance.get("INSTANCENAME", "")
                if instance_name:
                    instance_nodes[instance_name] = instance_nodes_list[0]
        
        # Parse connectors (data flow connections)
        connectors = mapping.xpath(".//CONNECTOR")
        for connector in connectors:
            connector_edges = self._parse_connector(
                connector, mapping_id, file_path, instance_nodes
            )
            edges.extend(connector_edges)
        
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
        instance_name = instance.get("INSTANCENAME", "UnknownInstance")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        transformation_type = instance.get("TRANSFORMATIONTYPE", "")
        
        # Get transformation definition if available
        transformation_def = transformation_definitions.get(transformation_name, {})
        if not transformation_def:
            # For built-in transformations like Source Qualifier, use instance info
            transformation_def = {
                "name": transformation_name,
                "type": transformation_type,
                "description": "",
                "is_reusable": False
            }
        
        # Dispatch to specific transformation parser based on type
        return self._dispatch_transformation_parser(
            instance, mapping_id, file_path, transformation_def, session_context
        )

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
        """
        transformation_type = transformation_def.get("type", "").lower()
        
        # Map transformation types to parser methods
        parser_map = {
            "source qualifier": self._parse_source_qualifier_transformation,
            "target definition": self._parse_target_transformation,
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
            "rank": self._parse_rank_transformation
        }
        
        parser_method = parser_map.get(transformation_type)
        if parser_method:
            return parser_method(instance, mapping_id, file_path, transformation_def, session_context)
        else:
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
        """
        edges = []
        
        from_instance = connector.get("FROMINSTANCE", "")
        to_instance = connector.get("TOINSTANCE", "")
        from_instancetype = connector.get("FROMINSTANCETYPE", "")
        to_instancetype = connector.get("TOINSTANCETYPE", "")
        
        if from_instance and to_instance:
            from_node = instance_nodes.get(from_instance)
            to_node = instance_nodes.get(to_instance)
            
            if from_node and to_node:
                source_context = SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    line_number=connector.sourceline or 0,
                    source_file_type="xml",
                    xml_path=f"//CONNECTOR[@FROMINSTANCE='{from_instance}'][@TOINSTANCE='{to_instance}']"
                    ,
            technology="Informatica"
        )
                
                # Handle target definitions specially to write to DATA_ASSET nodes
                if to_instancetype == "Target Definition":
                    # Create WRITES_TO edge to the actual DATA_ASSET node for the target
                    target_data_asset_id = f"data_asset:target:{to_instance}"
                    
                    connector_edge = Edge(
                        source_id=from_node.node_id,
                        target_id=target_data_asset_id,
                        relation=EdgeType.WRITES_TO.value,
                        properties={
                            "connector_type": "data_flow",
                            "from_instance_type": from_instancetype,
                            "to_instance_type": to_instancetype,
                            "target_instance_name": to_instance,
                            "source_context": source_context
                        }
                    )
                    edges.append(connector_edge)
                else:
                    # Determine edge type based on instance types
                    if from_instancetype == "SOURCE" or from_instancetype.startswith("Source"):
                        edge_type = EdgeType.READS_FROM
                    elif to_instancetype == "TARGET" or to_instancetype.startswith("Target"):
                        edge_type = EdgeType.WRITES_TO
                    else:
                        edge_type = EdgeType.DEPENDS_ON
                    
                    connector_edge = Edge(
                        source_id=from_node.node_id,
                        target_id=to_node.node_id,
                        relation=edge_type.value,
                        properties={
                            "connector_type": "data_flow",
                            "from_instance_type": from_instancetype,
                            "to_instance_type": to_instancetype,
                            "source_context": source_context
                        }
                    )
                    edges.append(connector_edge)
        
        return edges

    # Specific transformation parsers following SSIS parser patterns

    def _parse_source_qualifier_transformation(
        self,
        instance: etree._Element,
        mapping_id: str,
        file_path: str,
        transformation_def: Dict[str, Any],
        session_context: Dict[str, Any]
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse Source Qualifier transformation instance."""
        nodes = []
        edges = []
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        instance_id = f"{mapping_id}:source_qualifier:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        # Get transformation definition for more details
        transformation_element = transformation_def.get("element")
        sql_query = ""
        associated_source = ""
        
        if transformation_element is not None:
            # Extract SQL query from transformation
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                if attr.get("NAME") == "Sql Query":
                    sql_query = attr.get("VALUE", "")
                    break
            
            # Find associated source instance
            associated_source_elements = transformation_element.xpath(".//ASSOCIATED_SOURCE_INSTANCE")
            if associated_source_elements:
                associated_source = associated_source_elements[0].text or ""
        
        # Parse SQL semantics using the enhanced SQL parser
        sql_semantics = self.sql_parser.parse_sql_semantics(sql_query)
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Source Qualifier",
                "operation_subtype": self._categorize_operation_subtype("Source Qualifier"),
                "sql_query": sql_query,
                "sql_semantics": sql_semantics.to_dict(),
                "associated_source": associated_source,
                "source_context": source_context,
                "informatica_type": "source_qualifier"
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
        
        # If associated with a source, create READS_FROM edge
        if associated_source:
            # Reference the actual DATA_ASSET node created for this source
            source_data_asset_id = f"data_asset:source:{associated_source}"
            reads_from_edge = Edge(
                source_id=instance_id,
                target_id=source_data_asset_id,
                relation=EdgeType.READS_FROM.value,
                properties={
                    "relationship": "source_qualifier_reads_from_source",
                    "source_context": source_context
                }
            )
            edges.append(reads_from_edge)
        
        return nodes, edges

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
        This is a passthrough method. The instance itself does not become a node;
        the final WRITES_TO edge is created by the _parse_connector method.
        """
        return [], []

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
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
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
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse transformation fields for expressions
            transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
            for field in transform_fields:
                field_name = field.get("NAME", "")
                expression = field.get("EXPRESSION", "")
                if expression:
                    expressions[field_name] = expression
                    
                    # Check for unconnected lookup calls in expression
                    lookup_pattern = r':LKP\.(\w+)\('
                    lookup_matches = re.findall(lookup_pattern, expression)
                    for lookup_name in lookup_matches:
                        if lookup_name not in unconnected_lookups:
                            unconnected_lookups.append(lookup_name)
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Expression",
                "operation_subtype": self._categorize_operation_subtype("Expression"),
                "expressions": expressions,
                "unconnected_lookups": unconnected_lookups,
                "source_context": source_context,
                "informatica_type": "expression"
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
        
        # Create edges for unconnected lookups
        for lookup_name in unconnected_lookups:
            lookup_id = f"{mapping_id}:lookup:{lookup_name}"
            lookup_edge = Edge(
                source_id=instance_id,
                target_id=lookup_id,
                relation=EdgeType.DEPENDS_ON.value,
                properties={
                    "relationship": "unconnected_lookup_call",
                    "lookup_name": lookup_name,
                    "source_context": source_context
                }
            )
            edges.append(lookup_edge)
        
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
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
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
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Joiner",
                "operation_subtype": self._categorize_operation_subtype("Joiner"),
                "join_condition": join_condition,
                "join_type": join_type,
                "master_source": master_source,
                "detail_source": detail_source,
                "source_context": source_context,
                "informatica_type": "joiner"
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
        
        # Extract lookup properties
        lookup_source = ""
        lookup_condition = ""
        is_connected = True  # Default assumption
        
        transformation_element = transformation_def.get("element")
        if transformation_element is not None:
            # Parse table attributes for lookup properties
            table_attributes = transformation_element.xpath(".//TABLEATTRIBUTE")
            for attr in table_attributes:
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                if attr_name == "Lookup Source Database":
                    lookup_source = attr_value
                elif attr_name == "Lookup Condition":
                    lookup_condition = attr_value
        
        # Get connection information for lookup (may use database connections)
        connection_name = session_context.get("connections", {}).get(instance_name, "")
        connection_details = self.connections_context.get(connection_name, {})
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
                "transformation_name": transformation_name,
                "transformation_type": "Lookup",
                "operation_subtype": self._categorize_operation_subtype("Lookup"),
                "lookup_source": lookup_source,
                "lookup_condition": lookup_condition,
                "is_connected": is_connected,
                "connection_name": connection_name,
                "connection_details": connection_details,
                "server": connection_details.get('server'),
                "database": connection_details.get('database'),
                "provider": connection_details.get('provider'),
                "source_context": source_context,
                "informatica_type": "lookup"
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
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
        transformation_type = transformation_def.get("type", "Unknown")
        instance_id = f"{mapping_id}:transformation:{instance_name}"
        
        source_context = SourceContext.create_node_traceability(
            source_file_path=file_path,
            line_number=instance.sourceline or 0,
            source_file_type="xml",
            xml_path=f"//INSTANCE[@INSTANCENAME='{instance_name}']"
            ,
            technology="Informatica"
        )
        
        node = Node(
            node_id=instance_id,
            node_type=NodeType.OPERATION.value,
            name=instance_name,
            properties={
                "name": instance_name,
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
        
        instance_name = instance.get("INSTANCENAME", "")
        transformation_name = instance.get("TRANSFORMATIONNAME", "")
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