"""
Informatica Loader

This module provides the InformaticaLoader class that serves as the ingestion tool
for Informatica projects, following the exact architectural pattern of the SSIS loader.
It discovers and orchestrates the parsing of Informatica assets including workflows,
mappings, and parameter files.
"""

from ..ingestion_tool import IngestionTool
from .informatica_parser import CanonicalInformaticaParser
from typing import Generator, Tuple, List, Dict, Any
from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class InformaticaLoader(IngestionTool):
    """
    An ingestion tool for Informatica PowerCenter projects.
    It discovers and orchestrates the parsing of all relevant Informatica files.
    
    This class mirrors the purpose and structure of SsisLoader but is adapted
    for Informatica's specific file and project conventions.
    """

    def ingest(self) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Discovers and parses all Informatica workflow files in the project directory.
        First parses .par parameter files to build parameter context for enrichment.
        
        Workflow:
        1. Parse parameter files (.par) to build parameters context
        2. Create parameter nodes from parsed parameters
        3. Create parser with parameter context
        4. Discover and parse all workflow files
        5. Yield results for each workflow
        """
        # Parse connection files first to build connection context
        connections_context = self._parse_connection_files()
        if connections_context:
            logger.info(f"Found {len(connections_context)} Informatica connection(s) for enrichment.")

        # Parse parameter files to build parameter context
        parameters_context = self._parse_parameter_files()
        if parameters_context:
            logger.info(f"Found {len(parameters_context)} Informatica parameter(s) for enrichment.")

        # Create connection nodes from parsed connections
        connection_nodes = self._create_connection_nodes_from_context(connections_context)
        if connection_nodes:
            logger.info(f"Created {len(connection_nodes)} Informatica connection node(s).")

        # Create parameter nodes from parsed parameters
        parameter_nodes = self._create_parameter_nodes_from_context(parameters_context)
        if parameter_nodes:
            logger.info(f"Created {len(parameter_nodes)} Informatica parameter node(s).")

        # Create parser with both connection and parameter contexts
        parser = CanonicalInformaticaParser(
            connections_context=connections_context,
            parameters_context=parameters_context,
        )
        
        # Discover workflow files using common Informatica patterns
        workflow_files = self._discover_workflow_files()
        if workflow_files:
            logger.info(f"Found {len(workflow_files)} Informatica workflow file(s).")
        else:
            # Only log at debug level if no Informatica files found
            logger.debug("No Informatica workflow files discovered in the project.")

        # Yield connection and parameter nodes first if any exist
        all_global_nodes = connection_nodes + parameter_nodes
        if all_global_nodes:
            yield all_global_nodes, []

        # Process each workflow file
        for workflow_file in workflow_files:
            try:
                logger.info(f"Processing Informatica workflow: {workflow_file}")
                
                # Find corresponding mapping file
                mapping_file = self._find_mapping_file(workflow_file)
                
                # Parse the workflow and mapping files together
                yield from parser.parse(str(workflow_file), mapping_file)
                
            except Exception as e:
                logger.error(f"Failed to parse {workflow_file}: {e}", exc_info=True)
                continue

    def _parse_parameter_files(self) -> Dict[str, Dict[str, Any]]:
        """
        Discovers and parses .par files to extract Informatica parameters.
        
        Parameter files in Informatica are typically simple key=value text files
        that contain parameter definitions used across workflows and mappings.
        
        Returns a mapping of parameter names to their properties.
        """
        parameters_context = {}
        par_files = self.discover_files("*.par")

        for par_file in par_files:
            try:
                logger.debug(f"Parsing Informatica parameter file: {par_file}")

                with open(par_file, "r", encoding="utf-8") as f:
                    content = f.read()

                # Parse parameter file content (key=value format)
                params = self._parse_parameter_content(content)
                
                # Add file path to each parameter
                for param_name, param_value in params.items():
                    parameters_context[param_name] = {
                        "value": param_value,
                        "file_path": str(par_file),
                        "parameter_type": "informatica_parameter",
                    }

                logger.debug(f"Parsed {len(params)} parameters from: {par_file}")

            except Exception as e:
                logger.error(f"Failed to parse parameter file {par_file}: {e}")
                continue

        return parameters_context

    def _parse_parameter_content(self, content: str) -> Dict[str, str]:
        """
        Parse the content of an Informatica parameter file.
        
        Informatica parameter files typically use key=value format:
        PARAMETER_NAME=parameter_value
        # Comments start with hash
        
        Args:
            content: Raw file content
            
        Returns:
            Dictionary mapping parameter names to values
        """
        parameters = {}
        
        for line in content.split('\n'):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
                
            # Parse key=value pairs
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                # Remove quotes if present
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                    
                parameters[key] = value
        
        return parameters

    def _discover_workflow_files(self) -> List[Path]:
        """
        Discover Informatica workflow XML files using reliable patterns.
        
        Common Informatica workflow file patterns:
        - WorkFlow_*.XML
        - wf_*.xml or wf_*.XML  
        - Files containing 'workflow' in the name
        
        Returns:
            List of discovered workflow file paths
        """
        workflow_files = []
        
        # Primary patterns for Informatica workflow files
        patterns = [
            "WorkFlow_*.XML",
            "wf_*.xml",
            "wf_*.XML",
            "*workflow*.xml",
            "*workflow*.XML"
        ]
        
        for pattern in patterns:
            files = self.discover_files(pattern)
            workflow_files.extend(files)
        
        # Remove duplicates while preserving order
        unique_files = []
        seen = set()
        for file in workflow_files:
            if file not in seen:
                seen.add(file)
                unique_files.append(file)
        
        return unique_files

    def _find_mapping_file(self, workflow_file: Path) -> str:
        """
        Find the corresponding mapping file for a workflow file.
        
        Common Informatica mapping file patterns:
        - WorkFlow_Name.XML -> Mapping_Name.XML
        - wf_name.XML -> m_name.XML
        
        Args:
            workflow_file: Path to the workflow file
            
        Returns:
            Path to the mapping file, or None if not found
        """
        workflow_path = Path(workflow_file)
        directory = workflow_path.parent
        filename = workflow_path.name
        
        # Try common mapping file patterns
        mapping_candidates = []
        
        # Pattern 1: WorkFlow_*.XML -> Mapping_*.XML
        if filename.startswith("WorkFlow_"):
            mapping_name = filename.replace("WorkFlow_", "Mapping_")
            mapping_candidates.append(directory / mapping_name)
        
        # Pattern 2: wf_*.XML -> m_*.XML
        elif filename.lower().startswith("wf_"):
            mapping_name = filename.replace("wf_", "m_", 1)
            if mapping_name == filename:  # Case sensitivity handling
                mapping_name = filename.replace("WF_", "M_", 1)
            mapping_candidates.append(directory / mapping_name)
        
        # Pattern 3: Generic patterns
        base_name = workflow_path.stem
        mapping_candidates.extend([
            directory / f"Mapping_{base_name}.XML",
            directory / f"m_{base_name}.XML",
            directory / f"{base_name}_mapping.XML",
            directory / f"{base_name.replace('workflow', 'mapping')}.XML"
        ])
        
        # Check which mapping file exists
        for candidate in mapping_candidates:
            if candidate.exists():
                logger.info(f"Found mapping file: {candidate}")
                return str(candidate)
        
        # Look for any mapping files in the same directory
        mapping_patterns = ["Mapping_*.XML", "m_*.XML", "*mapping*.XML", "*mapping*.xml"]
        for pattern in mapping_patterns:
            mapping_files = list(directory.glob(pattern))
            if mapping_files:
                # Return the first mapping file found
                logger.info(f"Using mapping file: {mapping_files[0]}")
                return str(mapping_files[0])
        
        logger.warning(f"No mapping file found for workflow: {workflow_file}")
        return None

    def _create_parameter_nodes_from_context(
        self, parameters_context: Dict[str, Dict[str, Any]]
    ) -> List[Node]:
        """
        Creates parameter nodes from the parsed Informatica parameters.
        
        Args:
            parameters_context: Dictionary of parameter data
            
        Returns:
            List of parameter nodes
        """
        parameter_nodes = []
        processed_parameters = set()

        for param_name, param_data in parameters_context.items():
            # Skip if this is a duplicate
            if param_name in processed_parameters:
                continue

            processed_parameters.add(param_name)

            param_id = f"parameter:{param_name}"
            properties = {
                "value": param_data.get("value", ""),
                "file_path": param_data.get("file_path", ""),
                "parameter_type": param_data.get("parameter_type", "informatica_parameter"),
                "technology": "Informatica"
            }

            parameter_nodes.append(
                Node(
                    node_id=param_id,
                    node_type=NodeType.PARAMETER.value,
                    name=param_name,
                    properties=properties,
                )
            )

        return parameter_nodes

    def _parse_connection_files(self) -> Dict[str, Dict[str, Any]]:
        """
        Discovers and parses Informatica connection files to extract connection context.
        
        Informatica connections can be stored in various formats:
        - .con files: Connection manager files
        - .cnx files: Connection metadata files
        - Embedded in workflow/mapping XML files
        
        Returns a mapping of connection names to their properties.
        """
        connections_context = {}
        
        # Discover connection files using common patterns
        connection_patterns = ["*.con", "*.cnx", "*.connection"]
        connection_files = []
        
        for pattern in connection_patterns:
            files = self.discover_files(pattern)
            connection_files.extend(files)
        
        for connection_file in connection_files:
            try:
                logger.debug(f"Parsing Informatica connection file: {connection_file}")
                
                with open(connection_file, "r", encoding="utf-8") as f:
                    content = f.read()
                
                # Parse connection file content
                connections = self._parse_connection_content(content, str(connection_file))
                
                # Add to context
                for conn_name, conn_data in connections.items():
                    connections_context[conn_name] = conn_data
                
                logger.debug(f"Parsed {len(connections)} connections from: {connection_file}")
                
            except Exception as e:
                logger.error(f"Failed to parse connection file {connection_file}: {e}")
                continue
        
        return connections_context

    def _parse_connection_content(self, content: str, file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Parse the content of an Informatica connection file.
        
        Informatica connection files can be in various formats:
        - XML format for newer versions
        - Key-value format for parameter-style connections
        - Binary/proprietary format (skip these)
        
        Args:
            content: Raw file content
            file_path: Path to the connection file
            
        Returns:
            Dictionary mapping connection names to connection properties
        """
        connections = {}
        
        # Try to parse as XML first (most common format)
        if content.strip().startswith('<?xml') or '<' in content:
            connections.update(self._parse_xml_connection_content(content, file_path))
        else:
            # Try to parse as key-value format
            connections.update(self._parse_keyvalue_connection_content(content, file_path))
        
        return connections

    def _parse_xml_connection_content(self, content: str, file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Parse XML-formatted Informatica connection files.
        
        Args:
            content: XML content
            file_path: Path to the connection file
            
        Returns:
            Dictionary of connection data
        """
        connections = {}
        
        try:
            from lxml import etree
            
            # Parse XML content
            root = etree.fromstring(content.encode('utf-8'))
            
            # Look for connection elements (common patterns)
            connection_elements = (
                root.xpath("//CONNECTION") + 
                root.xpath("//Connection") + 
                root.xpath("//connection") +
                root.xpath("//CONNECTIONINFO") +
                root.xpath("//connectioninfo")
            )
            
            for conn_elem in connection_elements:
                conn_name = (
                    conn_elem.get("NAME") or 
                    conn_elem.get("name") or 
                    conn_elem.get("ConnectionName") or 
                    f"connection_{len(connections) + 1}"
                )
                
                # Extract connection properties
                conn_data = {
                    "name": conn_name,
                    "file_path": file_path,
                    "connection_type": conn_elem.get("TYPE") or conn_elem.get("type") or "unknown",
                    "server": conn_elem.get("SERVER") or conn_elem.get("server"),
                    "database": conn_elem.get("DATABASE") or conn_elem.get("database"),
                    "username": conn_elem.get("USERNAME") or conn_elem.get("username"),
                    "port": conn_elem.get("PORT") or conn_elem.get("port"),
                    "technology": "Informatica"
                }
                
                # Remove None values
                conn_data = {k: v for k, v in conn_data.items() if v is not None}
                
                connections[conn_name] = conn_data
                
        except Exception as e:
            logger.debug(f"Failed to parse XML connection content: {e}")
            # Fall back to basic parsing
            pass
        
        return connections

    def _parse_keyvalue_connection_content(self, content: str, file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Parse key-value formatted connection files.
        
        Args:
            content: Key-value content
            file_path: Path to the connection file
            
        Returns:
            Dictionary of connection data
        """
        connections = {}
        current_connection = {}
        connection_name = None
        
        for line in content.split('\n'):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Parse key=value pairs
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip().upper()
                value = value.strip().strip('"\'')
                
                # Connection name detection
                if key in ['CONNECTION_NAME', 'NAME', 'CONN_NAME']:
                    # Save previous connection if exists
                    if connection_name and current_connection:
                        connections[connection_name] = current_connection
                    
                    # Start new connection
                    connection_name = value
                    current_connection = {
                        "name": connection_name,
                        "file_path": file_path,
                        "technology": "Informatica"
                    }
                elif connection_name:
                    # Add property to current connection
                    if key in ['SERVER', 'HOST']:
                        current_connection["server"] = value
                    elif key in ['DATABASE', 'DB_NAME']:
                        current_connection["database"] = value
                    elif key in ['USERNAME', 'USER']:
                        current_connection["username"] = value
                    elif key in ['PORT']:
                        current_connection["port"] = value
                    elif key in ['TYPE', 'CONNECTION_TYPE']:
                        current_connection["connection_type"] = value
                    else:
                        current_connection[key.lower()] = value
        
        # Save the last connection
        if connection_name and current_connection:
            connections[connection_name] = current_connection
        
        # If no named connections found, create a default one from all properties
        if not connections and any('=' in line for line in content.split('\n')):
            default_conn = {"name": "default_connection", "file_path": file_path, "technology": "Informatica"}
            
            for line in content.split('\n'):
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    key = key.strip().upper()
                    value = value.strip().strip('"\'')
                    
                    if key in ['SERVER', 'HOST']:
                        default_conn["server"] = value
                    elif key in ['DATABASE', 'DB_NAME']:
                        default_conn["database"] = value
                    elif key in ['USERNAME', 'USER']:
                        default_conn["username"] = value
                    elif key in ['PORT']:
                        default_conn["port"] = value
                    elif key in ['TYPE', 'CONNECTION_TYPE']:
                        default_conn["connection_type"] = value
            
            if len(default_conn) > 3:  # More than just name, file_path, technology
                connections["default_connection"] = default_conn
        
        return connections

    def _create_connection_nodes_from_context(
        self, connections_context: Dict[str, Dict[str, Any]]
    ) -> List[Node]:
        """
        Creates connection nodes from the parsed Informatica connections.
        
        Args:
            connections_context: Dictionary of connection data
            
        Returns:
            List of connection nodes
        """
        connection_nodes = []
        processed_connections = set()

        for conn_name, conn_data in connections_context.items():
            # Skip if this is a duplicate
            if conn_name in processed_connections:
                continue

            processed_connections.add(conn_name)

            conn_id = f"connection:{conn_name}"
            properties = {
                "name": conn_data.get("name", conn_name),
                "connection_type": conn_data.get("connection_type", "unknown"),
                "server": conn_data.get("server", ""),
                "database": conn_data.get("database", ""),
                "username": conn_data.get("username", ""),
                "port": conn_data.get("port", ""),
                "file_path": conn_data.get("file_path", ""),
                "technology": "Informatica"
            }

            # Remove empty values
            properties = {k: v for k, v in properties.items() if v}

            connection_nodes.append(
                Node(
                    node_id=conn_id,
                    node_type=NodeType.CONNECTION.value,
                    name=conn_name,
                    properties=properties,
                )
            )

        return connection_nodes