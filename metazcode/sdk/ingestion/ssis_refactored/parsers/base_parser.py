"""
Base Parser for SSIS Components

Provides common functionality for all component parsers, reducing duplication
and enforcing consistent parsing patterns.
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional
from lxml import etree
import logging

from ..models.parsing_context import ParsingContext
from ..builders.graph_builder import GraphBuilder

logger = logging.getLogger(__name__)


class BaseComponentParser(ABC):
    """
    Abstract base class for SSIS component parsers.

    Provides:
    - Common XML namespace handling
    - Standard XML extraction methods
    - Context and builder access
    - Consistent logging

    Subclasses implement component-specific parsing logic.
    """

    # XML namespaces used by SSIS
    NS_MAP = {
        "DTS": "www.microsoft.com/SqlServer/Dts",
        "SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask",
    }

    def __init__(self, context: ParsingContext, builder: GraphBuilder):
        """
        Initialize the component parser.

        Args:
            context: Parsing context with state and collections
            builder: Graph builder for creating nodes/edges
        """
        self.context = context
        self.builder = builder
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def parse(self, component_xml: etree._Element, task_id: str) -> None:
        """
        Parse an SSIS component and add nodes/edges to the graph.

        Args:
            component_xml: XML element representing the component
            task_id: Parent task ID

        Note:
            Must be implemented by subclasses.
            Should add nodes/edges via self.builder.
        """
        pass

    # ==================== XML Extraction Utilities ====================

    def get_xml_property(
        self,
        element: etree._Element,
        property_name: str,
        default: Optional[str] = None,
    ) -> Optional[str]:
        """
        Extract a property value from DTS:Property elements.

        Args:
            element: Parent XML element
            property_name: Name of the property to find
            default: Default value if not found

        Returns:
            Property text value or default
        """
        prop = element.find(
            f"DTS:Property[@DTS:Name='{property_name}']",
            self.NS_MAP
        )
        return prop.text if prop is not None and prop.text else default

    def get_xml_attribute(
        self,
        element: etree._Element,
        attr_name: str,
        namespace: str = "DTS",
        default: Optional[str] = None,
    ) -> Optional[str]:
        """
        Extract an attribute value from an XML element.

        Args:
            element: XML element
            attr_name: Attribute name
            namespace: Namespace prefix (default: DTS)
            default: Default value if not found

        Returns:
            Attribute value or default
        """
        full_attr = f"{{{self.NS_MAP.get(namespace, '')}}}{attr_name}"
        return element.get(full_attr, default)

    def find_connection(
        self,
        component_xml: etree._Element,
        connection_type: str = "connection",
    ) -> Optional[etree._Element]:
        """
        Find a connection element within a component.

        Args:
            component_xml: Component XML element
            connection_type: Type of connection to find

        Returns:
            Connection XML element or None
        """
        connections = component_xml.find("connections")
        if connections is None:
            return None

        return connections.find(connection_type)

    def extract_columns(
        self,
        component_xml: etree._Element,
    ) -> list:
        """
        Extract column metadata from component outputs/inputs.

        Args:
            component_xml: Component XML element

        Returns:
            List of column dictionaries with metadata
        """
        columns = []

        # Extract from outputs
        outputs = component_xml.find("outputs")
        if outputs is not None:
            for output in outputs.findall("output"):
                output_columns = output.find("outputColumns")
                if output_columns is not None:
                    for col in output_columns.findall("outputColumn"):
                        col_data = self._extract_column_metadata(col)
                        if col_data:
                            columns.append(col_data)

        return columns

    def _extract_column_metadata(self, column_xml: etree._Element) -> Optional[Dict]:
        """
        Extract metadata from a single column element.

        Args:
            column_xml: Column XML element

        Returns:
            Dictionary with column metadata or None
        """
        col_id = column_xml.get("id")
        col_name = column_xml.get("name")

        if not col_name:
            return None

        metadata = {
            "id": col_id,
            "name": col_name,
            "data_type": column_xml.get("dataType"),
            "length": column_xml.get("length"),
            "precision": column_xml.get("precision"),
            "scale": column_xml.get("scale"),
            "code_page": column_xml.get("codePage"),
        }

        # Remove None values
        return {k: v for k, v in metadata.items() if v is not None}

    def resolve_connection_id(self, connection_guid: str) -> Optional[str]:
        """
        Resolve a connection GUID to its canonical node ID.

        Args:
            connection_guid: Connection GUID from SSIS XML

        Returns:
            Canonical node ID or None if not found
        """
        node_id = self.context.get_connection_id(connection_guid)
        if node_id is None:
            self.logger.warning(
                f"Connection GUID not found in registry: {connection_guid}"
            )
        return node_id

    def categorize_operation_subtype(self, native_type: str) -> str:
        """
        Categorize SSIS operations into standardized subtypes.

        Args:
            native_type: The SSIS native operation type

        Returns:
            Standardized operation subtype
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

        # Default fallback
        else:
            self.logger.warning(
                f"Unknown operation native_type '{native_type}', defaulting to 'EXECUTE'"
            )
            return "EXECUTE"

    # ==================== Logging Helpers ====================

    def log_parsing_start(self, component_name: str, component_type: str) -> None:
        """Log the start of component parsing."""
        self.logger.debug(
            f"Parsing {component_type}: {component_name} (task_id: {self.context.current_task_id})"
        )

    def log_parsing_complete(self, component_name: str, nodes_added: int, edges_added: int) -> None:
        """Log the completion of component parsing."""
        self.logger.debug(
            f"Completed parsing {component_name}: +{nodes_added} nodes, +{edges_added} edges"
        )

    def log_error(self, component_name: str, error: Exception) -> None:
        """Log a parsing error."""
        self.logger.error(
            f"Error parsing component {component_name}: {error}",
            exc_info=True
        )
