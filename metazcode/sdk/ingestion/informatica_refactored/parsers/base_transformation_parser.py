"""
Base Transformation Parser for Informatica

Provides common functionality for all transformation parsers, reducing the
60-70% code duplication found in the 18 transformation parser methods.

Legacy Issue:
- 18 transformation parser methods with nearly identical structure
- 60-70% duplicate boilerplate code
- No reuse between parsers

Refactored Solution:
- Template Method pattern
- Common extraction logic in base class
- Subclasses implement transformation-specific logic
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any
from lxml import etree
import logging

from ..models.parsing_context import InformaticaParsingContext
from ..builders.graph_builder import InformaticaGraphBuilder
from ....models.graph import Node, Edge

logger = logging.getLogger(__name__)


class BaseTransformationParser(ABC):
    """
    Abstract base class for Informatica transformation parsers.

    Provides:
    - Common instance name/ID extraction
    - Standard node/edge creation patterns
    - Field extraction utilities
    - Template method for parsing flow

    Subclasses implement transformation-specific logic.
    """

    def __init__(self, context: InformaticaParsingContext, builder: InformaticaGraphBuilder):
        """
        Initialize the transformation parser.

        Args:
            context: Parsing context with state
            builder: Graph builder for creating nodes/edges
        """
        self.context = context
        self.builder = builder
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def get_transformation_type(self) -> str:
        """
        Get the transformation type name.

        Returns:
            Transformation type (e.g., "Expression", "Joiner", "Lookup")
        """
        pass

    def parse(
        self,
        instance: etree._Element,
        mapping_id: str,
        transformation_def: Dict[str, any],
        session_context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse a transformation instance (Template Method).

        Args:
            instance: XML element for transformation instance
            mapping_id: Parent mapping ID
            transformation_def: Transformation definition dictionary
            session_context: Optional session context for overrides

        Returns:
            Tuple of (nodes, edges) created
        """
        nodes: List[Node] = []
        edges: List[Edge] = []

        # Step 1: Extract instance metadata (common)
        instance_name = self._extract_instance_name(instance)
        transformation_name = self._extract_transformation_name(instance)
        instance_id = self._create_instance_id(mapping_id, instance_name)

        self.logger.debug(
            f"Parsing {self.get_transformation_type()}: {instance_name} "
            f"(transformation: {transformation_name})"
        )

        # Step 2: Extract transformation-specific properties
        transformation_element = transformation_def.get("element")
        if transformation_element is None:
            self.logger.warning(
                f"No transformation element found for {instance_name}"
            )
            return nodes, edges

        # Step 3: Parse transformation-specific logic (subclass implements)
        properties = self.parse_transformation_properties(
            transformation_element, instance, session_context
        )

        # Step 4: Create transformation node (common)
        node = self.builder.create_transformation_node(
            instance_name=instance_name,
            transformation_type=self.get_transformation_type(),
            mapping_id=mapping_id,
            properties=properties,
        )
        nodes.append(node)

        # Step 5: Create containment edge (common)
        edge = self.builder.create_contains_edge(
            parent_id=mapping_id,
            child_id=instance_id,
        )
        edges.append(edge)

        # Step 6: Register transformation in context
        self.context.register_instance(instance_name, instance_id)
        if transformation_name:
            self.context.register_transformation(transformation_name, instance_id)

        return nodes, edges

    @abstractmethod
    def parse_transformation_properties(
        self,
        transformation_element: etree._Element,
        instance: etree._Element,
        session_context: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Parse transformation-specific properties.

        Subclasses implement this method to extract properties specific
        to their transformation type.

        Args:
            transformation_element: XML element with transformation definition
            instance: XML element with instance information
            session_context: Optional session context

        Returns:
            Dictionary of properties to attach to the node
        """
        pass

    # ==================== Common Extraction Methods ====================

    def _extract_instance_name(self, instance: etree._Element) -> str:
        """Extract instance name from XML."""
        return instance.get("INSTANCENAME") or instance.get("NAME", "")

    def _extract_transformation_name(self, instance: etree._Element) -> str:
        """Extract transformation name from XML."""
        return instance.get("TRANSFORMATIONNAME") or instance.get("NAME", "")

    def _create_instance_id(self, mapping_id: str, instance_name: str) -> str:
        """Create instance ID."""
        trans_type = self.get_transformation_type().replace(" ", "_").lower()
        return f"{mapping_id}:{trans_type}:{instance_name}"

    def extract_fields(self, transformation_element: etree._Element) -> List[Dict[str, Any]]:
        """
        Extract transformation fields from XML.

        Args:
            transformation_element: XML element with transformation definition

        Returns:
            List of field dictionaries
        """
        fields = []

        transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
        for field in transform_fields:
            field_data = {
                "name": field.get("NAME", ""),
                "datatype": field.get("DATATYPE", ""),
                "precision": field.get("PRECISION", ""),
                "scale": field.get("SCALE", ""),
                "field_type": field.get("PORTTYPE", ""),
                "expression": field.get("EXPRESSION", ""),
                "default_value": field.get("DEFAULTVALUE", ""),
            }
            # Remove empty values
            field_data = {k: v for k, v in field_data.items() if v}
            fields.append(field_data)

        return fields

    def extract_groups(self, transformation_element: etree._Element) -> List[Dict[str, Any]]:
        """
        Extract transformation groups from XML.

        Args:
            transformation_element: XML element with transformation definition

        Returns:
            List of group dictionaries
        """
        groups = []

        group_elements = transformation_element.xpath(".//TABLEATTRIBUTE[@NAME='Group Information']/TABLEATTRIBUTE")
        for group in group_elements:
            group_data = {
                "name": group.get("NAME", ""),
                "order": group.get("ORDER", ""),
                "group_by_columns": [],
            }

            # Extract group by columns
            for attr in group.findall("ATTRIBUTE"):
                if attr.get("NAME") == "GROUP BY PORTS":
                    group_data["group_by_columns"] = attr.get("VALUE", "").split(",")

            groups.append(group_data)

        return groups

    def extract_table_attribute(
        self,
        transformation_element: etree._Element,
        attribute_name: str,
    ) -> Optional[str]:
        """
        Extract a table attribute value.

        Args:
            transformation_element: XML element
            attribute_name: Name of the attribute

        Returns:
            Attribute value or None
        """
        attr = transformation_element.find(
            f".//TABLEATTRIBUTE[@NAME='{attribute_name}']"
        )
        if attr is not None:
            return attr.get("VALUE")
        return None

    def extract_transformation_attribute(
        self,
        transformation_element: etree._Element,
        attribute_name: str,
    ) -> Optional[str]:
        """
        Extract a transformation attribute value.

        Args:
            transformation_element: XML element
            attribute_name: Name of the attribute

        Returns:
            Attribute value or None
        """
        attr = transformation_element.find(
            f".//ATTRIBUTE[@NAME='{attribute_name}']"
        )
        if attr is not None:
            return attr.get("VALUE")
        return None

    # ==================== Logging Helpers ====================

    def log_parsing_start(self, instance_name: str) -> None:
        """Log the start of parsing."""
        self.logger.debug(
            f"Parsing {self.get_transformation_type()}: {instance_name}"
        )

    def log_parsing_complete(self, instance_name: str, nodes_added: int, edges_added: int) -> None:
        """Log the completion of parsing."""
        self.logger.debug(
            f"Completed {self.get_transformation_type()}: {instance_name} "
            f"(+{nodes_added} nodes, +{edges_added} edges)"
        )

    def log_error(self, instance_name: str, error: Exception) -> None:
        """Log a parsing error."""
        self.logger.error(
            f"Error parsing {self.get_transformation_type()} {instance_name}: {error}",
            exc_info=True,
        )
