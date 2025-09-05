"""
Traceability utilities for maintaining links between graph elements and source files.

This module provides standardized functions to create consistent traceability properties
for both nodes and edges in the knowledge graph, ensuring every element can be traced
back to its original source file and context. Supports multiple ETL technologies including
SSIS, Informatica, Talend, Airflow, DataStage, and others.
"""

from typing import Dict, Any, Optional
from pathlib import Path


class SourceContext:
    """Technology-agnostic source context for nodes and edges in ETL graphs"""
    
    @staticmethod
    def create_node_traceability(
        source_file_path: str,
        source_file_type: Optional[str] = None,
        element_path: Optional[str] = None,
        line_number: Optional[int] = None,
        parent_element: Optional[str] = None,
        technology: Optional[str] = None,
        # Legacy parameters for backward compatibility
        xml_path: Optional[str] = None,
        parent_package: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create standardized traceability properties for nodes.
        
        Args:
            source_file_path: Full path to the source file
            source_file_type: Type of source file (e.g., xml, json, yaml, py, dtsx, ipc, etc.)
                            If not provided, will be inferred from file extension
            element_path: Path to the element in the source file (XPath for XML, JSONPath for JSON, etc.)
            line_number: Line number in the source file
            parent_element: Name of parent element for cross-references (workflow, package, DAG, etc.)
            technology: Technology being parsed (SSIS, Informatica, Talend, Airflow, etc.)
                       If not provided, will be inferred from context
            
            Legacy parameters (for backward compatibility):
            xml_path: Deprecated - use element_path instead
            parent_package: Deprecated - use parent_element instead
            
        Returns:
            Dictionary with standardized traceability properties
        """
        # Handle legacy parameters for backward compatibility
        if xml_path and not element_path:
            element_path = xml_path
        if parent_package and not parent_element:
            parent_element = parent_package
            
        # Infer file type from extension if not provided
        if not source_file_type:
            file_ext = Path(source_file_path).suffix.lower()
            source_file_type = file_ext.lstrip('.')
            
        # Build context
        context = {
            "source_file_path": str(Path(source_file_path).resolve()),
            "source_file_type": source_file_type
        }
        
        # Add technology if provided
        if technology:
            context["technology"] = technology
            
        # Add optional fields
        if element_path:
            context["element_path"] = element_path
            # Keep xml_path for backward compatibility
            if source_file_type in ['xml', 'dtsx', 'ipc']:
                context["xml_path"] = element_path
        if line_number:
            context["line_number"] = line_number
        if parent_element:
            context["parent_element"] = parent_element
            # Keep parent_package for backward compatibility with SSIS
            if technology == "SSIS":
                context["parent_package"] = parent_element
            
        return context

    @staticmethod
    def create_edge_traceability(
        source_file_path: str,
        derivation_method: str,
        element_location: Optional[str] = None,
        context_info: Optional[Dict[str, Any]] = None,
        confidence_level: str = "high",
        technology: Optional[str] = None,
        # Legacy parameters for backward compatibility
        xml_location: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create standardized traceability properties for edges.
        
        Args:
            source_file_path: Full path to the source file
            derivation_method: How the relationship was derived 
                             (metadata_parsing|sql_parsing|code_analysis|data_flow_analysis|inference|configuration)
            element_location: Path to the element that defined this relationship (XPath, JSONPath, line number, etc.)
            context_info: Additional context about how the relationship was derived
            confidence_level: Confidence in the relationship (high|medium|low)
            technology: Technology being parsed (SSIS, Informatica, Talend, Airflow, etc.)
            
            Legacy parameters (for backward compatibility):
            xml_location: Deprecated - use element_location instead
            
        Returns:
            Dictionary with standardized traceability properties
        """
        # Handle legacy parameters for backward compatibility
        if xml_location and not element_location:
            element_location = xml_location
            
        # Map old derivation methods to new ones for compatibility
        derivation_method_mapping = {
            "xml_metadata": "metadata_parsing",
            "data_flow_analysis": "data_flow_analysis",
            "sql_parsing": "sql_parsing",
            "inference": "inference"
        }
        derivation_method = derivation_method_mapping.get(derivation_method, derivation_method)
        
        context = {
            "source_file_path": str(Path(source_file_path).resolve()),
            "derivation_method": derivation_method,
            "confidence_level": confidence_level
        }
        
        # Add technology if provided
        if technology:
            context["technology"] = technology
            
        # Add optional fields
        if element_location:
            context["element_location"] = element_location
            # Keep xml_location for backward compatibility
            file_ext = Path(source_file_path).suffix.lower()
            if file_ext in ['.xml', '.dtsx', '.ipc']:
                context["xml_location"] = element_location
        if context_info:
            context["context_info"] = context_info
            
        return context

    @staticmethod
    def create_sql_derivation_context(
        sql_statement: str,
        component_type: Optional[str] = None,
        property_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create context info for SQL-derived relationships.
        Works with SQL from any ETL platform (SSIS, Informatica, Talend, etc.)
        
        Args:
            sql_statement: The SQL statement that established the relationship
            component_type: Type of ETL component (e.g., "Execute SQL Task", "SQL Transformation", "tDBInput")
            property_name: Name of the property containing the SQL (e.g., "SqlCommand", "Query", "SQL")
            
        Returns:
            Dictionary with SQL derivation context
        """
        context = {
            "sql_statement": sql_statement[:500],  # Truncate for storage
            "sql_statement_length": len(sql_statement)
        }
        
        if component_type:
            context["component_type"] = component_type
        if property_name:
            context["property_name"] = property_name
            
        return context

    @staticmethod
    def create_dataflow_derivation_context(
        component_type: str,
        component_name: str,
        input_name: Optional[str] = None,
        output_name: Optional[str] = None,
        transformation_details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create context info for data flow-derived relationships.
        Technology-agnostic method for any ETL platform's data flow components.
        
        Args:
            component_type: Type of data flow component (e.g., "OLE DB Source", "Source Qualifier", "tInput")
            component_name: Name of the component instance
            input_name: Name of input if applicable
            output_name: Name of output if applicable
            transformation_details: Details about transformations applied
            
        Returns:
            Dictionary with data flow derivation context
        """
        context = {
            "component_type": component_type,
            "component_name": component_name
        }
        
        if input_name:
            context["input_name"] = input_name
        if output_name:
            context["output_name"] = output_name
        if transformation_details:
            context["transformation_details"] = transformation_details
            
        return context

    @staticmethod
    def create_metadata_derivation_context(
        element_name: str,
        attribute: Optional[str] = None,
        property: Optional[str] = None,
        format: str = "xml",
        # Legacy parameters for backward compatibility
        xml_element_name: Optional[str] = None,
        xml_attribute: Optional[str] = None,
        xml_property: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create context info for metadata-derived relationships.
        Supports XML, JSON, YAML, and other metadata formats.
        
        Args:
            element_name: Name of the metadata element
            attribute: Attribute that established the relationship
            property: Property that established the relationship
            format: Format of the metadata (xml, json, yaml, etc.)
            
            Legacy parameters (for backward compatibility):
            xml_element_name: Deprecated - use element_name instead
            xml_attribute: Deprecated - use attribute instead
            xml_property: Deprecated - use property instead
            
        Returns:
            Dictionary with metadata derivation context
        """
        # Handle legacy parameters for backward compatibility
        if xml_element_name and not element_name:
            element_name = xml_element_name
        if xml_attribute and not attribute:
            attribute = xml_attribute
        if xml_property and not property:
            property = xml_property
            
        context = {
            "element_name": element_name,
            "format": format
        }
        
        if attribute:
            context["attribute"] = attribute
        if property:
            context["property"] = property
            
        # Keep legacy fields for backward compatibility
        if format == "xml":
            context["xml_element_name"] = element_name
            if attribute:
                context["xml_attribute"] = attribute
            if property:
                context["xml_property"] = property
                
        return context
    
    # Alias for backward compatibility
    @staticmethod
    def create_xml_derivation_context(
        xml_element_name: str,
        xml_attribute: Optional[str] = None,
        xml_property: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Legacy method for XML metadata-derived relationships.
        Maintained for backward compatibility - new code should use create_metadata_derivation_context.
        """
        return SourceContext.create_metadata_derivation_context(
            element_name=xml_element_name,
            attribute=xml_attribute,
            property=xml_property,
            format="xml"
        )


class TraceabilityValidator:
    """Utilities for validating traceability in graph structures"""
    
    @staticmethod
    def validate_node_traceability(node_dict: Dict[str, Any]) -> Dict[str, bool]:
        """
        Validate that a node has proper traceability information.
        
        Args:
            node_dict: Node dictionary representation
            
        Returns:
            Dictionary with validation results
        """
        properties = node_dict.get("properties", {})
        
        # Check for either new or legacy field names
        has_element_path = "element_path" in properties or "xml_path" in properties
        has_parent = "parent_element" in properties or "parent_package" in properties
        
        return {
            "has_source_file_path": "source_file_path" in properties,
            "has_source_file_type": "source_file_type" in properties,
            "has_technology": "technology" in properties,  # Optional but recommended
            "has_element_path": has_element_path,  # Optional
            "has_parent_element": has_parent,  # Optional
            "is_valid_file_path": (
                "source_file_path" in properties and 
                bool(properties.get("source_file_path"))
            )
        }
    
    @staticmethod
    def validate_edge_traceability(edge_dict: Dict[str, Any]) -> Dict[str, bool]:
        """
        Validate that an edge has proper traceability information.
        
        Args:
            edge_dict: Edge dictionary representation
            
        Returns:
            Dictionary with validation results
        """
        properties = edge_dict.get("properties", {})
        
        # Extended list of valid derivation methods (both legacy and new)
        valid_derivation_methods = [
            # New technology-agnostic methods
            "metadata_parsing", "sql_parsing", "code_analysis", 
            "data_flow_analysis", "inference", "configuration",
            # Legacy SSIS-specific methods (for backward compatibility)
            "xml_metadata"
        ]
        
        # Check for either new or legacy field names
        has_location = "element_location" in properties or "xml_location" in properties
        
        return {
            "has_source_file_path": "source_file_path" in properties,
            "has_derivation_method": "derivation_method" in properties,
            "has_confidence_level": "confidence_level" in properties,
            "has_technology": "technology" in properties,  # Optional but recommended
            "has_element_location": has_location,  # Optional
            "is_valid_derivation": (
                "derivation_method" in properties and 
                properties.get("derivation_method") in valid_derivation_methods
            )
        }