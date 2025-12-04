"""
Column Lineage Builder for Informatica Transformations

Enhancement #1: Implement granular intra-component column lineage
This module builds detailed column-level lineage for Informatica transformations,
matching the SSIS parser's column_lineage structure.

Output Structure:
{
    "column_lineage": [
        {
            "component_name": "Expression_1",
            "input_ports": [...],
            "variable_ports": [...],
            "output_ports": [...],
            "transformations": {
                "output_column": {
                    "expression": "INPUT_COL1 + INPUT_COL2",
                    "friendly_expression": "Add Input Column 1 and Input Column 2",
                    "input_columns": ["INPUT_COL1", "INPUT_COL2"],
                    "variable_columns": [],
                    "data_type": "decimal"
                }
            }
        }
    ]
}
"""

from typing import Dict, List, Any, Optional
from lxml import etree
import re
import logging

logger = logging.getLogger(__name__)


class ColumnLineageBuilder:
    """
    Builds detailed column-level lineage for Informatica transformations.

    Supports all major transformation types:
    - Expression
    - Aggregator
    - Joiner
    - Lookup
    - Router
    - Filter
    - Sorter
    - Rank
    - Source Qualifier
    - Update Strategy
    """

    def __init__(self):
        """Initialize the column lineage builder."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def build_lineage(
        self,
        transformation_element: etree._Element,
        transformation_type: str,
        instance_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Build column lineage for a transformation.

        Args:
            transformation_element: XML element with transformation definition
            transformation_type: Type of transformation (Expression, Joiner, etc.)
            instance_name: Instance name of the transformation

        Returns:
            List of lineage dictionaries
        """
        if transformation_element is None:
            return []

        # Extract all transformation fields
        fields = self._extract_fields(transformation_element)

        if not fields:
            return []

        # Categorize ports by type
        input_ports = []
        output_ports = []
        variable_ports = []

        for field in fields:
            port_type = field.get("port_type", "")
            field_info = {
                "column_name": field.get("name", ""),
                "data_type": field.get("datatype", ""),
                "precision": field.get("precision", ""),
                "scale": field.get("scale", ""),
                "expression": field.get("expression", ""),
            }

            # Categorize by port type
            if "INPUT" in port_type or port_type == "I":
                input_ports.append(field_info)
            elif "OUTPUT" in port_type or port_type == "O":
                output_ports.append(field_info)
            elif "VARIABLE" in port_type or port_type == "V":
                variable_ports.append(field_info)

        # Build transformations mapping (for output/variable ports with expressions)
        transformations = {}

        # Process variable ports first (they can be used by other expressions)
        for field in fields:
            port_type = field.get("port_type", "")
            if "VARIABLE" in port_type or port_type == "V":
                column_name = field.get("name", "")
                expression = field.get("expression", "")

                if expression:
                    transformations[column_name] = self._analyze_expression(
                        expression=expression,
                        column_name=column_name,
                        data_type=field.get("datatype", ""),
                        all_fields=fields,
                    )

        # Process output ports with expressions
        for field in fields:
            port_type = field.get("port_type", "")
            if "OUTPUT" in port_type or port_type == "O":
                column_name = field.get("name", "")
                expression = field.get("expression", "")

                if expression:
                    transformations[column_name] = self._analyze_expression(
                        expression=expression,
                        column_name=column_name,
                        data_type=field.get("datatype", ""),
                        all_fields=fields,
                    )

        # Build the lineage structure
        lineage = {
            "component_name": instance_name,
            "transformation_type": transformation_type,
            "input_ports": input_ports,
            "variable_ports": variable_ports,
            "output_ports": output_ports,
            "transformations": transformations,
            "port_count": {
                "input": len(input_ports),
                "output": len(output_ports),
                "variable": len(variable_ports),
            }
        }

        return [lineage]

    def _extract_fields(self, transformation_element: etree._Element) -> List[Dict[str, Any]]:
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
                "port_type": field.get("PORTTYPE", ""),
                "expression": field.get("EXPRESSION", ""),
                "default_value": field.get("DEFAULTVALUE", ""),
                "description": field.get("DESCRIPTION", ""),
            }
            # Remove empty values
            field_data = {k: v for k, v in field_data.items() if v}
            fields.append(field_data)

        return fields

    def _analyze_expression(
        self,
        expression: str,
        column_name: str,
        data_type: str,
        all_fields: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Analyze an expression to identify column dependencies.

        Args:
            expression: The transformation expression
            column_name: Name of the output/variable column
            data_type: Data type of the column
            all_fields: All fields in the transformation (for context)

        Returns:
            Dictionary with expression analysis
        """
        # Extract referenced columns from expression
        input_columns = []
        variable_columns = []

        # Create a set of all field names for matching
        field_names = {f.get("name", "") for f in all_fields}
        input_field_names = {
            f.get("name", "") for f in all_fields
            if "INPUT" in f.get("port_type", "") or f.get("port_type", "") == "I"
        }
        variable_field_names = {
            f.get("name", "") for f in all_fields
            if "VARIABLE" in f.get("port_type", "") or f.get("port_type", "") == "V"
        }

        # Match field names in expression
        for field_name in field_names:
            if field_name and field_name in expression:
                if field_name in input_field_names:
                    if field_name not in input_columns:
                        input_columns.append(field_name)
                elif field_name in variable_field_names:
                    if field_name not in variable_columns:
                        variable_columns.append(field_name)

        # Generate friendly expression (Enhancement #4 preview)
        friendly_expression = self._generate_friendly_expression(expression, input_columns, variable_columns)

        return {
            "expression": expression,
            "friendly_expression": friendly_expression,
            "input_columns": input_columns,
            "variable_columns": variable_columns,
            "data_type": data_type,
            "complexity": self._assess_expression_complexity(expression),
        }

    def _generate_friendly_expression(
        self,
        expression: str,
        input_columns: List[str],
        variable_columns: List[str],
    ) -> str:
        """
        Generate a human-readable version of an expression (Enhancement #4).

        Args:
            expression: The original expression
            input_columns: List of input columns referenced
            variable_columns: List of variable columns referenced

        Returns:
            Friendly description of the expression
        """
        # Simple heuristics for common patterns
        expr_lower = expression.lower()

        # Concatenation patterns
        if "||" in expression or "concat" in expr_lower:
            return f"Concatenate {', '.join(input_columns + variable_columns)}"

        # Arithmetic operations
        if "+" in expression and len(input_columns) == 2:
            return f"Add {' and '.join(input_columns)}"
        elif "-" in expression and len(input_columns) == 2:
            return f"Subtract {input_columns[1]} from {input_columns[0]}"
        elif "*" in expression and len(input_columns) == 2:
            return f"Multiply {' and '.join(input_columns)}"
        elif "/" in expression and len(input_columns) == 2:
            return f"Divide {input_columns[0]} by {input_columns[1]}"

        # Conditional logic
        if "iif" in expr_lower:
            return f"Conditional transformation based on {', '.join(input_columns + variable_columns)}"

        # Date operations
        if any(kw in expr_lower for kw in ["to_date", "to_char", "sysdate", "add_to_date"]):
            return f"Date transformation using {', '.join(input_columns + variable_columns) if input_columns or variable_columns else 'system date'}"

        # String operations
        if any(kw in expr_lower for kw in ["substr", "trim", "ltrim", "rtrim", "upper", "lower"]):
            return f"String transformation of {', '.join(input_columns + variable_columns)}"

        # Lookup operations
        if ":lkp." in expr_lower:
            return f"Lookup transformation using {', '.join(input_columns + variable_columns)}"

        # Default: just list the columns involved
        if input_columns or variable_columns:
            return f"Transform {', '.join(input_columns + variable_columns)}"
        else:
            return "Constant or system-generated value"

    def _assess_expression_complexity(self, expression: str) -> str:
        """
        Assess the complexity of an expression.

        Args:
            expression: The transformation expression

        Returns:
            Complexity level: "simple", "moderate", or "complex"
        """
        if not expression:
            return "simple"

        # Count nested function calls
        nested_count = expression.count("(") - expression.count(")")
        function_count = len(re.findall(r'\w+\(', expression))

        # Check for conditional logic
        has_conditionals = "IIF" in expression.upper() or "DECODE" in expression.upper()

        if function_count >= 3 or has_conditionals:
            return "complex"
        elif function_count >= 1 or "||" in expression:
            return "moderate"
        else:
            return "simple"


class JoinerLineageBuilder(ColumnLineageBuilder):
    """
    Specialized lineage builder for Joiner transformations.

    Adds join-specific metadata.
    """

    def build_lineage(
        self,
        transformation_element: etree._Element,
        transformation_type: str,
        instance_name: str,
    ) -> List[Dict[str, Any]]:
        """Build lineage with join-specific information."""
        lineage = super().build_lineage(transformation_element, transformation_type, instance_name)

        if not lineage:
            return []

        # Extract join condition
        join_condition = ""
        join_type = "NORMAL"  # Default

        for attr in transformation_element.findall(".//TABLEATTRIBUTE"):
            attr_name = attr.get("NAME", "")
            attr_value = attr.get("VALUE", "")

            if attr_name == "Join Condition":
                join_condition = attr_value
            elif attr_name == "Join Type":
                join_type = attr_value

        # Add join metadata to lineage
        lineage[0]["join_metadata"] = {
            "join_type": join_type,
            "join_condition": join_condition,
            "master_ports": [p for p in lineage[0]["input_ports"] if "MASTER" in p.get("column_name", "").upper()],
            "detail_ports": [p for p in lineage[0]["input_ports"] if "DETAIL" in p.get("column_name", "").upper()],
        }

        return lineage


class LookupLineageBuilder(ColumnLineageBuilder):
    """
    Specialized lineage builder for Lookup transformations.

    Adds lookup-specific metadata.
    """

    def build_lineage(
        self,
        transformation_element: etree._Element,
        transformation_type: str,
        instance_name: str,
    ) -> List[Dict[str, Any]]:
        """Build lineage with lookup-specific information."""
        lineage = super().build_lineage(transformation_element, transformation_type, instance_name)

        if not lineage:
            return []

        # Extract lookup condition and source
        lookup_condition = ""
        lookup_source = ""
        lookup_sql = ""

        for attr in transformation_element.findall(".//TABLEATTRIBUTE"):
            attr_name = attr.get("NAME", "")
            attr_value = attr.get("VALUE", "")

            if attr_name == "Lookup Condition":
                lookup_condition = attr_value
            elif attr_name == "Lookup SQL Override":
                lookup_sql = attr_value

        # Extract source qualifier name if available
        for attr in transformation_element.findall(".//ATTRIBUTE"):
            attr_name = attr.get("NAME", "")
            attr_value = attr.get("VALUE", "")

            if attr_name == "Source Qualifier":
                lookup_source = attr_value

        # Add lookup metadata
        lineage[0]["lookup_metadata"] = {
            "lookup_condition": lookup_condition,
            "lookup_source": lookup_source,
            "lookup_sql_override": lookup_sql,
            "return_ports": [p for p in lineage[0]["output_ports"] if p.get("port_type", "") == "OUTPUT/RETURNPORT"],
        }

        return lineage
