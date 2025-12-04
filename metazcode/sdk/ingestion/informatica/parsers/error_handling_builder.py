"""
Error Handling and Reject Path Builder for Informatica Transformations

Enhancement #5: Explicitly model error handling and reject paths
This module extracts error handling metadata from Informatica transformations,
making data quality rules and error paths visible in the graph output.

Output Structure:
{
    "error_handling": {
        "has_error_handling": true,
        "error_output_ports": ["ERROR_CODE", "ERROR_MSG"],
        "reject_strategy": "CONTINUE",
        "error_conditions": [
            {
                "condition": "ISNULL(CUSTOMER_ID)",
                "action": "REJECT",
                "error_code": "ERR_001",
                "error_message": "Customer ID cannot be null"
            }
        ],
        "data_quality_rules": [...]
    }
}
"""

from typing import Dict, List, Any, Optional
from lxml import etree
import re
import logging

logger = logging.getLogger(__name__)


class ErrorHandlingBuilder:
    """
    Builds error handling metadata for Informatica transformations.

    Supports:
    - Router transformations with reject groups
    - Filter transformations with reject paths
    - Expression transformations with error flags
    - Update Strategy transformations with reject conditions
    - Lookup transformations with error handling
    """

    def __init__(self):
        """Initialize the error handling builder."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def build_error_handling_metadata(
        self,
        transformation_element: etree._Element,
        transformation_type: str,
        instance_name: str,
        properties: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build error handling metadata for a transformation.

        Args:
            transformation_element: XML element with transformation definition
            transformation_type: Type of transformation
            instance_name: Instance name
            properties: Existing properties (may contain expressions, etc.)

        Returns:
            Error handling metadata dictionary
        """
        if transformation_element is None:
            return self._empty_error_handling()

        error_handling = {
            "has_error_handling": False,
            "error_output_ports": [],
            "reject_strategy": "NONE",
            "error_conditions": [],
            "data_quality_rules": [],
        }

        # Check for error-related output ports
        error_ports = self._extract_error_ports(transformation_element)
        if error_ports:
            error_handling["has_error_handling"] = True
            error_handling["error_output_ports"] = error_ports

        # Transformation-specific error handling
        trans_type_lower = transformation_type.lower()

        if "router" in trans_type_lower:
            router_error_handling = self._extract_router_error_handling(transformation_element)
            error_handling.update(router_error_handling)

        elif "filter" in trans_type_lower:
            filter_error_handling = self._extract_filter_error_handling(transformation_element, properties)
            error_handling.update(filter_error_handling)

        elif "expression" in trans_type_lower:
            expression_error_handling = self._extract_expression_error_handling(transformation_element, properties)
            error_handling.update(expression_error_handling)

        elif "update strategy" in trans_type_lower:
            update_error_handling = self._extract_update_strategy_error_handling(transformation_element, properties)
            error_handling.update(update_error_handling)

        elif "lookup" in trans_type_lower:
            lookup_error_handling = self._extract_lookup_error_handling(transformation_element)
            error_handling.update(lookup_error_handling)

        # Only return if there's actual error handling
        if error_handling["has_error_handling"]:
            return error_handling
        else:
            return {}

    def _empty_error_handling(self) -> Dict[str, Any]:
        """Return empty error handling structure."""
        return {}

    def _extract_error_ports(self, transformation_element: etree._Element) -> List[str]:
        """
        Extract error-related output ports.

        Common patterns:
        - ERR_CODE, ERROR_CODE, ERROR_CD
        - ERR_MSG, ERROR_MSG, ERROR_MESSAGE
        - ERR_FLAG, ERROR_FLAG, IS_ERROR
        - REJECT_FLAG, IS_REJECTED
        """
        error_ports = []
        error_patterns = [
            r'ERR[_]?CODE',
            r'ERROR[_]?CODE',
            r'ERR[_]?MSG',
            r'ERROR[_]?MSG',
            r'ERROR[_]?MESSAGE',
            r'ERR[_]?FLAG',
            r'ERROR[_]?FLAG',
            r'IS[_]?ERROR',
            r'REJECT[_]?FLAG',
            r'IS[_]?REJECTED',
        ]

        transform_fields = transformation_element.xpath(".//TRANSFORMFIELD")
        for field in transform_fields:
            field_name = field.get("NAME", "")
            port_type = field.get("PORTTYPE", "")

            # Check if it's an output port with error-related name
            if "OUTPUT" in port_type or port_type == "O":
                for pattern in error_patterns:
                    if re.search(pattern, field_name, re.IGNORECASE):
                        error_ports.append(field_name)
                        break

        return error_ports

    def _extract_router_error_handling(self, transformation_element: etree._Element) -> Dict[str, Any]:
        """
        Extract error handling from Router transformations.

        Routers often have "default" or "reject" groups for handling errors.
        """
        result = {}

        # Find router groups
        groups = transformation_element.xpath(".//TABLEATTRIBUTE[@NAME='Router Group Information']/TABLEATTRIBUTE")

        reject_groups = []
        default_groups = []

        for group in groups:
            group_name = group.get("NAME", "")
            group_name_lower = group_name.lower()

            # Check if it's a reject/error/default group
            if any(keyword in group_name_lower for keyword in ["reject", "error", "invalid", "bad"]):
                # Extract group condition
                condition = ""
                for attr in group.findall("ATTRIBUTE"):
                    if attr.get("NAME") == "Group Filter Condition":
                        condition = attr.get("VALUE", "")

                reject_groups.append({
                    "group_name": group_name,
                    "condition": condition,
                    "type": "reject"
                })

            elif "default" in group_name_lower:
                default_groups.append({
                    "group_name": group_name,
                    "type": "default"
                })

        if reject_groups or default_groups:
            result["has_error_handling"] = True
            result["reject_strategy"] = "ROUTER_GROUPS"
            result["error_conditions"] = reject_groups + default_groups

        return result

    def _extract_filter_error_handling(
        self,
        transformation_element: etree._Element,
        properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Extract error handling from Filter transformations.

        Filters may have reject paths for invalid data.
        """
        result = {}

        # Check if filter has a reject path (some filters output to reject port)
        filter_condition = properties.get("filter_condition", "")

        if filter_condition:
            # Analyze filter condition for data quality rules
            quality_rules = self._extract_data_quality_rules(filter_condition)

            if quality_rules:
                result["has_error_handling"] = True
                result["reject_strategy"] = "FILTER_CONDITION"
                result["data_quality_rules"] = quality_rules

        return result

    def _extract_expression_error_handling(
        self,
        transformation_element: etree._Element,
        properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Extract error handling from Expression transformations.

        Expressions may set error flags based on validation logic.
        """
        result = {}

        # Check expressions for error flag assignments
        expressions = properties.get("expressions", {})

        error_conditions = []

        for field_name, expression in expressions.items():
            # Check if this expression sets an error flag
            if any(kw in field_name.upper() for kw in ["ERR", "ERROR", "REJECT", "INVALID"]):
                error_conditions.append({
                    "field": field_name,
                    "expression": expression,
                    "type": "validation"
                })

        if error_conditions:
            result["has_error_handling"] = True
            result["reject_strategy"] = "ERROR_FLAGS"
            result["error_conditions"] = error_conditions

        return result

    def _extract_update_strategy_error_handling(
        self,
        transformation_element: etree._Element,
        properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Extract error handling from Update Strategy transformations.

        Update Strategy may have DD_REJECT operations for invalid records.
        """
        result = {}

        update_expression = properties.get("update_strategy_expression", {}).get("original_expression", "")

        if "DD_REJECT" in update_expression:
            result["has_error_handling"] = True
            result["reject_strategy"] = "DD_REJECT"
            result["error_conditions"] = [{
                "condition": update_expression,
                "action": "REJECT",
                "type": "update_strategy"
            }]

        return result

    def _extract_lookup_error_handling(self, transformation_element: etree._Element) -> Dict[str, Any]:
        """
        Extract error handling from Lookup transformations.

        Lookups may have error handling for failed lookups.
        """
        result = {}

        # Check lookup properties
        for attr in transformation_element.findall(".//ATTRIBUTE"):
            attr_name = attr.get("NAME", "")
            attr_value = attr.get("VALUE", "")

            if attr_name == "Lookup Failure":
                if attr_value and attr_value != "STOP":
                    result["has_error_handling"] = True
                    result["reject_strategy"] = f"LOOKUP_FAILURE_{attr_value}"

        return result

    def _extract_data_quality_rules(self, condition: str) -> List[Dict[str, Any]]:
        """
        Extract data quality rules from a condition.

        Common patterns:
        - NOT ISNULL(field) - null check
        - LENGTH(field) > 0 - length check
        - field LIKE pattern - format check
        - field IN (...) - valid values check
        """
        rules = []

        if not condition:
            return rules

        condition_upper = condition.upper()

        # Null checks
        if "ISNULL" in condition_upper:
            rules.append({
                "type": "null_check",
                "condition": condition,
                "severity": "HIGH"
            })

        # Length checks
        if "LENGTH" in condition_upper or "LEN(" in condition_upper:
            rules.append({
                "type": "length_validation",
                "condition": condition,
                "severity": "MEDIUM"
            })

        # Format checks (LIKE patterns)
        if "LIKE" in condition_upper:
            rules.append({
                "type": "format_validation",
                "condition": condition,
                "severity": "MEDIUM"
            })

        # Range checks
        if any(op in condition for op in [">", "<", ">=", "<=", "BETWEEN"]):
            rules.append({
                "type": "range_validation",
                "condition": condition,
                "severity": "MEDIUM"
            })

        # Valid values check
        if " IN " in condition_upper or " IN(" in condition_upper:
            rules.append({
                "type": "valid_values_check",
                "condition": condition,
                "severity": "MEDIUM"
            })

        return rules
