"""Parser for PL/SQL functions."""

import re
from .procedure_parser import ProcedureParser


class FunctionParser(ProcedureParser):
    """Parses PL/SQL function definitions. Inherits from ProcedureParser since they're similar."""

    FUNC_PATTERN = re.compile(
        r"\b(create|replace)\s+(or\s+replace\s+)?function\s+([a-zA-Z0-9_\$#]+)",
        re.IGNORECASE
    )

    def detect_functions(self, text: str):
        """Detect function definitions in text."""
        functions = []
        for match in self.FUNC_PATTERN.finditer(text):
            func_name = match.group(3)
            start = match.start()
            # Find END; statement
            end_pattern = re.compile(r"\bend\s+" + re.escape(func_name) + r"\s*;", re.IGNORECASE)
            end_match = end_pattern.search(text, start)
            end = end_match.end() if end_match else len(text)
            functions.append((func_name, (start, end)))
        return functions

    def parse_function(self, func_name: str, block: str, line_number: int = 0):
        """Parse a function - delegates to procedure parser with different type."""
        # Functions are parsed like procedures but with type "function"
        nodes, edges = self.parse_procedure(func_name, block, line_number)

        # Update the operation type to function
        if nodes:
            nodes[0].properties["procedure_type"] = "function"
            nodes[0].properties["operation_type"] = "function"

        return nodes, edges
