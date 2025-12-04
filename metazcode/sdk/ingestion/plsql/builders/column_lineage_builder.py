"""
Column lineage builder for PL/SQL parser.

Extracts column-level lineage from SQL statements and cursor definitions.
"""

import logging
import re
from typing import List, Dict, Any, Optional

from ..sql_semantics import EnhancedPlsqlParser, SqlSemantics

logger = logging.getLogger(__name__)


class ColumnLineageBuilder:
    """
    Builds column-level lineage information from SQL statements.

    Similar to SSIS/Informatica column lineage builders, but adapted for PL/SQL.
    """

    # Cursor pattern for extracting cursor definitions
    CURSOR_RE = re.compile(
        r"\bcursor\s+([a-zA-Z0-9_\$#]+)\s+is\s+(.*?)(?=\bopen\b|\bfor\b|;)",
        re.IGNORECASE | re.DOTALL
    )

    def __init__(self, sql_parser: Optional[EnhancedPlsqlParser] = None):
        """
        Initialize column lineage builder.

        Args:
            sql_parser: Optional SQL parser for semantic analysis
        """
        self.sql_parser = sql_parser or EnhancedPlsqlParser()
        self.logger = logging.getLogger(__name__)

    def extract_column_lineage(
        self,
        sql_statements: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Extract column lineage from SQL statements.

        Args:
            sql_statements: List of SQL statement strings

        Returns:
            List of lineage entries with source/target column mappings
        """
        lineage = []

        for sql_stmt in sql_statements:
            try:
                semantics = self.sql_parser.parse_sql_semantics(sql_stmt)
                if semantics and semantics.columns:
                    for col in semantics.columns:
                        # Clean and normalize expression
                        clean_expr = self._clean_expression(col.expression) if col.expression else col.column_name

                        # Determine transformation type
                        transformation_type = self._determine_transformation_type(col, clean_expr)

                        lineage_entry = {
                            "source_expression": col.expression or col.column_name,
                            "cleaned_expression": clean_expr,
                            "target_column": col.alias or col.column_name or "unknown",
                            "transformation_type": transformation_type,
                            "sql_statement": sql_stmt[:100] + "..." if len(sql_stmt) > 100 else sql_stmt,
                            "source_table": col.source_table,
                            "source_alias": col.source_alias
                        }
                        lineage.append(lineage_entry)

            except Exception as e:
                self.logger.warning(f"Failed to extract lineage from statement: {e}")

        return lineage

    def extract_cursor_lineage(
        self,
        block: str
    ) -> List[Dict[str, Any]]:
        """
        Extract column lineage from cursor definitions.

        Args:
            block: PL/SQL block containing cursor definitions

        Returns:
            List of cursor lineage entries
        """
        lineage = []

        for match in self.CURSOR_RE.finditer(block):
            cursor_name = match.group(1)
            cursor_sql = match.group(2)

            try:
                semantics = self.sql_parser.parse_sql_semantics(cursor_sql)
                if semantics:
                    # Extract column information
                    input_columns = []
                    output_columns = []

                    # Get source tables and columns
                    for table in semantics.tables:
                        input_columns.append({
                            "table_name": table.name,
                            "schema": table.schema or "public",
                            "alias": table.alias
                        })

                    # Get selected columns (avoid SELECT *)
                    for col in semantics.columns:
                        if col.column_name and col.column_name != '*':
                            output_columns.append({
                                "column_name": col.column_name,
                                "alias": col.alias,
                                "expression": self._clean_expression(col.expression) if col.expression else col.column_name,
                                "source_table": col.source_table
                            })

                    if input_columns or output_columns:
                        lineage.append({
                            "cursor_name": cursor_name,
                            "operation_type": "CURSOR_LOAD",
                            "input_columns": input_columns,
                            "output_columns": output_columns,
                            "source_sql": cursor_sql.strip()
                        })

            except Exception as e:
                self.logger.warning(f"Failed to parse cursor {cursor_name}: {e}")

        return lineage

    def _clean_expression(self, expression: str) -> str:
        """
        Clean SQL expressions and group complex functions.

        Args:
            expression: Raw SQL expression

        Returns:
            Cleaned expression
        """
        if not expression:
            return expression

        # Remove extra whitespace
        cleaned = ' '.join(expression.split())

        # Group complete ROUND(AVG(...),2) type expressions
        patterns = [
            (r'ROUND\s*\(\s*AVG\s*\([^)]+\)\s*,\s*\d+\s*\)', 'ROUNDED_AVERAGE'),
            (r'ROUND\s*\(\s*SUM\s*\([^)]+\)\s*,\s*\d+\s*\)', 'ROUNDED_SUM'),
            (r'ROUND\s*\(\s*COUNT\s*\([^)]+\)\s*,\s*\d+\s*\)', 'ROUNDED_COUNT'),
            (r'TO_DATE\s*\([^)]+\)', 'DATE_CONVERSION'),
            (r'TO_CHAR\s*\([^)]+\)', 'CHAR_CONVERSION'),
            (r'EXTRACT\s*\([^)]+\)', 'DATE_PART_EXTRACTION'),
            (r'NVL\s*\([^,]+,\s*[^)]+\)', 'NULL_VALUE_REPLACEMENT'),
            (r'DECODE\s*\([^)]+\)', 'CONDITIONAL_LOGIC'),
            (r'COUNT\s*\([^)]+\)\s+\w+', 'AGGREGATE_WITH_ALIAS'),
            (r'AVG\s*\([^)]+\)\s+\w+', 'AVERAGE_WITH_ALIAS'),
            (r'SUM\s*\([^)]+\)\s+\w+', 'SUM_WITH_ALIAS')
        ]

        for pattern, replacement in patterns:
            cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)

        return cleaned

    def _determine_transformation_type(self, col, clean_expr: str) -> str:
        """
        Determine the type of transformation applied to a column.

        Args:
            col: Column expression object
            clean_expr: Cleaned expression string

        Returns:
            Transformation type string
        """
        if clean_expr and clean_expr != col.column_name:
            if any(func in clean_expr.upper() for func in ['ROUNDED_', 'DATE_CONVERSION', 'AGGREGATE_']):
                return "TRANSFORMED"
            elif col.alias:
                return "DERIVED"
            else:
                return "COMPUTED"
        return "DIRECT"
