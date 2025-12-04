"""
Base parser for PL/SQL constructs.

Provides common functionality for all PL/SQL parsers.
"""

import logging
import re
from typing import List, Tuple, Set, Dict, Any, Optional

from ....models.graph import Node, Edge
from ..models.parsing_context import PlsqlParsingContext
from ..builders.graph_builder import PlsqlGraphBuilder
from ..builders.column_lineage_builder import ColumnLineageBuilder
from ..builders.connection_builder import ConnectionBuilder

logger = logging.getLogger(__name__)


class BasePlsqlParser:
    """
    Base parser providing common functionality for PL/SQL construct parsing.

    Similar to SSIS base_parser.py and Informatica base_transformation_parser.py.
    """

    # Common Oracle functions that should not be treated as tables
    ORACLE_FUNCTIONS = {
        'round', 'avg', 'sum', 'count', 'max', 'min', 'substr', 'to_date', 'to_char',
        'nvl', 'decode', 'coalesce', 'trim', 'ltrim', 'rtrim', 'upper', 'lower',
        'sysdate', 'current_date', 'cast', 'extract', 'trunc', 'ceil', 'floor',
        'abs', 'mod', 'power', 'sqrt', 'sign', 'length', 'instr', 'replace',
        'translate', 'lpad', 'rpad', 'soundex', 'ascii', 'chr', 'initcap'
    }

    # SQL keywords and reserved words
    RESERVED_WORDS = {
        "select", "from", "insert", "update", "delete", "merge", "into", "values",
        "where", "and", "or", "not", "exists", "in", "between", "like",
        "join", "inner", "left", "right", "full", "outer", "on", "group", "order",
        "by", "having", "union", "all", "distinct", "case", "when", "then", "else", "end",
        "dual", "rownum", "varchar2", "number", "date", "char", "timestamp",
        "with", "as", "is", "begin", "declare", "loop", "for", "while",
        "commit", "rollback", "table", "column", "row", "add", "comment", "prompt", "source"
    }

    def __init__(
        self,
        context: PlsqlParsingContext,
        graph_builder: PlsqlGraphBuilder,
        lineage_builder: ColumnLineageBuilder,
        connection_builder: ConnectionBuilder
    ):
        """
        Initialize base parser.

        Args:
            context: Parsing context
            graph_builder: Graph builder
            lineage_builder: Column lineage builder
            connection_builder: Connection builder
        """
        self.context = context
        self.graph_builder = graph_builder
        self.lineage_builder = lineage_builder
        self.connection_builder = connection_builder
        self.logger = logging.getLogger(__name__)

    def strip_comments(self, text: str) -> str:
        """Remove SQL comments from text."""
        # Remove /* */ comments
        text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
        # Remove -- comments
        text = re.sub(r"--.*?$", " ", text, flags=re.M)
        return text

    def is_reserved_word(self, word: str) -> bool:
        """Check if word is a reserved SQL keyword."""
        return word.lower() in self.RESERVED_WORDS

    def is_oracle_function(self, name: str) -> bool:
        """Check if name is an Oracle built-in function."""
        return name.lower() in self.ORACLE_FUNCTIONS

    def is_fake_table_node(self, table_name: str) -> bool:
        """Check if this is a fake table node that should be rejected."""
        return table_name == "(" or table_name.startswith("(") or self.is_oracle_function(table_name)

    def extract_tables_from_select(self, sql: str) -> Set[str]:
        """
        Extract table names from SELECT statements.

        Args:
            sql: SQL statement text

        Returns:
            Set of table names
        """
        result = set()
        select_pattern = re.compile(r"\bfrom\s+([a-zA-Z0-9_\.\$#\"]+)", re.IGNORECASE)

        for match in select_pattern.finditer(sql):
            name = match.group(1).strip('"').strip()
            if not self.is_reserved_word(name) and not self.is_fake_table_node(name):
                result.add(name)

        return result

    def extract_tables_from_dml(self, sql: str) -> Tuple[Set[str], Set[str]]:
        """
        Extract table names from DML statements.

        Args:
            sql: SQL statement text

        Returns:
            Tuple of (read_tables, write_tables)
        """
        writes = set()
        reads = set()

        # Extract INSERT INTO tables
        insert_pattern = re.compile(r"\binsert\s+into\s+([a-zA-Z0-9_\.\$#\"]+)", re.IGNORECASE)
        for match in insert_pattern.finditer(sql):
            name = match.group(1).strip('"').strip()
            if not self.is_reserved_word(name) and not self.is_fake_table_node(name):
                writes.add(name)

        # Extract UPDATE tables
        update_pattern = re.compile(r"\bupdate\s+([a-zA-Z0-9_\.\$#\"]+)\b", re.IGNORECASE)
        for match in update_pattern.finditer(sql):
            name = match.group(1).strip('"').strip()
            if not self.is_reserved_word(name) and not self.is_fake_table_node(name):
                writes.add(name)

        # Extract MERGE INTO tables
        merge_pattern = re.compile(r"\bmerge\s+into\s+([a-zA-Z0-9_\.\$#\"]+)", re.IGNORECASE)
        for match in merge_pattern.finditer(sql):
            name = match.group(1).strip('"').strip()
            if not self.is_reserved_word(name) and not self.is_fake_table_node(name):
                writes.add(name)

        # FROM clauses could appear in INSERT ... SELECT or MERGE
        reads |= self.extract_tables_from_select(sql)

        return reads, writes

    def extract_create_tables(self, block: str) -> List[Tuple[str, str]]:
        """
        Extract CREATE TABLE statements with table names.

        Args:
            block: SQL block text

        Returns:
            List of (table_name, create_statement) tuples
        """
        results = []
        create_pattern = re.compile(
            r"\bcreate\s+table\s+([a-zA-Z0-9_\.\$#\"]+)\s*\(",
            re.IGNORECASE
        )

        for match in create_pattern.finditer(block):
            table_name = match.group(1).strip('"').strip()
            if self.is_fake_table_node(table_name):
                continue

            # Extract full CREATE TABLE statement using balanced parentheses
            start_pos = match.start()
            paren_count = 0
            pos = match.end() - 1  # Start at the opening parenthesis

            for i in range(pos, len(block)):
                if block[i] == '(':
                    paren_count += 1
                elif block[i] == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        # Found the matching closing parenthesis
                        end_pos = i + 1
                        # Look for optional semicolon
                        if end_pos < len(block) and block[end_pos:end_pos + 1].strip() == ';':
                            end_pos += 1
                        create_statement = block[start_pos:end_pos].strip()
                        results.append((table_name, create_statement))
                        break

        return results

    def categorize_operation_subtype(self, sql_statements: List[str]) -> str:
        """
        Categorize operation subtype based on SQL patterns.

        Matches SSIS approach of categorizing operations as DATA_FLOW or EXECUTE.

        Args:
            sql_statements: List of SQL statements

        Returns:
            Operation subtype string
        """
        has_select = any('select' in stmt.lower() for stmt in sql_statements)
        has_insert = any('insert' in stmt.lower() for stmt in sql_statements)
        has_update = any('update' in stmt.lower() for stmt in sql_statements)
        has_merge = any('merge' in stmt.lower() for stmt in sql_statements)
        has_create = any('create table' in stmt.lower() for stmt in sql_statements)
        has_procedure = any(
            'create procedure' in stmt.lower() or 'create function' in stmt.lower()
            for stmt in sql_statements
        )

        # Match SSIS operation subtypes
        if has_procedure:
            return "EXECUTE"
        elif has_merge:
            return "DATA_FLOW"
        elif has_create:
            return "EXECUTE"
        elif has_insert and has_select:
            return "DATA_FLOW"
        elif has_update:
            return "DATA_FLOW"
        elif has_insert:
            return "DATA_FLOW"
        elif has_select:
            return "DATA_FLOW"
        else:
            return "EXECUTE"

    def extract_sql_statements(self, block: str) -> List[str]:
        """
        Extract individual SQL statements from a PL/SQL block.

        Args:
            block: PL/SQL block text

        Returns:
            List of SQL statements
        """
        # Simple statement extraction by semicolon
        # This is a basic implementation - could be enhanced with proper PL/SQL parser
        statements = []

        # Split by semicolon but preserve statements
        parts = block.split(';')

        for part in parts:
            cleaned = part.strip()
            if cleaned and len(cleaned) > 10:  # Filter out very short statements
                # Check if it looks like a SQL statement
                if any(keyword in cleaned.lower() for keyword in [
                    'select', 'insert', 'update', 'delete', 'merge', 'create'
                ]):
                    statements.append(cleaned)

        return statements
