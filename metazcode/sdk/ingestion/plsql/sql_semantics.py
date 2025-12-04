#!/usr/bin/env python3
"""
Enhanced SQL Semantics Parser for PL/SQL Migration Support

This module provides enhanced SQL parsing capabilities using SQLGlot to capture complete
SQL semantics including JOIN relationships, column aliases, and query structure
for PL/SQL to target platform migration.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
from enum import Enum

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.expressions import Select, Table, Join, Column, Identifier, Subquery
    _HAS_SQLGLOT = True
except ImportError:
    _HAS_SQLGLOT = False
    
import re

logger = logging.getLogger(__name__)

class JoinType(str, Enum):
    """Supported SQL JOIN types."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN" 
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"
    CROSS = "CROSS JOIN"

@dataclass
class TableReference:
    """Represents a table reference with optional alias."""
    name: str
    alias: Optional[str] = None
    schema: Optional[str] = None
    
    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name
    
    @property
    def display_name(self) -> str:
        """Get display name (alias if available, otherwise name)."""
        return self.alias or self.name

@dataclass 
class JoinRelationship:
    """Represents a JOIN relationship between tables."""
    join_type: JoinType
    left_table: TableReference
    right_table: TableReference
    condition: str
    raw_condition: str  # Original condition text
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "join_type": self.join_type.value,
            "left_table": {
                "name": self.left_table.name,
                "alias": self.left_table.alias,
                "schema": self.left_table.schema,
                "full_name": self.left_table.full_name
            },
            "right_table": {
                "name": self.right_table.name,
                "alias": self.right_table.alias, 
                "schema": self.right_table.schema,
                "full_name": self.right_table.full_name
            },
            "condition": self.condition,
            "raw_condition": self.raw_condition
        }

@dataclass
class ColumnExpression:
    """Represents a column expression in SELECT clause."""
    expression: str
    alias: Optional[str] = None
    source_table: Optional[str] = None
    source_alias: Optional[str] = None
    column_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "expression": self.expression,
            "alias": self.alias,
            "source_table": self.source_table,
            "source_alias": self.source_alias,
            "column_name": self.column_name,
            "effective_name": self.alias or self.column_name or self.expression
        }

@dataclass
class InlineView:
    """Represents an inline view/subquery with base table references."""
    alias: str
    sql: str
    base_tables: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "alias": self.alias,
            "sql": self.sql,
            "base_tables": self.base_tables
        }

@dataclass
class SqlSemantics:
    """Complete SQL semantics metadata for migration support."""
    original_query: str
    tables: List[TableReference]
    joins: List[JoinRelationship]
    columns: List[ColumnExpression]
    inline_views: List[InlineView]
    where_clause: Optional[str] = None
    dynamic_sql_info: Optional[Dict[str, Any]] = None  # GAP 4: Dynamic SQL metadata
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "original_query": self.original_query,
            "tables": [
                {
                    "name": t.name,
                    "alias": t.alias,
                    "schema": t.schema,
                    "full_name": t.full_name
                } for t in self.tables
            ],
            "joins": [j.to_dict() for j in self.joins],
            "columns": [c.to_dict() for c in self.columns],
            "inline_views": [iv.to_dict() for iv in self.inline_views],
            "where_clause": self.where_clause,
            "migration_metadata": {
                "table_count": len(self.tables),
                "join_count": len(self.joins),
                "column_count": len(self.columns),
                "inline_view_count": len(self.inline_views),
                "has_aliases": any(c.alias for c in self.columns),
                "has_joins": len(self.joins) > 0,
                "has_inline_views": len(self.inline_views) > 0,
                "join_types": list(set(j.join_type.value for j in self.joins))
            }
        }
        
        # GAP 4: Add dynamic SQL metadata if present
        if self.dynamic_sql_info:
            result["dynamic_sql_info"] = self.dynamic_sql_info
            result["migration_metadata"]["has_dynamic_sql"] = True
            result["migration_metadata"]["dynamic_sql_types"] = self.dynamic_sql_info.get("types", [])
        else:
            result["migration_metadata"]["has_dynamic_sql"] = False
            
        return result

class EnhancedPlsqlParser:
    """
    Enhanced PL/SQL SQL parser using SQLGlot for accurate parsing.
    
    This parser extracts:
    1. Table names with aliases and schemas
    2. JOIN relationships with conditions and types
    3. Column expressions with aliases
    4. Inline views/subqueries
    5. Complete query structure
    """
    
    # Oracle SQL keywords that should not be treated as table aliases
    SQL_KEYWORDS = {
        'select', 'from', 'where', 'join', 'inner', 'left', 'right', 'full', 'outer',
        'union', 'order', 'group', 'by', 'having', 'distinct', 'as', 'on', 'and', 'or',
        'not', 'in', 'exists', 'case', 'when', 'then', 'else', 'end', 'null', 'is',
        'between', 'like', 'into', 'values', 'insert', 'update', 'delete', 'merge',
        'create', 'alter', 'drop', 'table', 'view', 'index', 'sequence', 'constraint',
        'primary', 'key', 'foreign', 'references', 'unique', 'check', 'default',
        'varchar2', 'number', 'date', 'timestamp', 'char', 'clob', 'blob', 'rowid',
        'dual', 'sysdate', 'systimestamp', 'rownum', 'nextval', 'currval'
    }
    
    # Oracle built-in functions that should not be treated as tables
    ORACLE_FUNCTIONS = {
        'to_date', 'to_char', 'to_number', 'extract', 'substr', 'length', 'instr',
        'upper', 'lower', 'initcap', 'trim', 'ltrim', 'rtrim', 'replace', 'translate',
        'decode', 'nvl', 'nvl2', 'coalesce', 'nullif', 'greatest', 'least',
        'abs', 'ceil', 'floor', 'round', 'trunc', 'mod', 'power', 'sqrt', 'sign',
        'sin', 'cos', 'tan', 'asin', 'acos', 'atan', 'atan2', 'exp', 'ln', 'log',
        'avg', 'count', 'max', 'min', 'sum', 'stddev', 'variance',
        'rank', 'dense_rank', 'row_number', 'lead', 'lag', 'first_value', 'last_value',
        'sysdate', 'systimestamp', 'current_date', 'current_timestamp', 'localtimestamp',
        'add_months', 'months_between', 'next_day', 'last_day', 'trunc_date'
    }
    
    def __init__(self):
        """Initialize the enhanced PL/SQL parser."""
        self.logger = logging.getLogger(__name__)
        self.validation_report = {
            'sql_statements_parsed': 0,
            'sqlglot_parse_failures': 0,
            'regex_fallback_used': 0,
            'joins_extracted': 0,
            'inline_views_found': 0,
            'tables_resolved': 0,
            'columns_extracted': 0
        }
    
    def get_validation_report(self) -> Dict[str, int]:
        """Get validation report for testing and monitoring."""
        return self.validation_report.copy()
    
    def parse_sql_semantics(self, sql_query: str) -> Optional[SqlSemantics]:
        """
        Parse complete SQL semantics from a PL/SQL query using SQLGlot.
        
        GAP 3: Enhanced with comprehensive column identification to ensure complete coverage.
        
        Args:
            sql_query: SQL query string to parse
            
        Returns:
            SqlSemantics object with complete metadata
        """
        if not sql_query or not isinstance(sql_query, str):
            return None
        
        self.validation_report['sql_statements_parsed'] += 1
        
        # Clean and normalize the SQL
        sql = self._normalize_sql(sql_query)
        
        # GAP 3: Try multiple parsing strategies for comprehensive coverage
        semantics = None
        
        # Strategy 1: Enhanced SQLGlot parsing with comprehensive column extraction
        if _HAS_SQLGLOT:
            semantics = self._parse_with_enhanced_sqlglot(sql)
        
        # Strategy 2: If SQLGlot fails or incomplete results, try regex-based comprehensive parsing
        if not semantics or self._is_semantics_incomplete(semantics):
            self.validation_report['regex_fallback_used'] += 1
            regex_semantics = self._extract_comprehensive_semantics_with_regex(sql)
            
            # Merge or use regex results if more complete
            if regex_semantics and self._is_more_complete(regex_semantics, semantics):
                semantics = regex_semantics
            elif semantics and regex_semantics:
                # Merge the best parts of both approaches
                semantics = self._merge_semantics(semantics, regex_semantics)
        
        # Strategy 3: Post-processing enhancement for any remaining gaps
        if semantics:
            semantics = self._enhance_semantics_completeness(semantics, sql)
        
        # GAP 4: Check for and parse dynamic SQL constructs
        if semantics:
            dynamic_sql_info = self._parse_dynamic_sql_constructs(sql)
            if dynamic_sql_info:
                semantics = self._enhance_with_dynamic_sql(semantics, dynamic_sql_info)
        
        return semantics
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for consistent parsing."""
        # Remove extra whitespace and normalize line breaks
        sql = ' '.join(sql.split())
        
        # Remove PL/SQL-specific constructs that might confuse parsing
        sql = re.sub(r'\s*INTO\s+[^F\s]+(?=\s+FROM)', ' ', sql, flags=re.IGNORECASE)
        
        # Ensure consistent spacing around keywords
        sql = re.sub(r'\s*(,)\s*', r'\1 ', sql)
        sql = re.sub(r'\s+(FROM|JOIN|WHERE|ON|AS|UNION|ORDER|GROUP)\s+', r' \1 ', sql, flags=re.IGNORECASE)
        
        return sql.strip()
    
    def _parse_with_enhanced_sqlglot(self, sql: str) -> Optional[SqlSemantics]:
        """GAP 3: Enhanced SQLGlot parsing with comprehensive column extraction."""
        try:
            # Use SQLGlot directly on Oracle SQL - let it handle everything
            # Only suppress logging output for cleaner console
            import warnings
            import logging
            
            # Suppress both warnings and SQLGlot logger output
            sqlglot_logger = logging.getLogger('sqlglot')
            original_level = sqlglot_logger.level
            sqlglot_logger.setLevel(logging.ERROR)
            
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    parsed = sqlglot.parse_one(sql, dialect="oracle", error_level=None)
            finally:
                sqlglot_logger.setLevel(original_level)
            
            if parsed:
                return self._extract_comprehensive_semantics_from_ast(parsed, sql)
        except Exception as e:
            self.logger.debug(f"Enhanced SQLGlot parsing failed: {e}")
            self.validation_report['sqlglot_parse_failures'] += 1
        
        return None
    
    def _is_semantics_incomplete(self, semantics: Optional[SqlSemantics]) -> bool:
        """Check if parsed semantics are incomplete and need enhancement."""
        if not semantics:
            return True
        
        # Check for various incompleteness indicators
        has_select_star = any(
            col.expression == '*' or col.column_name == '*' 
            for col in semantics.columns
        )
        
        # If we have SELECT * but no actual column details, it's incomplete
        if has_select_star and len(semantics.columns) <= 1:
            return True
        
        # If no columns extracted from a SELECT statement, it's incomplete
        if 'select' in semantics.original_query.lower() and not semantics.columns:
            return True
        
        # If no tables extracted but query contains FROM, it's incomplete
        if 'from' in semantics.original_query.lower() and not semantics.tables:
            return True
        
        return False
    
    def _is_more_complete(self, new_semantics: SqlSemantics, old_semantics: Optional[SqlSemantics]) -> bool:
        """Check if new semantics are more complete than old ones."""
        if not old_semantics:
            return True
        
        # Compare completeness metrics
        new_score = len(new_semantics.columns) + len(new_semantics.tables) + len(new_semantics.joins)
        old_score = len(old_semantics.columns) + len(old_semantics.tables) + len(old_semantics.joins)
        
        return new_score > old_score
    
    def _merge_semantics(self, primary: SqlSemantics, secondary: SqlSemantics) -> SqlSemantics:
        """Merge two semantics objects, taking the best parts of each."""
        # Use the one with more tables as primary structure
        if len(secondary.tables) > len(primary.tables):
            primary, secondary = secondary, primary
        
        # Merge columns, avoiding duplicates
        merged_columns = list(primary.columns)
        primary_expressions = {col.expression for col in primary.columns}
        
        for col in secondary.columns:
            if col.expression not in primary_expressions:
                merged_columns.append(col)
        
        # Merge tables, avoiding duplicates
        merged_tables = list(primary.tables)
        primary_table_names = {table.name for table in primary.tables}
        
        for table in secondary.tables:
            if table.name not in primary_table_names:
                merged_tables.append(table)
        
        # Merge joins, avoiding duplicates
        merged_joins = list(primary.joins)
        primary_join_sigs = {(j.left_table.name, j.right_table.name) for j in primary.joins}
        
        for join in secondary.joins:
            join_sig = (join.left_table.name, join.right_table.name)
            if join_sig not in primary_join_sigs:
                merged_joins.append(join)
        
        return SqlSemantics(
            original_query=primary.original_query,
            tables=merged_tables,
            joins=merged_joins,
            columns=merged_columns,
            inline_views=primary.inline_views + secondary.inline_views,
            where_clause=primary.where_clause or secondary.where_clause
        )
    
    def _extract_semantics_from_ast(self, parsed_ast, original_sql: str) -> SqlSemantics:
        """Extract semantics using SQLGlot AST."""
        tables = self._extract_tables_from_ast(parsed_ast)
        joins = self._extract_joins_from_ast(parsed_ast, tables)
        columns = self._extract_columns_from_ast(parsed_ast, tables)
        inline_views = self._extract_inline_views_from_ast(parsed_ast)
        where_clause = self._extract_where_from_ast(parsed_ast)
        
        self.validation_report['joins_extracted'] += len(joins)
        self.validation_report['inline_views_found'] += len(inline_views)
        self.validation_report['tables_resolved'] += len(tables)
        self.validation_report['columns_extracted'] += len(columns)
        
        return SqlSemantics(
            original_query=original_sql,
            tables=tables,
            joins=joins,
            columns=columns,
            inline_views=inline_views,
            where_clause=where_clause
        )
    
    def _extract_comprehensive_semantics_from_ast(self, parsed_ast, original_sql: str) -> SqlSemantics:
        """GAP 3: Enhanced semantics extraction with comprehensive column identification."""
        tables = self._extract_tables_from_ast_comprehensive(parsed_ast)
        joins = self._extract_joins_from_ast_comprehensive(parsed_ast, tables)
        columns = self._extract_columns_from_ast_comprehensive(parsed_ast, tables)
        inline_views = self._extract_inline_views_from_ast(parsed_ast)
        where_clause = self._extract_where_from_ast(parsed_ast)
        
        self.validation_report['joins_extracted'] += len(joins)
        self.validation_report['inline_views_found'] += len(inline_views)
        self.validation_report['tables_resolved'] += len(tables)
        self.validation_report['columns_extracted'] += len(columns)
        
        return SqlSemantics(
            original_query=original_sql,
            tables=tables,
            joins=joins,
            columns=columns,
            inline_views=inline_views,
            where_clause=where_clause
        )
    
    def _extract_tables_from_ast(self, ast) -> List[TableReference]:
        """Extract table references from SQLGlot AST."""
        tables = []
        
        # Find all table expressions
        for table_node in ast.find_all(Table):
            table_name = self._get_table_name_from_node(table_node)
            schema = self._get_schema_from_node(table_node)
            alias = self._get_alias_from_node(table_node)
            
            if table_name:
                # Filter out Oracle built-in functions that might be mistaken for tables
                if table_name.lower() in self.ORACLE_FUNCTIONS:
                    continue
                    
                # Validate alias is not a SQL keyword
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                
                tables.append(TableReference(
                    name=table_name,
                    schema=schema,
                    alias=alias
                ))
        
        return tables
    
    def _extract_tables_from_ast_comprehensive(self, ast) -> List[TableReference]:
        """GAP 3: Comprehensive table extraction with enhanced detection."""
        tables = []
        
        # Find all table expressions with multiple strategies
        for table_node in ast.find_all(Table):
            table_name = self._get_table_name_from_node(table_node)
            schema = self._get_schema_from_node(table_node)
            alias = self._get_alias_from_node(table_node)
            
            if table_name:
                # Filter out Oracle built-in functions that might be mistaken for tables
                if table_name.lower() in self.ORACLE_FUNCTIONS:
                    continue
                    
                # Validate alias is not a SQL keyword
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                
                tables.append(TableReference(
                    name=table_name,
                    schema=schema,
                    alias=alias
                ))
        
        # Additional extraction for complex cases that SQLGlot might miss
        # Check for tables in INSERT, UPDATE, MERGE statements
        if hasattr(ast, 'args'):
            # INSERT INTO table
            if hasattr(ast, 'this') and isinstance(ast.this, Table):
                table_name = self._get_table_name_from_node(ast.this)
                if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                    tables.append(TableReference(name=table_name))
            
            # UPDATE table
            if hasattr(ast, 'this') and ast.this:
                if isinstance(ast.this, Table):
                    table_name = self._get_table_name_from_node(ast.this)
                    if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                        tables.append(TableReference(name=table_name))
        
        # Remove duplicates while preserving order
        seen = set()
        unique_tables = []
        for table in tables:
            key = (table.name, table.schema, table.alias)
            if key not in seen:
                seen.add(key)
                unique_tables.append(table)
        
        return unique_tables
    
    def _extract_joins_from_ast(self, ast, tables: List[TableReference]) -> List[JoinRelationship]:
        """Extract JOIN relationships from SQLGlot AST."""
        joins = []
        
        for join_node in ast.find_all(Join):
            # Determine join type
            join_type = self._get_join_type_from_node(join_node)
            
            # Get right table (the table being joined)
            right_table = None
            if hasattr(join_node, 'this') and isinstance(join_node.this, Table):
                right_table_name = self._get_table_name_from_node(join_node.this)
                right_schema = self._get_schema_from_node(join_node.this)
                right_alias = self._get_alias_from_node(join_node.this)
                
                if right_table_name:
                    # Filter out Oracle built-in functions that might be mistaken for tables
                    if right_table_name.lower() in self.ORACLE_FUNCTIONS:
                        continue
                        
                    if right_alias and right_alias.lower() in self.SQL_KEYWORDS:
                        right_alias = None
                    right_table = TableReference(
                        name=right_table_name,
                        schema=right_schema,
                        alias=right_alias
                    )
            
            # Get left table (find the previous table in the FROM clause or join chain)
            left_table = None
            if tables:
                # For now, use the first table as left table
                # More sophisticated logic could track join chains
                left_table = tables[0]
                
                # Filter out Oracle functions from left table too
                if left_table.name.lower() in self.ORACLE_FUNCTIONS:
                    continue
            
            # Get join condition
            condition = ""
            if hasattr(join_node, 'on') and join_node.on:
                condition = join_node.on.sql(dialect="oracle")
            
            if left_table and right_table:
                joins.append(JoinRelationship(
                    join_type=join_type,
                    left_table=left_table,
                    right_table=right_table,
                    condition=condition,
                    raw_condition=condition
                ))
        
        return joins
    
    def _extract_columns_from_ast(self, ast, tables: List[TableReference]) -> List[ColumnExpression]:
        """Extract column expressions from SQLGlot AST."""
        columns = []
        
        # Find the main SELECT node
        select_node = ast if isinstance(ast, Select) else ast.find(Select)
        if not select_node or not hasattr(select_node, 'expressions'):
            return columns
        
        # Create table alias lookup
        alias_to_table = {}
        for table in tables:
            if table.alias:
                alias_to_table[table.alias] = table.name
        
        for expr in select_node.expressions:
            column_expr = self._extract_column_from_expression(expr, alias_to_table)
            if column_expr:
                columns.append(column_expr)
        
        return columns
    
    def _extract_column_from_expression(self, expr, alias_to_table: Dict[str, str]) -> Optional[ColumnExpression]:
        """Extract column information from a SELECT expression."""
        # Get the full expression as SQL
        expression_sql = expr.sql(dialect="oracle")
        
        # Get alias if present
        alias = None
        if hasattr(expr, 'alias') and expr.alias:
            alias = expr.alias
        
        # Extract source information
        source_table = None
        source_alias = None
        column_name = None
        
        # Handle simple column references
        if isinstance(expr, Column):
            column_name = expr.name
            if hasattr(expr, 'table') and expr.table:
                source_alias = expr.table
                source_table = alias_to_table.get(source_alias)
        
        return ColumnExpression(
            expression=expression_sql,
            alias=alias,
            source_table=source_table,
            source_alias=source_alias,
            column_name=column_name
        )
    
    def _extract_inline_views_from_ast(self, ast) -> List[InlineView]:
        """Extract inline views/subqueries from SQLGlot AST."""
        inline_views = []
        
        for subquery in ast.find_all(Subquery):
            alias = self._get_alias_from_node(subquery)
            if alias:
                sql = subquery.sql(dialect="oracle")
                
                # Extract base tables from the subquery
                base_tables = []
                for table_node in subquery.find_all(Table):
                    table_name = self._get_table_name_from_node(table_node)
                    if table_name:
                        base_tables.append(table_name)
                
                inline_views.append(InlineView(
                    alias=alias,
                    sql=sql,
                    base_tables=base_tables
                ))
        
        return inline_views
    
    def _extract_where_from_ast(self, ast) -> Optional[str]:
        """Extract WHERE clause from SQLGlot AST."""
        select_node = ast if isinstance(ast, Select) else ast.find(Select)
        if select_node and hasattr(select_node, 'where') and select_node.where:
            return select_node.where.sql(dialect="oracle")
        return None
    
    def _get_table_name_from_node(self, table_node) -> Optional[str]:
        """Extract table name from a table node."""
        if hasattr(table_node, 'this') and table_node.this:
            if isinstance(table_node.this, Identifier):
                return table_node.this.this
            elif hasattr(table_node.this, 'name'):
                return table_node.this.name
        return None
    
    def _get_schema_from_node(self, table_node) -> Optional[str]:
        """Extract schema name from a table node."""
        if hasattr(table_node, 'db') and table_node.db:
            if isinstance(table_node.db, Identifier):
                return table_node.db.this
            elif hasattr(table_node.db, 'name'):
                return table_node.db.name
        return None
    
    def _get_alias_from_node(self, node) -> Optional[str]:
        """Extract alias from a node."""
        if hasattr(node, 'alias') and node.alias:
            if isinstance(node.alias, Identifier):
                return node.alias.this
            elif hasattr(node.alias, 'name'):
                return node.alias.name
            elif isinstance(node.alias, str):
                return node.alias
        return None
    
    def _get_join_type_from_node(self, join_node) -> JoinType:
        """Extract join type from a join node."""
        if hasattr(join_node, 'kind') and join_node.kind:
            kind = join_node.kind.upper()
            if 'LEFT' in kind:
                return JoinType.LEFT
            elif 'RIGHT' in kind:
                return JoinType.RIGHT
            elif 'FULL' in kind:
                return JoinType.FULL
            elif 'CROSS' in kind:
                return JoinType.CROSS
        return JoinType.INNER
    
    def _extract_joins_from_ast_comprehensive(self, ast, tables: List[TableReference]) -> List[JoinRelationship]:
        """GAP 3: Comprehensive JOIN extraction with enhanced detection."""
        joins = []
        
        for join_node in ast.find_all(Join):
            # Determine join type
            join_type = self._get_join_type_from_node(join_node)
            
            # Get right table (the table being joined)
            right_table = None
            if hasattr(join_node, 'this') and isinstance(join_node.this, Table):
                right_table_name = self._get_table_name_from_node(join_node.this)
                right_schema = self._get_schema_from_node(join_node.this)
                right_alias = self._get_alias_from_node(join_node.this)
                
                if right_table_name:
                    # Filter out Oracle built-in functions that might be mistaken for tables
                    if right_table_name.lower() in self.ORACLE_FUNCTIONS:
                        continue
                        
                    if right_alias and right_alias.lower() in self.SQL_KEYWORDS:
                        right_alias = None
                    right_table = TableReference(
                        name=right_table_name,
                        schema=right_schema,
                        alias=right_alias
                    )
            
            # Get left table (find the previous table in the FROM clause or join chain)
            left_table = None
            if tables:
                # For now, use the first table as left table
                # More sophisticated logic could track join chains
                left_table = tables[0]
                
                # Filter out Oracle functions from left table too
                if left_table.name.lower() in self.ORACLE_FUNCTIONS:
                    continue
            
            # Get join condition
            condition = ""
            if hasattr(join_node, 'on') and join_node.on:
                condition = join_node.on.sql(dialect="oracle")
            
            if left_table and right_table:
                joins.append(JoinRelationship(
                    join_type=join_type,
                    left_table=left_table,
                    right_table=right_table,
                    condition=condition,
                    raw_condition=condition
                ))
        
        return joins
    
    def _extract_columns_from_ast_comprehensive(self, ast, tables: List[TableReference]) -> List[ColumnExpression]:
        """GAP 3: Comprehensive column extraction with enhanced detection."""
        columns = []
        
        # Find the main SELECT node
        select_node = ast if isinstance(ast, Select) else ast.find(Select)
        if not select_node or not hasattr(select_node, 'expressions'):
            return columns
        
        # Create table alias lookup
        alias_to_table = {}
        for table in tables:
            if table.alias:
                alias_to_table[table.alias] = table.name
        
        for expr in select_node.expressions:
            column_expr = self._extract_column_from_expression_comprehensive(expr, alias_to_table)
            if column_expr:
                columns.append(column_expr)
        
        # Additional extraction for complex expressions that might be missed
        # Handle function calls, arithmetic expressions, etc.
        for expr in select_node.expressions:
            if hasattr(expr, 'find_all'):
                # Extract column references from within complex expressions
                for col_ref in expr.find_all(Column):
                    if hasattr(col_ref, 'name') and col_ref.name:
                        # Check if this column is already captured
                        already_captured = any(
                            c.column_name == col_ref.name for c in columns
                        )
                        if not already_captured:
                            source_alias = col_ref.table if hasattr(col_ref, 'table') else None
                            source_table = alias_to_table.get(source_alias) if source_alias else None
                            
                            columns.append(ColumnExpression(
                                expression=col_ref.name,
                                column_name=col_ref.name,
                                source_table=source_table,
                                source_alias=source_alias
                            ))
        
        return columns
    
    def _extract_column_from_expression_comprehensive(self, expr, alias_to_table: Dict[str, str]) -> Optional[ColumnExpression]:
        """GAP 3: Enhanced column extraction from expressions."""
        # Get the full expression as SQL
        expression_sql = expr.sql(dialect="oracle")
        
        # Get alias if present
        alias = None
        if hasattr(expr, 'alias') and expr.alias:
            alias = expr.alias
        
        # Extract source information
        source_table = None
        source_alias = None
        column_name = None
        
        # Handle simple column references
        if isinstance(expr, Column):
            column_name = expr.name
            if hasattr(expr, 'table') and expr.table:
                source_alias = expr.table
                source_table = alias_to_table.get(source_alias)
        else:
            # For complex expressions, try to extract column names
            if hasattr(expr, 'find_all'):
                col_refs = list(expr.find_all(Column))
                if len(col_refs) == 1:
                    # Single column reference in expression
                    col_ref = col_refs[0]
                    column_name = col_ref.name
                    if hasattr(col_ref, 'table') and col_ref.table:
                        source_alias = col_ref.table
                        source_table = alias_to_table.get(source_alias)
        
        return ColumnExpression(
            expression=expression_sql,
            alias=alias,
            source_table=source_table,
            source_alias=source_alias,
            column_name=column_name
        )
    
    def _extract_comprehensive_semantics_with_regex(self, sql: str) -> SqlSemantics:
        """GAP 3: Comprehensive regex-based semantic extraction with enhanced column detection."""
        tables = self._extract_table_references_regex_comprehensive(sql)
        joins = self._extract_join_relationships_regex(sql, tables)
        columns = self._extract_column_expressions_regex_comprehensive(sql, tables)
        inline_views = []  # Not easily extractable with regex
        where_clause = self._extract_where_clause_regex(sql)
        
        return SqlSemantics(
            original_query=sql,
            tables=tables,
            joins=joins,
            columns=columns,
            inline_views=inline_views,
            where_clause=where_clause
        )
    
    def _extract_table_references_regex_comprehensive(self, sql: str) -> List[TableReference]:
        """GAP 3: Enhanced table extraction using multiple regex patterns."""
        tables = []
        
        # Strategy 1: Standard FROM and JOIN clauses
        # FROM clause
        from_pattern = r'FROM\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        from_match = re.search(from_pattern, sql, re.IGNORECASE)
        if from_match:
            schema = from_match.group(1)
            table_name = from_match.group(2)
            alias = from_match.group(3)
            
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                tables.append(TableReference(name=table_name, schema=schema, alias=alias))
        
        # JOIN clauses
        join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+OUTER\s+|CROSS\s+)?JOIN\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        for join_match in re.finditer(join_pattern, sql, re.IGNORECASE):
            schema = join_match.group(1)
            table_name = join_match.group(2)
            alias = join_match.group(3)
            
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                tables.append(TableReference(name=table_name, schema=schema, alias=alias))
        
        # Strategy 2: INSERT INTO, UPDATE, MERGE target tables
        insert_pattern = r'INSERT\s+INTO\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        insert_match = re.search(insert_pattern, sql, re.IGNORECASE)
        if insert_match:
            schema = insert_match.group(1)
            table_name = insert_match.group(2)
            alias = insert_match.group(3)
            
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                tables.append(TableReference(name=table_name, schema=schema, alias=alias))
        
        # UPDATE pattern
        update_pattern = r'UPDATE\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        update_match = re.search(update_pattern, sql, re.IGNORECASE)
        if update_match:
            schema = update_match.group(1)
            table_name = update_match.group(2)
            alias = update_match.group(3)
            
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                tables.append(TableReference(name=table_name, schema=schema, alias=alias))
        
        # Remove duplicates while preserving order
        seen = set()
        unique_tables = []
        for table in tables:
            key = (table.name, table.schema, table.alias)
            if key not in seen:
                seen.add(key)
                unique_tables.append(table)
        
        return unique_tables
    
    def _extract_column_expressions_regex_comprehensive(self, sql: str, tables: List[TableReference]) -> List[ColumnExpression]:
        """GAP 3: Enhanced column extraction using comprehensive regex patterns."""
        columns = []
        
        # Find SELECT clause with enhanced pattern
        select_patterns = [
            r'SELECT\s+(.*?)\s+FROM',
            r'SELECT\s+(.*?)$'  # For SELECT without FROM
        ]
        
        select_clause = None
        for pattern in select_patterns:
            select_match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
            if select_match:
                select_clause = select_match.group(1).strip()
                break
        
        if not select_clause:
            return columns
        
        # Handle SELECT * by returning placeholder
        if select_clause.strip() == '*':
            return [ColumnExpression(expression='*', column_name='*')]
        
        # Enhanced column splitting that handles nested functions and expressions
        column_expressions = self._smart_split_columns(select_clause)
        
        # Create table alias lookup
        alias_to_table = {}
        for table in tables:
            if table.alias:
                alias_to_table[table.alias] = table.name
        
        for expr in column_expressions:
            if not expr.strip():
                continue
            
            # Enhanced parsing for each column expression
            column_info = self._parse_column_expression_comprehensive(expr, alias_to_table)
            if column_info:
                columns.append(column_info)
        
        return columns
    
    def _smart_split_columns(self, select_clause: str) -> List[str]:
        """GAP 3: Smart column splitting that handles nested parentheses and functions."""
        columns = []
        current_column = ""
        paren_depth = 0
        in_string = False
        string_char = None
        
        for char in select_clause:
            if char in ('"', "'") and not in_string:
                in_string = True
                string_char = char
                current_column += char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
                current_column += char
            elif in_string:
                current_column += char
            elif char == '(':
                paren_depth += 1
                current_column += char
            elif char == ')':
                paren_depth -= 1
                current_column += char
            elif char == ',' and paren_depth == 0:
                if current_column.strip():
                    columns.append(current_column.strip())
                current_column = ""
            else:
                current_column += char
        
        # Add the last column
        if current_column.strip():
            columns.append(current_column.strip())
        
        return columns
    
    def _parse_column_expression_comprehensive(self, expr: str, alias_to_table: Dict[str, str]) -> Optional[ColumnExpression]:
        """GAP 3: Comprehensive parsing of individual column expressions."""
        expr = expr.strip()
        if not expr:
            return None
        
        # Check for alias (AS keyword or implicit)
        alias = None
        source_expr = expr
        
        # Pattern 1: explicit AS alias
        as_match = re.search(r'^(.+?)\s+AS\s+(\w+)$', expr, re.IGNORECASE)
        if as_match:
            source_expr = as_match.group(1).strip()
            alias = as_match.group(2)
        else:
            # Pattern 2: implicit alias (space-separated)
            implicit_match = re.search(r'^(.+?)\s+(\w+)$', expr)
            if implicit_match:
                potential_expr = implicit_match.group(1).strip()
                potential_alias = implicit_match.group(2)
                
                # Only treat as alias if it's not a SQL keyword and the expression looks complex
                if (potential_alias.lower() not in self.SQL_KEYWORDS and 
                    ('(' in potential_expr or '.' in potential_expr or 
                     any(func in potential_expr.lower() for func in ['count', 'sum', 'avg', 'max', 'min']))):
                    source_expr = potential_expr
                    alias = potential_alias
        
        # Extract source information
        source_table = None
        source_alias = None
        column_name = None
        
        # Pattern 1: table.column format
        table_col_match = re.match(r'^(\w+)\.(\w+)$', source_expr)
        if table_col_match:
            source_alias = table_col_match.group(1)
            column_name = table_col_match.group(2)
            source_table = alias_to_table.get(source_alias)
        elif re.match(r'^\w+$', source_expr):
            # Simple column name
            column_name = source_expr
        else:
            # Complex expression - try to extract referenced columns
            col_refs = re.findall(r'\b(\w+)\.(\w+)\b', source_expr)
            if col_refs:
                # Use the first column reference found
                source_alias = col_refs[0][0]
                column_name = col_refs[0][1]
                source_table = alias_to_table.get(source_alias)
        
        return ColumnExpression(
            expression=expr,
            alias=alias,
            source_table=source_table,
            source_alias=source_alias,
            column_name=column_name
        )
    
    def _enhance_semantics_completeness(self, semantics: SqlSemantics, original_sql: str) -> SqlSemantics:
        """GAP 3: Post-processing enhancement to fill any remaining gaps."""
        # Create enhanced copies
        enhanced_tables = list(semantics.tables)
        enhanced_columns = list(semantics.columns)
        enhanced_joins = list(semantics.joins)
        
        # Enhancement 1: If no columns but SELECT statement, add placeholder
        if 'select' in original_sql.lower() and not enhanced_columns:
            enhanced_columns.append(ColumnExpression(
                expression='*',
                column_name='*'
            ))
        
        # Enhancement 2: Extract additional table references from complex WHERE clauses
        if semantics.where_clause:
            additional_tables = self._extract_tables_from_where_clause(semantics.where_clause)
            for table_name in additional_tables:
                # Check if table already exists
                if not any(t.name == table_name for t in enhanced_tables):
                    enhanced_tables.append(TableReference(name=table_name))
        
        # Enhancement 3: Infer missing table information for columns
        for column in enhanced_columns:
            if column.source_table is None and column.source_alias:
                # Try to find matching table by alias
                for table in enhanced_tables:
                    if table.alias == column.source_alias:
                        column.source_table = table.name
                        break
        
        return SqlSemantics(
            original_query=semantics.original_query,
            tables=enhanced_tables,
            joins=enhanced_joins,
            columns=enhanced_columns,
            inline_views=semantics.inline_views,
            where_clause=semantics.where_clause
        )
    
    def _extract_tables_from_where_clause(self, where_clause: str) -> List[str]:
        """Extract additional table references from WHERE clause."""
        tables = []
        
        # Look for table.column patterns in WHERE clause
        table_col_pattern = r'\b(\w+)\.(\w+)\b'
        matches = re.findall(table_col_pattern, where_clause)
        
        for table_alias, column in matches:
            if (table_alias.lower() not in self.SQL_KEYWORDS and 
                table_alias.lower() not in self.ORACLE_FUNCTIONS):
                tables.append(table_alias)
        
        # Remove duplicates
        return list(set(tables))
    
    def _parse_dynamic_sql_constructs(self, sql: str) -> Optional[Dict[str, Any]]:
        """
        GAP 4: Parse dynamic SQL constructs like EXECUTE IMMEDIATE, DBMS_SQL, etc.
        
        Args:
            sql: SQL statement that may contain dynamic SQL constructs
            
        Returns:
            Dictionary with dynamic SQL metadata or None if no dynamic SQL found
        """
        dynamic_info = {
            "types": [],
            "execute_immediate_statements": [],
            "dbms_sql_usage": [],
            "dynamic_table_names": [],
            "dynamic_column_names": [],
            "concatenated_sql": [],
            "sql_injection_risks": []
        }
        
        has_dynamic_sql = False
        
        # Check for EXECUTE IMMEDIATE statements
        execute_immediate_patterns = [
            r'EXECUTE\s+IMMEDIATE\s+(.*?)(?:USING|INTO|;|$)',
            r'EXEC\s+IMMEDIATE\s+(.*?)(?:USING|INTO|;|$)'
        ]
        
        for pattern in execute_immediate_patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL)
            for match in matches:
                has_dynamic_sql = True
                dynamic_sql_expr = match.group(1).strip()
                
                # Parse the dynamic SQL expression
                parsed_expr = self._parse_dynamic_sql_expression(dynamic_sql_expr)
                dynamic_info["execute_immediate_statements"].append({
                    "expression": dynamic_sql_expr,
                    "parsed_info": parsed_expr,
                    "position": match.span()
                })
                
                if "execute_immediate" not in dynamic_info["types"]:
                    dynamic_info["types"].append("execute_immediate")
        
        # Check for DBMS_SQL package usage
        dbms_sql_patterns = [
            r'DBMS_SQL\.\w+',
            r'dbms_sql\.\w+'
        ]
        
        for pattern in dbms_sql_patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE)
            for match in matches:
                has_dynamic_sql = True
                function_call = match.group(0)
                dynamic_info["dbms_sql_usage"].append({
                    "function": function_call,
                    "position": match.span()
                })
                
                if "dbms_sql" not in dynamic_info["types"]:
                    dynamic_info["types"].append("dbms_sql")
        
        # Check for string concatenation that might build SQL
        concatenation_patterns = [
            r"('[^']*'\s*\|\|\s*\w+\s*\|\|\s*'[^']*')",  # 'SELECT * FROM ' || table_name || ' WHERE'
            r'("[^"]*"\s*\|\|\s*\w+\s*\|\|\s*"[^"]*")',  # "SELECT * FROM " || table_name || " WHERE"
            r'(\w+\s*\|\|\s*\'[^\']*\')',  # variable || ' clause'
            r'(\w+\s*\|\|\s*"[^"]*")'     # variable || " clause"
        ]
        
        for pattern in concatenation_patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE)
            for match in matches:
                has_dynamic_sql = True
                concat_expr = match.group(1)
                dynamic_info["concatenated_sql"].append({
                    "expression": concat_expr,
                    "position": match.span(),
                    "risk_level": self._assess_sql_injection_risk(concat_expr)
                })
                
                if "string_concatenation" not in dynamic_info["types"]:
                    dynamic_info["types"].append("string_concatenation")
        
        # Extract dynamic table and column names
        dynamic_tables = self._extract_dynamic_table_names(sql)
        dynamic_columns = self._extract_dynamic_column_names(sql)
        
        if dynamic_tables:
            has_dynamic_sql = True
            dynamic_info["dynamic_table_names"] = dynamic_tables
            if "dynamic_tables" not in dynamic_info["types"]:
                dynamic_info["types"].append("dynamic_tables")
        
        if dynamic_columns:
            has_dynamic_sql = True
            dynamic_info["dynamic_column_names"] = dynamic_columns
            if "dynamic_columns" not in dynamic_info["types"]:
                dynamic_info["types"].append("dynamic_columns")
        
        # Assess SQL injection risks
        injection_risks = self._detect_sql_injection_patterns(sql)
        if injection_risks:
            dynamic_info["sql_injection_risks"] = injection_risks
        
        return dynamic_info if has_dynamic_sql else None
    
    def _parse_dynamic_sql_expression(self, expr: str) -> Dict[str, Any]:
        """Parse a dynamic SQL expression to extract metadata."""
        parsed = {
            "type": "unknown",
            "static_parts": [],
            "variable_parts": [],
            "table_references": [],
            "column_references": [],
            "complexity": "low"
        }
        
        # Check if it's a simple variable reference
        if re.match(r'^\w+$', expr.strip()):
            parsed["type"] = "variable_reference"
            parsed["variable_parts"] = [expr.strip()]
            parsed["complexity"] = "low"
        # Check if it's string concatenation
        elif '||' in expr:
            parsed["type"] = "concatenation"
            parsed["complexity"] = "medium"
            
            # Split by concatenation operator
            parts = [part.strip() for part in expr.split('||')]
            for part in parts:
                if part.startswith("'") and part.endswith("'"):
                    # Static string literal
                    static_part = part[1:-1]  # Remove quotes
                    parsed["static_parts"].append(static_part)
                    
                    # Look for SQL keywords in static parts
                    if any(keyword in static_part.upper() for keyword in ['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE']):
                        # Try to extract table/column references from static SQL
                        self._extract_references_from_static_sql(static_part, parsed)
                else:
                    # Variable reference
                    parsed["variable_parts"].append(part)
            
            if len(parsed["variable_parts"]) > 3:
                parsed["complexity"] = "high"
        else:
            parsed["type"] = "complex_expression"
            parsed["complexity"] = "high"
        
        return parsed
    
    def _extract_references_from_static_sql(self, static_sql: str, parsed: Dict[str, Any]):
        """Extract table and column references from static SQL parts."""
        # Look for table references after FROM, JOIN, UPDATE, INSERT INTO
        table_patterns = [
            r'FROM\s+(\w+)',
            r'JOIN\s+(\w+)',
            r'UPDATE\s+(\w+)',
            r'INSERT\s+INTO\s+(\w+)'
        ]
        
        for pattern in table_patterns:
            matches = re.finditer(pattern, static_sql, re.IGNORECASE)
            for match in matches:
                table_name = match.group(1)
                if table_name not in parsed["table_references"]:
                    parsed["table_references"].append(table_name)
        
        # Look for column references (simple pattern)
        column_patterns = [
            r'SELECT\s+([\w\s,*]+)\s+FROM',
            r'(\w+)\s*=\s*',
            r'WHERE\s+(\w+)',
            r'ORDER\s+BY\s+([\w\s,]+)'
        ]
        
        for pattern in column_patterns:
            matches = re.finditer(pattern, static_sql, re.IGNORECASE)
            for match in matches:
                column_ref = match.group(1).strip()
                if column_ref != '*' and column_ref not in parsed["column_references"]:
                    parsed["column_references"].append(column_ref)
    
    def _extract_dynamic_table_names(self, sql: str) -> List[Dict[str, Any]]:
        """Extract dynamically constructed table names."""
        dynamic_tables = []
        
        # Look for patterns like: 'table_' || suffix
        table_concat_patterns = [
            r"'(\w+[_]?)'\s*\|\|\s*(\w+)",  # 'table_' || suffix
            r"(\w+)\s*\|\|\s*'([_]?\w*)'",  # prefix || '_table'
            r'"(\w+[_]?)"\s*\|\|\s*(\w+)',  # "table_" || suffix
            r'(\w+)\s*\|\|\s*"([_]?\w*)"'   # prefix || "_table"
        ]
        
        for pattern in table_concat_patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE)
            for match in matches:
                dynamic_tables.append({
                    "pattern": match.group(0),
                    "static_part": match.group(1),
                    "variable_part": match.group(2),
                    "position": match.span()
                })
        
        return dynamic_tables
    
    def _extract_dynamic_column_names(self, sql: str) -> List[Dict[str, Any]]:
        """Extract dynamically constructed column names."""
        dynamic_columns = []
        
        # Look for patterns in SELECT clauses with concatenation
        select_patterns = [
            r"SELECT\s+.*?(\w+\s*\|\|\s*'[^']*')",
            r"SELECT\s+.*?('[^']*'\s*\|\|\s*\w+)"
        ]
        
        for pattern in select_patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL)
            for match in matches:
                dynamic_columns.append({
                    "pattern": match.group(1),
                    "context": "SELECT",
                    "position": match.span()
                })
        
        return dynamic_columns
    
    def _assess_sql_injection_risk(self, expr: str) -> str:
        """Assess SQL injection risk level for a concatenated expression."""
        # High risk indicators
        high_risk_patterns = [
            r'\w+\s*\|\|\s*\'[^\']*WHERE[^\']*\'',  # Direct WHERE clause concatenation
            r'\w+\s*\|\|\s*\'[^\']*OR[^\']*\'',     # OR injection pattern
            r'\w+\s*\|\|\s*\'[^\']*UNION[^\']*\'',  # UNION injection
            r'\w+\s*\|\|\s*\'\s*;\s*\'',            # Statement termination
        ]
        
        for pattern in high_risk_patterns:
            if re.search(pattern, expr, re.IGNORECASE):
                return "high"
        
        # Medium risk: user input directly concatenated
        if re.search(r'\w+\s*\|\|\s*\w+\s*\|\|\s*\w+', expr):
            return "medium"
        
        # Low risk: simple concatenation with constants
        return "low"
    
    def _detect_sql_injection_patterns(self, sql: str) -> List[Dict[str, Any]]:
        """Detect potential SQL injection vulnerabilities."""
        risks = []
        
        # Pattern 1: Unparameterized string concatenation in WHERE clauses
        where_concat_pattern = r'WHERE\s+[^=]*\s*\|\|\s*\w+\s*\|\|'
        matches = re.finditer(where_concat_pattern, sql, re.IGNORECASE)
        for match in matches:
            risks.append({
                "type": "unparameterized_where_clause",
                "severity": "high",
                "pattern": match.group(0),
                "position": match.span(),
                "recommendation": "Use bind variables or parameterized queries"
            })
        
        # Pattern 2: Dynamic SQL without proper escaping
        unescaped_patterns = [
            r"EXECUTE\s+IMMEDIATE\s+\w+\s*\|\|\s*\w+",  # EXECUTE IMMEDIATE with concatenation
            r"EXECUTE\s+IMMEDIATE\s+'\s*\w+\s*'\s*\|\|\s*\w+"
        ]
        
        for pattern in unescaped_patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE)
            for match in matches:
                risks.append({
                    "type": "unescaped_dynamic_sql",
                    "severity": "medium",
                    "pattern": match.group(0),
                    "position": match.span(),
                    "recommendation": "Use DBMS_ASSERT for input validation"
                })
        
        return risks
    
    def _enhance_with_dynamic_sql(self, semantics: SqlSemantics, dynamic_info: Dict[str, Any]) -> SqlSemantics:
        """GAP 4: Enhance semantics with dynamic SQL information."""
        # Create a new SqlSemantics object with dynamic SQL info
        enhanced_semantics = SqlSemantics(
            original_query=semantics.original_query,
            tables=semantics.tables,
            joins=semantics.joins,
            columns=semantics.columns,
            inline_views=semantics.inline_views,
            where_clause=semantics.where_clause,
            dynamic_sql_info=dynamic_info
        )
        
        # Add dynamic table references to the tables list
        for table_info in dynamic_info.get("dynamic_table_names", []):
            static_part = table_info.get("static_part", "")
            if static_part and not any(t.name == static_part for t in enhanced_semantics.tables):
                enhanced_semantics.tables.append(TableReference(
                    name=static_part,
                    alias=None,
                    schema=None
                ))
        
        # Extract tables from EXECUTE IMMEDIATE statements
        for exec_stmt in dynamic_info.get("execute_immediate_statements", []):
            parsed_info = exec_stmt.get("parsed_info", {})
            for table_ref in parsed_info.get("table_references", []):
                if not any(t.name == table_ref for t in enhanced_semantics.tables):
                    enhanced_semantics.tables.append(TableReference(
                        name=table_ref,
                        alias=None,
                        schema=None
                    ))
        
        return enhanced_semantics
    
    def _extract_semantics_with_regex(self, sql: str) -> SqlSemantics:
        """Fallback regex-based semantic extraction."""
        tables = self._extract_table_references_regex(sql)
        joins = self._extract_join_relationships_regex(sql, tables)
        columns = self._extract_column_expressions_regex(sql, tables)
        inline_views = []  # Not easily extractable with regex
        where_clause = self._extract_where_clause_regex(sql)
        
        return SqlSemantics(
            original_query=sql,
            tables=tables,
            joins=joins,
            columns=columns,
            inline_views=inline_views,
            where_clause=where_clause
        )
    
    def _extract_table_references_regex(self, sql: str) -> List[TableReference]:
        """Extract table references using regex patterns."""
        tables = []
        
        # FROM clause
        from_pattern = r'FROM\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        from_match = re.search(from_pattern, sql, re.IGNORECASE)
        if from_match:
            schema = from_match.group(1)
            table_name = from_match.group(2)
            alias = from_match.group(3)
            
            # Filter out Oracle built-in functions that might be mistaken for tables
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                # Validate alias is not a SQL keyword
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                
                tables.append(TableReference(name=table_name, schema=schema, alias=alias))
        
        # JOIN clauses
        join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+OUTER\s+|CROSS\s+)?JOIN\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        for join_match in re.finditer(join_pattern, sql, re.IGNORECASE):
            schema = join_match.group(1)
            table_name = join_match.group(2)
            alias = join_match.group(3)
            
            # Filter out Oracle built-in functions that might be mistaken for tables
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                # Validate alias is not a SQL keyword
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                
                tables.append(TableReference(name=table_name, schema=schema, alias=alias))
        
        return tables
    
    def _extract_join_relationships_regex(self, sql: str, tables: List[TableReference]) -> List[JoinRelationship]:
        """Extract JOIN relationships using regex patterns."""
        joins = []
        
        join_pattern = r'((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+OUTER\s+|CROSS\s+)?JOIN)\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+([^$]+?)(?=\s*(?:INNER|LEFT|RIGHT|FULL|CROSS|WHERE|ORDER|GROUP|HAVING|$))'
        
        for join_match in re.finditer(join_pattern, sql, re.IGNORECASE | re.DOTALL):
            join_type_raw = join_match.group(1).strip().upper()
            if join_type_raw == 'JOIN':
                join_type_raw = 'INNER JOIN'
            
            try:
                join_type = JoinType(join_type_raw)
            except ValueError:
                join_type = JoinType.INNER
            
            schema = join_match.group(2)
            table_name = join_match.group(3)
            alias = join_match.group(4)
            condition = join_match.group(5).strip()
            
            # Filter out Oracle built-in functions that might be mistaken for tables
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                # Validate alias is not a SQL keyword
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                
                right_table = TableReference(name=table_name, schema=schema, alias=alias)
                left_table = tables[0] if tables else TableReference(name="Unknown")
                
                # Filter out Oracle functions from left table too
                if left_table.name.lower() not in self.ORACLE_FUNCTIONS:
                    joins.append(JoinRelationship(
                        join_type=join_type,
                        left_table=left_table,
                        right_table=right_table,
                        condition=condition,
                        raw_condition=condition
                    ))
        
        return joins
    
    def _extract_column_expressions_regex(self, sql: str, tables: List[TableReference]) -> List[ColumnExpression]:
        """Extract column expressions using regex patterns."""
        columns = []
        
        # Find SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return columns
        
        select_clause = select_match.group(1).strip()
        
        # Skip if SELECT *
        if select_clause.strip() == '*':
            return columns
        
        # Split by commas (simple approach)
        column_expressions = [expr.strip() for expr in select_clause.split(',')]
        
        # Create table alias lookup
        alias_to_table = {}
        for table in tables:
            if table.alias:
                alias_to_table[table.alias] = table.name
        
        for expr in column_expressions:
            if not expr:
                continue
            
            # Check for alias (AS keyword)
            as_match = re.search(r'^(.+?)\s+AS\s+(\w+)$', expr, re.IGNORECASE)
            if as_match:
                source_expr = as_match.group(1).strip()
                alias = as_match.group(2)
            else:
                source_expr = expr
                alias = None
            
            # Extract source information
            source_table = None
            source_alias = None
            column_name = None
            
            # Check for table.column format
            table_col_match = re.match(r'^(\w+)\.(\w+)$', source_expr)
            if table_col_match:
                source_alias = table_col_match.group(1)
                column_name = table_col_match.group(2)
                source_table = alias_to_table.get(source_alias)
            else:
                column_name = source_expr
            
            columns.append(ColumnExpression(
                expression=expr,
                alias=alias,
                source_table=source_table,
                source_alias=source_alias,
                column_name=column_name
            ))
        
        return columns
    
    def _extract_where_clause_regex(self, sql: str) -> Optional[str]:
        """Extract WHERE clause using regex."""
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+(?:ORDER|GROUP|HAVING|$))', sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            return where_match.group(1).strip()
        return None

def create_join_edges_from_semantics(semantics: SqlSemantics) -> List[Dict[str, Any]]:
    """
    Create graph edges from SQL semantics for integration into MetaZCode graph.
    
    Args:
        semantics: Parsed SQL semantics
        
    Returns:
        List of edge dictionaries ready for graph integration
    """
    edges = []
    
    for join in semantics.joins:
        # Create JOIN edge between tables
        edge = {
            "source_id": f"table::{join.left_table.name}",
            "target_id": f"table::{join.right_table.name}",
            "edge_type": "REFERENCES",
            "properties": {
                "join_type": join.join_type.value,
                "condition": join.condition,
                "left_alias": join.left_table.alias,
                "right_alias": join.right_table.alias,
                "raw_condition": join.raw_condition,
                "relationship_type": "join_relationship"
            }
        }
        edges.append(edge)
    
    return edges
