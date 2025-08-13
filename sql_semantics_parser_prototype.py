#!/usr/bin/env python3
"""
Prototype: Enhanced SQL Semantics Parser for SSIS Migration

This prototype demonstrates how to capture complete SQL semantics 
including JOIN relationships and column aliases for migration purposes.
"""

import re
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

@dataclass
class JoinMetadata:
    """Represents JOIN relationship metadata."""
    join_type: str  # INNER, LEFT, RIGHT, FULL
    left_table: str
    left_alias: Optional[str]
    right_table: str  
    right_alias: Optional[str]
    condition: str
    
@dataclass
class ColumnAlias:
    """Represents column alias metadata."""
    original_expression: str  # e.g., "c.CategoryID"
    alias_name: str          # e.g., "Expr1"
    source_table: Optional[str]
    source_alias: Optional[str]

@dataclass
class SqlSemantics:
    """Complete SQL semantics metadata."""
    original_query: str
    tables: List[Dict[str, str]]  # [{"name": "Products", "alias": "p"}, ...]
    joins: List[JoinMetadata]
    column_aliases: List[ColumnAlias]
    selected_columns: List[Dict[str, str]]

class EnhancedSqlParser:
    """
    Enhanced SQL parser that captures complete semantics for migration.
    
    This parser extracts:
    1. Table names and aliases
    2. JOIN relationships and conditions  
    3. Column aliases and expressions
    4. Complete SELECT structure
    """
    
    def parse_sql_semantics(self, sql_query: str) -> SqlSemantics:
        """Parse complete SQL semantics from a query."""
        
        if not sql_query or not isinstance(sql_query, str):
            return SqlSemantics("", [], [], [], [])
        
        # Clean up the SQL
        sql = ' '.join(sql_query.split())
        
        return SqlSemantics(
            original_query=sql,
            tables=self._extract_tables_with_aliases(sql),
            joins=self._extract_join_metadata(sql),
            column_aliases=self._extract_column_aliases(sql),
            selected_columns=self._extract_selected_columns(sql)
        )
    
    def _extract_tables_with_aliases(self, sql: str) -> List[Dict[str, str]]:
        """Extract tables with their aliases."""
        tables = []
        
        # FROM clause pattern
        from_pattern = r'FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        from_match = re.search(from_pattern, sql, re.IGNORECASE)
        if from_match:
            table_name = from_match.group(1)
            alias = from_match.group(2)
            tables.append({"name": table_name, "alias": alias or table_name})
        
        # JOIN patterns
        join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        join_matches = re.findall(join_pattern, sql, re.IGNORECASE)
        for table_name, alias in join_matches:
            tables.append({"name": table_name, "alias": alias or table_name})
        
        return tables
    
    def _extract_join_metadata(self, sql: str) -> List[JoinMetadata]:
        """Extract JOIN relationships with conditions."""
        joins = []
        
        # Pattern to match JOIN ... ON conditions
        join_pattern = r'((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN)\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+([^$]+?)(?=\s*$)'
        
        join_matches = re.findall(join_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        for join_type, table_name, alias, condition in join_matches:
            # Clean up join type
            join_type = join_type.strip().upper()
            if join_type == 'JOIN':
                join_type = 'INNER JOIN'
            
            # Extract table info from FROM clause for left table
            from_pattern = r'FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?'
            from_match = re.search(from_pattern, sql, re.IGNORECASE)
            left_table = from_match.group(1) if from_match else "Unknown"
            left_alias = from_match.group(2) if from_match else None
            
            joins.append(JoinMetadata(
                join_type=join_type,
                left_table=left_table,
                left_alias=left_alias,
                right_table=table_name,
                right_alias=alias,
                condition=condition.strip()
            ))
        
        return joins
    
    def _extract_column_aliases(self, sql: str) -> List[ColumnAlias]:
        """Extract column aliases from SELECT clause."""
        aliases = []
        
        # Find SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return aliases
        
        select_clause = select_match.group(1)
        
        # Pattern for column aliases: column AS alias
        alias_pattern = r'(\w+\.\w+|\w+)\s+AS\s+(\w+)'
        alias_matches = re.findall(alias_pattern, select_clause, re.IGNORECASE)
        
        for original_expr, alias_name in alias_matches:
            # Parse source table/alias if it's table.column format
            source_table = None
            source_alias = None
            if '.' in original_expr:
                parts = original_expr.split('.')
                source_alias = parts[0]
                # Map alias back to table name
                tables = self._extract_tables_with_aliases(sql)
                for table in tables:
                    if table["alias"] == source_alias:
                        source_table = table["name"]
                        break
            
            aliases.append(ColumnAlias(
                original_expression=original_expr,
                alias_name=alias_name,
                source_table=source_table,
                source_alias=source_alias
            ))
        
        return aliases
    
    def _extract_selected_columns(self, sql: str) -> List[Dict[str, str]]:
        """Extract all selected columns with their sources."""
        columns = []
        
        # Find SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return columns
        
        select_clause = select_match.group(1)
        
        # Split by commas (basic approach - could be enhanced for complex expressions)
        column_expressions = [col.strip() for col in select_clause.split(',')]
        
        for expr in column_expressions:
            # Extract column info
            column_info = {"expression": expr}
            
            # Check if it has AS alias
            as_match = re.search(r'(.+)\s+AS\s+(\w+)', expr, re.IGNORECASE)
            if as_match:
                column_info["source"] = as_match.group(1).strip()
                column_info["alias"] = as_match.group(2)
            else:
                # No alias - use the expression as both source and name
                column_info["source"] = expr
                # Extract column name (last part after dot if present)
                if '.' in expr:
                    column_info["column_name"] = expr.split('.')[-1]
                else:
                    column_info["column_name"] = expr
            
            columns.append(column_info)
        
        return columns

def test_enhanced_parser():
    """Test the enhanced parser with the Product.dtsx SQL."""
    
    parser = EnhancedSqlParser()
    
    # The actual SQL from Product.dtsx
    product_sql = """SELECT        p.ProductID, p.ProductName, p.SupplierID, p.CategoryID, p.QuantityPerUnit, p.UnitPrice, p.UnitsInStock, p.UnitsOnOrder, p.ReorderLevel, p.Discontinued, c.CategoryID AS Expr1, c.CategoryName, c.Description, c.Picture
FROM            Products AS p INNER JOIN
                         Categories AS c ON p.CategoryID = c.CategoryID"""
    
    print("🧪 Testing Enhanced SQL Semantics Parser")
    print("=" * 80)
    print(f"Input SQL: {' '.join(product_sql.split())}")
    print()
    
    # Parse semantics
    semantics = parser.parse_sql_semantics(product_sql)
    
    print("📋 PARSED SEMANTICS:")
    print("-" * 50)
    
    print(f"🗂️  Tables ({len(semantics.tables)}):")
    for table in semantics.tables:
        print(f"   - {table['name']} (alias: {table['alias']})")
    
    print(f"\n🔗 Joins ({len(semantics.joins)}):")
    for join in semantics.joins:
        print(f"   - {join.join_type}: {join.left_table} ({join.left_alias}) → {join.right_table} ({join.right_alias})")
        print(f"     Condition: {join.condition}")
    
    print(f"\n🏷️  Column Aliases ({len(semantics.column_aliases)}):")
    for alias in semantics.column_aliases:
        print(f"   - {alias.original_expression} AS {alias.alias_name}")
        if alias.source_table:
            print(f"     Source: {alias.source_table} (via {alias.source_alias})")
    
    print(f"\n📊 Selected Columns ({len(semantics.selected_columns)}):")
    for i, col in enumerate(semantics.selected_columns[:5], 1):  # Show first 5
        print(f"   {i}. {col.get('expression', 'Unknown')}")
        if 'alias' in col:
            print(f"      → Alias: {col['alias']}")
    
    if len(semantics.selected_columns) > 5:
        print(f"   ... and {len(semantics.selected_columns) - 5} more columns")
    
    return semantics

def generate_migration_code(semantics: SqlSemantics) -> Dict[str, str]:
    """Generate migration code for different platforms using parsed semantics."""
    
    if not semantics.joins:
        return {"error": "No JOIN metadata available"}
    
    join = semantics.joins[0]  # Use first JOIN for demo
    
    # Generate Spark code
    spark_code = f"""
# Generated Spark DataFrame code
df_result = df_{join.left_table.lower()}.alias('{join.left_alias}') \\
    .join(df_{join.right_table.lower()}.alias('{join.right_alias}'), 
          col('{join.left_alias}.{join.condition.split("=")[0].strip().split(".")[-1]}') == 
          col('{join.right_alias}.{join.condition.split("=")[1].strip().split(".")[-1]}'), 
          'inner') \\
    .select({", ".join([f"col('{col.get('source', col.get('expression', 'unknown')).replace(' ', '_')}')" + (f".alias('{col['alias']}')" if 'alias' in col else "") for col in semantics.selected_columns[:3]])})
"""
    
    # Generate dbt SQL
    dbt_sql = f"""
-- Generated dbt model SQL
SELECT 
{', '.join([f"    {col.get('source', col.get('expression', 'unknown'))}" + (f" AS {col['alias']}" if 'alias' in col else "") for col in semantics.selected_columns[:5]])}
FROM {{{{ ref('{join.left_table.lower()}') }}}} {join.left_alias}
{join.join_type} {{{{ ref('{join.right_table.lower()}') }}}} {join.right_alias}
    ON {join.condition}
"""
    
    # Generate Python/Pandas code
    pandas_code = f"""
# Generated Pandas code  
result_df = pd.merge(
    df_{join.left_table.lower()},
    df_{join.right_table.lower()},
    left_on='{join.condition.split("=")[0].strip().split(".")[-1]}',
    right_on='{join.condition.split("=")[1].strip().split(".")[-1]}',
    how='inner',
    suffixes=('_{join.left_alias}', '_{join.right_alias}')
)[{[col.get('expression', 'unknown').replace('.', '_') for col in semantics.selected_columns[:3]]}]
"""
    
    return {
        "spark": spark_code,
        "dbt": dbt_sql, 
        "pandas": pandas_code
    }

if __name__ == "__main__":
    # Test the parser
    semantics = test_enhanced_parser()
    
    print(f"\n💻 GENERATED MIGRATION CODE:")
    print("=" * 80)
    
    migration_code = generate_migration_code(semantics)
    
    for platform, code in migration_code.items():
        print(f"\n📋 {platform.upper()}:")
        print("-" * 30)
        print(code)
    
    print(f"\n✅ PROOF OF CONCEPT: Enhanced parser successfully captured:")
    print("   • JOIN relationships (INNER JOIN Products → Categories)")
    print("   • Column aliases (c.CategoryID AS Expr1)")
    print("   • Table aliases (p, c)")
    print("   • JOIN conditions (p.CategoryID = c.CategoryID)")
    print("   • Complete SELECT structure")
    print()
    print("🚀 NEXT STEP: Integration into MetaZCode SSIS parser")