#!/usr/bin/env python3
"""Test various SQL patterns to identify what the parser needs to handle"""

import re

def test_sql_patterns():
    """Test various SQL patterns that might appear in SSIS packages"""
    
    # Test cases covering different SQL patterns
    test_cases = [
        # 1. Current failing case from Product.dtsx
        {
            "name": "Product.dtsx - Categories JOIN",
            "sql": """SELECT p.ProductID, p.ProductName, c.CategoryName
FROM Products AS p INNER JOIN
     Categories AS c ON p.CategoryID = c.CategoryID""",
            "expected": ["Products", "Categories"]
        },
        
        # 2. Schema-qualified tables
        {
            "name": "Schema qualified tables",
            "sql": "SELECT * FROM [dbo].[Customers] c INNER JOIN [dbo].[Orders] o ON c.ID = o.CustomerID",
            "expected": ["dbo.Customers", "dbo.Orders"]
        },
        
        # 3. Mixed bracket styles
        {
            "name": "Mixed bracket styles",
            "sql": "SELECT * FROM dbo.Products p JOIN [Categories] c ON p.CategoryID = c.ID",
            "expected": ["dbo.Products", "Categories"]
        },
        
        # 4. Multiple JOINs
        {
            "name": "Multiple JOINs",
            "sql": """SELECT * FROM Orders o 
                     LEFT JOIN Customers c ON o.CustomerID = c.ID
                     RIGHT JOIN Products p ON o.ProductID = p.ID
                     FULL OUTER JOIN Categories cat ON p.CategoryID = cat.ID""",
            "expected": ["Orders", "Customers", "Products", "Categories"]
        },
        
        # 5. Database prefixed tables
        {
            "name": "Database prefixed",
            "sql": "SELECT * FROM MyDB.dbo.Orders o JOIN AnotherDB.dbo.Customers c ON o.CustomerID = c.ID",
            "expected": ["MyDB.dbo.Orders", "AnotherDB.dbo.Customers"]
        },
        
        # 6. Temporary tables
        {
            "name": "Temporary tables",
            "sql": "SELECT * FROM #TempOrders t JOIN ##GlobalTemp g ON t.ID = g.OrderID",
            "expected": ["#TempOrders", "##GlobalTemp"]
        },
        
        # 7. Table variables
        {
            "name": "Table variables",
            "sql": "SELECT * FROM @Orders o JOIN @Customers c ON o.CustomerID = c.ID",
            "expected": ["@Orders", "@Customers"]
        },
        
        # 8. Complex whitespace and newlines
        {
            "name": "Complex whitespace",
            "sql": """SELECT    *    FROM
                           Products     AS     p
                      INNER    JOIN
                           Categories    AS    c    ON    p.CategoryID = c.ID""",
            "expected": ["Products", "Categories"]
        },
        
        # 9. Subqueries (should NOT extract inner tables)
        {
            "name": "Subqueries",
            "sql": "SELECT * FROM (SELECT * FROM InnerTable) AS derived JOIN OuterTable ON 1=1",
            "expected": ["OuterTable"]  # Should not extract InnerTable from subquery
        },
        
        # 10. No alias
        {
            "name": "No alias",
            "sql": "SELECT * FROM Products JOIN Categories ON Products.CategoryID = Categories.ID",
            "expected": ["Products", "Categories"]
        },
        
        # 11. Single table
        {
            "name": "Single table",
            "sql": "SELECT * FROM [dbo].[Products]",
            "expected": ["dbo.Products"]
        },
        
        # 12. Common table expressions (should extract main tables only)
        {
            "name": "CTE",
            "sql": """WITH ProductCTE AS (SELECT * FROM Products)
                     SELECT * FROM ProductCTE p JOIN Categories c ON p.CategoryID = c.ID""",
            "expected": ["Categories"]  # CTE reference shouldn't count as table
        }
    ]
    
    # Current parser regex (the broken one)
    current_regex = r"(?:FROM|JOIN)\s+\[?(\w+)\]?\.\[?(\w+)\]?"
    
    # Proposed improved regex patterns to test
    regex_patterns = [
        {
            "name": "Current (broken)",
            "pattern": r"(?:FROM|JOIN)\s+\[?(\w+)\]?\.\[?(\w+)\]?"
        },
        {
            "name": "Simple improvement",
            "pattern": r"(?:FROM|JOIN)\s+(?:\[?([^\s\[\]]+)\]?\.\[?([^\s\[\]]+)\]?|\[?([^\s\[\]]+)\]?)(?:\s+(?:AS\s+)?\w+)?"
        },
        {
            "name": "Comprehensive pattern",
            "pattern": r"(?:FROM|JOIN)\s+(?:(?:\[?([^.\s\[\]]+)\]?\.)?(?:\[?([^.\s\[\]]+)\]?\.)?)\[?([^.\s\[\]]+)\]?(?:\s+(?:AS\s+)?\w+)?"
        },
        {
            "name": "Multi-step approach",
            "pattern": None  # Will use custom logic
        }
    ]
    
    print("🧪 Testing SQL Pattern Recognition")
    print("=" * 80)
    
    for test_case in test_cases:
        print(f"\n📋 Test Case: {test_case['name']}")
        print(f"SQL: {test_case['sql']}")
        print(f"Expected: {test_case['expected']}")
        print("-" * 40)
        
        for regex_info in regex_patterns:
            pattern_name = regex_info['name']
            pattern = regex_info['pattern']
            
            if pattern is None:
                # Custom multi-step approach
                extracted = extract_tables_multistep(test_case['sql'])
            else:
                extracted = extract_tables_with_regex(test_case['sql'], pattern)
            
            # Compare results
            success = set(extracted) == set(test_case['expected'])
            status = "✅" if success else "❌"
            
            print(f"  {status} {pattern_name}: {extracted}")
    
    return test_cases

def extract_tables_with_regex(sql, pattern):
    """Extract tables using a regex pattern"""
    matches = re.findall(pattern, sql, re.IGNORECASE | re.MULTILINE)
    tables = []
    
    for match in matches:
        if isinstance(match, tuple):
            # Handle multiple capture groups
            # Find the non-empty groups and combine them
            parts = [part for part in match if part]
            if len(parts) >= 2:
                # Assume last two parts are schema.table
                tables.append(f"{parts[-2]}.{parts[-1]}")
            elif len(parts) == 1:
                tables.append(parts[0])
        else:
            tables.append(match)
    
    return tables

def extract_tables_multistep(sql):
    """Extract tables using a multi-step approach (more robust)"""
    tables = []
    
    # Step 1: Remove subqueries and CTEs (simplified)
    # This is a basic approach - in production you'd want a proper SQL parser
    clean_sql = sql
    
    # Remove simple CTEs (very basic)
    clean_sql = re.sub(r'WITH\s+\w+\s+AS\s*\([^)]+\)', '', clean_sql, flags=re.IGNORECASE | re.MULTILINE)
    
    # Step 2: Find FROM and JOIN clauses
    # Pattern to match various table reference formats
    table_pattern = r"""
        (?:FROM|JOIN)\s+                    # FROM or JOIN keyword
        (?:
            (?P<db>\w+)\.(?P<schema>\w+)\.(?P<table1>\w+)  |    # db.schema.table
            (?P<schema2>\w+)\.(?P<table2>\w+)  |               # schema.table
            (?P<temp>[#@]\w+)  |                               # temp tables/variables
            (?P<simple>\w+)                                    # simple table name
        )
        (?:\s+(?:AS\s+)?\w+)?                # optional alias
    """
    
    # Also handle bracketed versions
    bracket_pattern = r"""
        (?:FROM|JOIN)\s+                    # FROM or JOIN keyword
        (?:
            (?:\[(?P<db>\w+)\]\.)?\[(?P<schema>\w+)\]\.\[(?P<table1>\w+)\]  |  # [db].[schema].[table]
            \[(?P<schema2>\w+)\]\.\[(?P<table2>\w+)\]  |                      # [schema].[table]
            \[(?P<simple>\w+)\]                                               # [table]
        )
        (?:\s+(?:AS\s+)?\w+)?                # optional alias
    """
    
    # Combine both patterns
    combined_pattern = f"({table_pattern}|{bracket_pattern})"
    
    matches = re.finditer(combined_pattern, clean_sql, re.IGNORECASE | re.MULTILINE | re.VERBOSE)
    
    for match in matches:
        groups = match.groupdict()
        
        # Find which group matched
        if groups.get('db') and groups.get('schema') and groups.get('table1'):
            tables.append(f"{groups['db']}.{groups['schema']}.{groups['table1']}")
        elif groups.get('schema2') and groups.get('table2'):
            tables.append(f"{groups['schema2']}.{groups['table2']}")
        elif groups.get('temp'):
            tables.append(groups['temp'])
        elif groups.get('simple'):
            tables.append(groups['simple'])
    
    return list(set(tables))  # Remove duplicates

if __name__ == "__main__":
    test_sql_patterns()