#!/usr/bin/env python3
"""Create a robust SQL table extraction function"""

import re

def extract_tables_robust(sql_text):
    """
    Robust SQL table extraction that handles various SSIS patterns.
    
    This function handles:
    - Schema.table format: [dbo].[Products], dbo.Products
    - Simple table names: Products, [Categories]
    - Table aliases: Products AS p, Categories c
    - Multiple JOIN types: INNER JOIN, LEFT JOIN, etc.
    - Complex whitespace and line breaks
    """
    if not sql_text or not isinstance(sql_text, str):
        return []
    
    # Clean up SQL - normalize whitespace
    sql_clean = re.sub(r'\s+', ' ', sql_text.strip())
    
    tables = []
    
    # Step 1: Find all FROM and JOIN clauses
    # This regex captures the table reference after FROM/JOIN keywords
    pattern = r'''
        (?:FROM|(?:\w+\s+)?JOIN)\s+          # FROM or any JOIN keyword (with optional qualifier like INNER, LEFT, etc.)
        (?:
            # Pattern 1: [schema].[table] or schema.table
            (?:\[([^\]]+)\]|([^\s\[\.]+))     # schema part (bracketed or not)
            \.                                # dot separator
            (?:\[([^\]]+)\]|([^\s\[\.]+))     # table part (bracketed or not)
            |
            # Pattern 2: Just [table] or table
            (?:\[([^\]]+)\]|([^\s\[\.]+))     # table only (bracketed or not)
        )
        (?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?       # optional alias (AS alias or just alias)
    '''
    
    matches = re.findall(pattern, sql_clean, re.IGNORECASE | re.VERBOSE)
    
    for match_groups in matches:
        # match_groups will have 6 groups:
        # 0: schema (bracketed), 1: schema (unbracketed)
        # 2: table (bracketed), 3: table (unbracketed)  
        # 4: simple table (bracketed), 5: simple table (unbracketed)
        
        schema_bracketed, schema_plain, table_bracketed, table_plain, simple_bracketed, simple_plain = match_groups
        
        # Determine schema and table
        schema = schema_bracketed or schema_plain
        table = table_bracketed or table_plain or simple_bracketed or simple_plain
        
        if schema and table:
            # Schema.table format
            table_name = f"{schema}.{table}"
        elif table:
            # Simple table name
            table_name = table
        else:
            continue
            
        # Clean table name and add if not already present
        table_name = table_name.strip()
        if table_name and table_name not in tables:
            tables.append(table_name)
    
    return tables

def test_robust_parser():
    """Test the robust parser with various SQL patterns"""
    
    test_cases = [
        {
            "name": "Product.dtsx - Original failing case",
            "sql": """SELECT p.ProductID, p.ProductName, c.CategoryName
FROM Products AS p INNER JOIN
     Categories AS c ON p.CategoryID = c.CategoryID""",
            "expected": ["Products", "Categories"]
        },
        
        {
            "name": "Schema qualified with brackets",
            "sql": "SELECT * FROM [dbo].[Customers] c JOIN [dbo].[Orders] o ON c.ID = o.CustomerID",
            "expected": ["dbo.Customers", "dbo.Orders"]
        },
        
        {
            "name": "Mixed bracket styles",
            "sql": "SELECT * FROM dbo.Products p JOIN [Categories] c ON p.CategoryID = c.ID",
            "expected": ["dbo.Products", "Categories"]
        },
        
        {
            "name": "Multiple JOINs different types",
            "sql": """SELECT * FROM Orders o 
                     LEFT JOIN Customers c ON o.CustomerID = c.ID
                     INNER JOIN Products p ON o.ProductID = p.ID
                     RIGHT JOIN Categories cat ON p.CategoryID = cat.ID""",
            "expected": ["Orders", "Customers", "Products", "Categories"]
        },
        
        {
            "name": "No aliases",
            "sql": "SELECT * FROM Products JOIN Categories ON Products.CategoryID = Categories.ID",
            "expected": ["Products", "Categories"]
        },
        
        {
            "name": "Complex whitespace and newlines",
            "sql": """SELECT    *    FROM
                           Products     AS     p
                      INNER    JOIN
                           Categories    AS    c    ON    p.CategoryID = c.ID""",
            "expected": ["Products", "Categories"]
        },
        
        {
            "name": "Single table with schema",
            "sql": "SELECT * FROM [dbo].[Products]",
            "expected": ["dbo.Products"]
        },
        
        {
            "name": "Single table simple",
            "sql": "SELECT * FROM Products",
            "expected": ["Products"]
        },
        
        {
            "name": "OpenRowset style (current working case)",
            "sql": "[dbo].[Products]",  # This is what's in OpenRowset property
            "expected": []  # This function is for SQL, not table names
        }
    ]
    
    print("🧪 Testing Robust SQL Table Extraction")
    print("=" * 80)
    
    all_passed = True
    
    for test_case in test_cases:
        print(f"\n📋 {test_case['name']}")
        print(f"SQL: {' '.join(test_case['sql'].split())}")
        print(f"Expected: {test_case['expected']}")
        
        extracted = extract_tables_robust(test_case['sql'])
        success = set(extracted) == set(test_case['expected'])
        
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"Result: {extracted}")
        print(f"Status: {status}")
        
        if not success:
            all_passed = False
            
    print(f"\n{'='*80}")
    print(f"Overall: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    
    return all_passed

if __name__ == "__main__":
    test_robust_parser()