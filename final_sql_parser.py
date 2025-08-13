#!/usr/bin/env python3
"""Final robust SQL table extraction function"""

import re

def extract_tables_from_sql(sql_text):
    """
    Extract table names from SQL queries in SSIS components.
    
    Handles various formats:
    - Simple tables: Products, Categories
    - Bracketed tables: [Products], [Categories]  
    - Schema-qualified: dbo.Products, [dbo].[Products]
    - With aliases: Products AS p, Categories c
    - Multiple JOIN types: INNER JOIN, LEFT JOIN, etc.
    """
    if not sql_text or not isinstance(sql_text, str):
        return []
    
    # Normalize whitespace
    sql_clean = re.sub(r'\s+', ' ', sql_text.strip())
    
    tables = set()  # Use set to avoid duplicates
    
    # Step 1: Find schema.table patterns first (higher priority)
    schema_table_pattern = r'''
        (?:FROM|(?:\w+\s+)*JOIN)\s+          # FROM or any JOIN variant
        (?:\[([^\]]+)\]|([^\s\[\]\.]+))      # schema (bracketed or plain)
        \.                                   # dot separator
        (?:\[([^\]]+)\]|([^\s\[\]\.]+))      # table (bracketed or plain)
        (?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?       # optional alias
    '''
    
    schema_matches = re.findall(schema_table_pattern, sql_clean, re.IGNORECASE | re.VERBOSE)
    for match in schema_matches:
        schema_bracket, schema_plain, table_bracket, table_plain = match
        schema = schema_bracket or schema_plain
        table = table_bracket or table_plain
        if schema and table:
            tables.add(f"{schema}.{table}")
    
    # Step 2: Find simple table patterns (only if not already covered by schema.table)
    simple_table_pattern = r'''
        (?:FROM|(?:\w+\s+)*JOIN)\s+          # FROM or any JOIN variant
        (?:\[([^\]]+)\]|([^\s\[\]\.]+))      # table name (bracketed or plain)
        (?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?       # optional alias
    '''
    
    simple_matches = re.findall(simple_table_pattern, sql_clean, re.IGNORECASE | re.VERBOSE)
    for match in simple_matches:
        table_bracket, table_plain = match if isinstance(match, tuple) else (match, '')
        table = table_bracket or table_plain
        if table:
            # Only add if this table isn't already part of a schema.table entry
            if not any(existing.endswith(f".{table}") for existing in tables):
                tables.add(table)
    
    return sorted(list(tables))  # Return sorted list for consistent output

def test_final_parser():
    """Test the final parser implementation"""
    
    test_cases = [
        {
            "name": "Product.dtsx - Original failing case",
            "sql": """SELECT p.ProductID, p.ProductName, c.CategoryName
FROM Products AS p INNER JOIN
     Categories AS c ON p.CategoryID = c.CategoryID""",
            "expected": ["Categories", "Products"]
        },
        
        {
            "name": "Schema qualified with brackets",
            "sql": "SELECT * FROM [dbo].[Customers] c JOIN [dbo].[Orders] o ON c.ID = o.CustomerID",
            "expected": ["dbo.Customers", "dbo.Orders"]
        },
        
        {
            "name": "Mixed bracket styles",
            "sql": "SELECT * FROM dbo.Products p JOIN [Categories] c ON p.CategoryID = c.ID",
            "expected": ["Categories", "dbo.Products"]
        },
        
        {
            "name": "Multiple JOINs different types",
            "sql": """SELECT * FROM Orders o 
                     LEFT JOIN Customers c ON o.CustomerID = c.ID
                     INNER JOIN Products p ON o.ProductID = p.ID
                     RIGHT JOIN Categories cat ON p.CategoryID = cat.ID""",
            "expected": ["Categories", "Customers", "Orders", "Products"]
        },
        
        {
            "name": "No aliases - the failing one",
            "sql": "SELECT * FROM Products JOIN Categories ON Products.CategoryID = Categories.ID",
            "expected": ["Categories", "Products"]
        },
        
        {
            "name": "Complex whitespace and newlines",
            "sql": """SELECT    *    FROM
                           Products     AS     p
                      INNER    JOIN
                           Categories    AS    c    ON    p.CategoryID = c.ID""",
            "expected": ["Categories", "Products"]
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
            "name": "FULL OUTER JOIN",
            "sql": "SELECT * FROM Orders o FULL OUTER JOIN Products p ON o.ProductID = p.ID",
            "expected": ["Orders", "Products"]
        }
    ]
    
    print("🧪 Testing Final SQL Table Extraction")
    print("=" * 80)
    
    all_passed = True
    
    for test_case in test_cases:
        print(f"\n📋 {test_case['name']}")
        print(f"SQL: {' '.join(test_case['sql'].split())}")
        print(f"Expected: {test_case['expected']}")
        
        extracted = extract_tables_from_sql(test_case['sql'])
        success = extracted == test_case['expected']  # Order matters for this test
        
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"Result: {extracted}")
        print(f"Status: {status}")
        
        if not success:
            all_passed = False
            
    print(f"\n{'='*80}")
    print(f"Overall: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    
    return all_passed

# Test specifically with the original Product.dtsx SQL
def test_original_product_sql():
    """Test with the exact SQL from Product.dtsx"""
    original_sql = """SELECT        p.ProductID, p.ProductName, p.SupplierID, p.CategoryID, p.QuantityPerUnit, p.UnitPrice, p.UnitsInStock, p.UnitsOnOrder, p.ReorderLevel, p.Discontinued, c.CategoryID AS Expr1, c.CategoryName, c.Description, c.Picture
FROM            Products AS p INNER JOIN
                         Categories AS c ON p.CategoryID = c.CategoryID"""
    
    print(f"\n🎯 Testing Original Product.dtsx SQL")
    print("=" * 60)
    print(f"SQL: {' '.join(original_sql.split())}")
    
    extracted = extract_tables_from_sql(original_sql)
    print(f"Extracted tables: {extracted}")
    
    expected = ["Categories", "Products"]
    success = set(extracted) == set(expected)
    print(f"Expected: {expected}")
    print(f"Success: {'✅ YES' if success else '❌ NO'}")
    
    return success

if __name__ == "__main__":
    test_final_parser()
    test_original_product_sql()