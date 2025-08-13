#!/usr/bin/env python3
"""Production-ready SQL table extraction for SSIS parser"""

import re

def extract_tables_from_sql_command(sql_text):
    """
    Extract table names from SQL commands in SSIS components.
    
    This is the production version to replace the broken regex in ssis_parser.py
    
    Handles:
    - Simple tables: Products, Categories  
    - Bracketed tables: [Products], [Categories]
    - Schema-qualified: dbo.Products, [dbo].[Products]
    - Table aliases: Products AS p, Categories c
    - All JOIN types: INNER JOIN, LEFT JOIN, etc.
    """
    if not sql_text or not isinstance(sql_text, str):
        return []
    
    tables = set()
    
    # Pattern 1: Handle [schema].[table] format
    bracketed_schema_pattern = r'(?:FROM|JOIN)\s+\[([^\]]+)\]\.\[([^\]]+)\](?:\s+(?:AS\s+)?\w+)?'
    matches = re.findall(bracketed_schema_pattern, sql_text, re.IGNORECASE)
    for schema, table in matches:
        tables.add(f"{schema}.{table}")
    
    # Pattern 2: Handle schema.table format (no brackets)
    plain_schema_pattern = r'(?:FROM|JOIN)\s+([^\s\[\]\.]+)\.([^\s\[\]\.]+)(?:\s+(?:AS\s+)?\w+)?'
    matches = re.findall(plain_schema_pattern, sql_text, re.IGNORECASE)
    for schema, table in matches:
        if not any(existing == f"{schema}.{table}" for existing in tables):
            tables.add(f"{schema}.{table}")
    
    # Pattern 3: Handle simple table names (only if not already captured above)
    simple_pattern = r'(?:FROM|JOIN)\s+\[?([^\s\[\]\.]+)\]?(?:\s+(?:AS\s+)?\w+)?'
    matches = re.findall(simple_pattern, sql_text, re.IGNORECASE)
    for table in matches:
        # Only add if this table isn't already part of a schema.table entry
        if not any(existing.endswith(f".{table}") for existing in tables):
            tables.add(table)
    
    return sorted(list(tables))

def test_production_parser():
    """Test the production-ready parser"""
    
    test_cases = [
        {
            "name": "🎯 Product.dtsx - THE CRITICAL CASE",
            "sql": """SELECT p.ProductID, p.ProductName, c.CategoryName
FROM Products AS p INNER JOIN
     Categories AS c ON p.CategoryID = c.CategoryID""",
            "expected": ["Categories", "Products"]
        },
        
        {
            "name": "Exact Product.dtsx SQL from file",
            "sql": """SELECT        p.ProductID, p.ProductName, p.SupplierID, p.CategoryID, p.QuantityPerUnit, p.UnitPrice, p.UnitsInStock, p.UnitsOnOrder, p.ReorderLevel, p.Discontinued, c.CategoryID AS Expr1, c.CategoryName, c.Description, c.Picture
FROM            Products AS p INNER JOIN
                         Categories AS c ON p.CategoryID = c.CategoryID""",
            "expected": ["Categories", "Products"]
        },
        
        {
            "name": "Schema qualified with brackets",
            "sql": "SELECT * FROM [dbo].[Products] p JOIN [dbo].[Categories] c ON p.ID = c.ID",
            "expected": ["dbo.Categories", "dbo.Products"]
        },
        
        {
            "name": "Mixed bracket styles",
            "sql": "SELECT * FROM dbo.Products p JOIN [Categories] c ON p.CategoryID = c.ID",
            "expected": ["Categories", "dbo.Products"]
        },
        
        {
            "name": "The failing 'no aliases' case",
            "sql": "SELECT * FROM Products JOIN Categories ON Products.CategoryID = Categories.ID",
            "expected": ["Categories", "Products"]
        },
        
        {
            "name": "Multiple JOIN types",
            "sql": "SELECT * FROM Orders o LEFT JOIN Products p ON o.ProductID = p.ID INNER JOIN Categories c ON p.CategoryID = c.ID",
            "expected": ["Categories", "Orders", "Products"]
        },
        
        {
            "name": "Single table",
            "sql": "SELECT * FROM Products",
            "expected": ["Products"]
        },
        
        {
            "name": "Single table with schema",
            "sql": "SELECT * FROM [dbo].[Products]",
            "expected": ["dbo.Products"]
        }
    ]
    
    print("🧪 Testing Production-Ready SQL Table Extraction")
    print("=" * 80)
    
    all_passed = True
    critical_case_passed = False
    
    for i, test_case in enumerate(test_cases):
        print(f"\n📋 Test {i+1}: {test_case['name']}")
        
        result = extract_tables_from_sql_command(test_case['sql'])
        expected = test_case['expected']
        success = set(result) == set(expected)
        
        if "CRITICAL" in test_case['name'] and success:
            critical_case_passed = True
            
        print(f"Expected: {expected}")
        print(f"Got: {result}")
        print(f"Status: {'✅ PASS' if success else '❌ FAIL'}")
        
        if not success:
            all_passed = False
            print(f"   Missing: {set(expected) - set(result)}")
            print(f"   Extra: {set(result) - set(expected)}")
    
    print(f"\n{'='*80}")
    print(f"Critical case (Product.dtsx): {'✅ PASSED' if critical_case_passed else '❌ FAILED'}")
    print(f"Overall result: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    
    return all_passed, critical_case_passed

if __name__ == "__main__":
    test_production_parser()