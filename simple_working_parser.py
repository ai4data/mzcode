#!/usr/bin/env python3
"""Simple, working SQL table extraction based on the debug findings"""

import re

def extract_tables_simple(sql_text):
    """
    Simple but effective SQL table extraction.
    Based on the successful debug patterns.
    """
    if not sql_text or not isinstance(sql_text, str):
        return []
    
    tables = set()
    
    # Approach 1: Handle schema.table patterns first (like [dbo].[Products])  
    schema_pattern = r'(?:FROM|(?:\w+\s+)*JOIN)\s+\[?([^\]\s]+)\]?\.\[?([^\]\s]+)\]?(?:\s+(?:AS\s+)?\w+)?'
    schema_matches = re.findall(schema_pattern, sql_text, re.IGNORECASE)
    
    for schema, table in schema_matches:
        if schema and table:
            tables.add(f"{schema}.{table}")
    
    # Approach 2: Handle simple table names (like Products, Categories)
    # But exclude those already captured as schema.table
    simple_pattern = r'(?:FROM|(?:\w+\s+)*JOIN)\s+\[?([^\]\s\.]+)\]?(?:\s+(?:AS\s+)?\w+)?'
    simple_matches = re.findall(simple_pattern, sql_text, re.IGNORECASE)
    
    for table in simple_matches:
        if table:
            # Only add if not already part of a schema.table
            if not any(existing.endswith(f".{table}") for existing in tables):
                tables.add(table)
    
    return sorted(list(tables))

def test_simple_parser():
    """Test the simple parser"""
    
    test_cases = [
        {
            "name": "Product.dtsx - THE key failing case",
            "sql": """SELECT p.ProductID, p.ProductName, c.CategoryName
FROM Products AS p INNER JOIN
     Categories AS c ON p.CategoryID = c.CategoryID""",
            "expected": ["Categories", "Products"]
        },
        
        {
            "name": "Schema qualified",
            "sql": "SELECT * FROM [dbo].[Customers] c JOIN [dbo].[Orders] o ON c.ID = o.CustomerID",
            "expected": ["dbo.Customers", "dbo.Orders"]
        },
        
        {
            "name": "Mixed formats",
            "sql": "SELECT * FROM dbo.Products p JOIN [Categories] c ON p.CategoryID = c.ID",
            "expected": ["Categories", "dbo.Products"]
        },
        
        {
            "name": "No aliases (was failing)",
            "sql": "SELECT * FROM Products JOIN Categories ON Products.CategoryID = Categories.ID",
            "expected": ["Categories", "Products"]
        },
        
        {
            "name": "Single table",
            "sql": "SELECT * FROM Products",
            "expected": ["Products"]
        },
        
        {
            "name": "Original Product.dtsx exact SQL",
            "sql": """SELECT        p.ProductID, p.ProductName, p.SupplierID, p.CategoryID, p.QuantityPerUnit, p.UnitPrice, p.UnitsInStock, p.UnitsOnOrder, p.ReorderLevel, p.Discontinued, c.CategoryID AS Expr1, c.CategoryName, c.Description, c.Picture
FROM            Products AS p INNER JOIN
                         Categories AS c ON p.CategoryID = c.CategoryID""",
            "expected": ["Categories", "Products"]
        }
    ]
    
    print("🧪 Testing Simple SQL Table Extraction")
    print("=" * 80)
    
    all_passed = True
    
    for test_case in test_cases:
        print(f"\n📋 {test_case['name']}")
        extracted = extract_tables_simple(test_case['sql'])
        expected = test_case['expected']
        success = set(extracted) == set(expected)
        
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"Expected: {expected}")
        print(f"Got: {extracted}")
        print(f"Status: {status}")
        
        if not success:
            all_passed = False
            print(f"❌ Missing: {set(expected) - set(extracted)}")
            print(f"❌ Extra: {set(extracted) - set(expected)}")
            
    print(f"\n{'='*80}")
    print(f"Overall: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    
    return all_passed

if __name__ == "__main__":
    test_simple_parser()