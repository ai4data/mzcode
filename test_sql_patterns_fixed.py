#!/usr/bin/env python3
"""Test various SQL patterns - simplified version"""

import re

def test_sql_patterns():
    """Test key SQL patterns that appear in SSIS packages"""
    
    test_cases = [
        # Core failing case from Product.dtsx
        {
            "name": "Product.dtsx - Categories JOIN",
            "sql": """SELECT p.ProductID, p.ProductName, c.CategoryName
FROM Products AS p INNER JOIN
     Categories AS c ON p.CategoryID = c.CategoryID""",
            "expected": ["Products", "Categories"]
        },
        
        # Schema-qualified tables
        {
            "name": "Schema qualified",
            "sql": "SELECT * FROM [dbo].[Customers] c JOIN [dbo].[Orders] o ON c.ID = o.CustomerID",
            "expected": ["dbo.Customers", "dbo.Orders"]
        },
        
        # Mixed formats
        {
            "name": "Mixed formats",
            "sql": "SELECT * FROM dbo.Products p JOIN [Categories] c ON p.CategoryID = c.ID",
            "expected": ["dbo.Products", "Categories"]
        },
        
        # Multiple JOINs
        {
            "name": "Multiple JOINs",
            "sql": """SELECT * FROM Orders o 
                     LEFT JOIN Customers c ON o.CustomerID = c.ID
                     INNER JOIN Products p ON o.ProductID = p.ID""",
            "expected": ["Orders", "Customers", "Products"]
        },
        
        # No aliases
        {
            "name": "No aliases",
            "sql": "SELECT * FROM Products JOIN Categories ON Products.CategoryID = Categories.ID",
            "expected": ["Products", "Categories"]
        },
        
        # Complex whitespace
        {
            "name": "Complex whitespace",
            "sql": """SELECT    *    FROM
                           Products     AS     p
                      INNER    JOIN
                           Categories    AS    c    ON    p.CategoryID = c.ID""",
            "expected": ["Products", "Categories"]
        }
    ]
    
    print("🧪 Testing SQL Pattern Recognition")
    print("=" * 80)
    
    # Test different regex approaches
    for test_case in test_cases:
        print(f"\n📋 Test Case: {test_case['name']}")
        print(f"SQL: {' '.join(test_case['sql'].split())}")  # Clean whitespace for display
        print(f"Expected: {test_case['expected']}")
        print("-" * 50)
        
        # Current broken regex
        current_regex = r"(?:FROM|JOIN)\s+\[?(\w+)\]?\.\[?(\w+)\]?"
        extracted = extract_with_current_regex(test_case['sql'], current_regex)
        success = set(extracted) == set(test_case['expected'])
        print(f"  {'✅' if success else '❌'} Current (broken): {extracted}")
        
        # Improved regex approach
        improved_extracted = extract_tables_improved(test_case['sql'])
        success = set(improved_extracted) == set(test_case['expected'])
        print(f"  {'✅' if success else '❌'} Improved approach: {improved_extracted}")

def extract_with_current_regex(sql, pattern):
    """Extract tables using the current broken regex"""
    matches = re.findall(pattern, sql, re.IGNORECASE)
    tables = []
    for match in matches:
        if isinstance(match, tuple) and len(match) == 2:
            tables.append(f"{match[0]}.{match[1]}")
        elif isinstance(match, str):
            tables.append(match)
    return tables

def extract_tables_improved(sql):
    """Improved table extraction that handles multiple patterns"""
    tables = []
    
    # Pattern 1: Schema.Table format (with or without brackets)
    schema_table_pattern = r"(?:FROM|JOIN)\s+\[?([^\s\[\]\.]+)\]?\.\[?([^\s\[\]\.]+)\]?(?:\s+(?:AS\s+)?\w+)?"
    matches = re.findall(schema_table_pattern, sql, re.IGNORECASE | re.MULTILINE)
    for match in matches:
        if len(match) == 2 and match[0] and match[1]:
            tables.append(f"{match[0]}.{match[1]}")
    
    # Pattern 2: Simple table names (with or without brackets)
    simple_table_pattern = r"(?:FROM|JOIN)\s+\[?([^\s\[\]\.]+)\]?(?:\s+(?:AS\s+)?\w+)?"
    matches = re.findall(simple_table_pattern, sql, re.IGNORECASE | re.MULTILINE)
    for match in matches:
        # Only add if it's not already captured as schema.table
        if match and not any(table.endswith(f".{match}") for table in tables):
            tables.append(match)
    
    return tables

if __name__ == "__main__":
    test_sql_patterns()