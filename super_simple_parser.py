#!/usr/bin/env python3
"""Super simple approach - exactly what worked in debug"""

import re

def extract_tables_super_simple(sql_text):
    """
    The simplest approach that actually works.
    Based exactly on what worked in the debug script.
    """
    if not sql_text or not isinstance(sql_text, str):
        return []
    
    tables = set()
    
    # Step 1: Find all FROM occurrences
    from_matches = re.findall(r'FROM\s+\[?([^\s\[\]]+)\]?', sql_text, re.IGNORECASE)
    for match in from_matches:
        # Handle schema.table format
        if '.' in match:
            parts = match.split('.')
            if len(parts) == 2:
                tables.add(f"{parts[0]}.{parts[1]}")
            else:
                tables.add(match)
        else:
            tables.add(match)
    
    # Step 2: Find all JOIN occurrences  
    join_matches = re.findall(r'JOIN\s+\[?([^\s\[\]]+)\]?', sql_text, re.IGNORECASE)
    for match in join_matches:
        # Handle schema.table format
        if '.' in match:
            parts = match.split('.')
            if len(parts) == 2:
                tables.add(f"{parts[0]}.{parts[1]}")
            else:
                tables.add(match)
        else:
            tables.add(match)
    
    return sorted(list(tables))

def test_super_simple():
    """Test the super simple approach"""
    
    # The failing case that MUST work
    failing_sql = "SELECT * FROM Products JOIN Categories ON Products.CategoryID = Categories.ID"
    
    print("🔍 Testing the problematic case:")
    print(f"SQL: {failing_sql}")
    
    # Manual regex test (what worked in debug)
    print("\nManual regex tests:")
    from_matches = re.findall(r'FROM\s+(\w+)', failing_sql, re.IGNORECASE)
    join_matches = re.findall(r'JOIN\s+(\w+)', failing_sql, re.IGNORECASE)
    
    print(f"FROM matches: {from_matches}")
    print(f"JOIN matches: {join_matches}")
    print(f"Combined: {from_matches + join_matches}")
    
    # Now test with the function
    print(f"\nFunction result: {extract_tables_super_simple(failing_sql)}")
    
    print("\n" + "="*60)
    
    # Test all cases
    test_cases = [
        {
            "name": "The failing case",
            "sql": "SELECT * FROM Products JOIN Categories ON Products.CategoryID = Categories.ID",
            "expected": ["Categories", "Products"]
        },
        {
            "name": "Product.dtsx case",
            "sql": """SELECT p.ProductID, p.ProductName FROM Products AS p INNER JOIN Categories AS c ON p.CategoryID = c.CategoryID""",
            "expected": ["Categories", "Products"]
        },
        {
            "name": "Schema qualified",
            "sql": "SELECT * FROM [dbo].[Products] p JOIN [dbo].[Categories] c ON p.ID = c.ID",
            "expected": ["dbo.Categories", "dbo.Products"]
        },
        {
            "name": "Single table",
            "sql": "SELECT * FROM Products",
            "expected": ["Products"]
        }
    ]
    
    all_passed = True
    for test_case in test_cases:
        print(f"\n📋 {test_case['name']}")
        result = extract_tables_super_simple(test_case['sql'])
        expected = test_case['expected']
        success = set(result) == set(expected)
        
        print(f"Expected: {expected}")
        print(f"Got: {result}")
        print(f"Status: {'✅ PASS' if success else '❌ FAIL'}")
        
        if not success:
            all_passed = False

    print(f"\n{'='*60}")
    print(f"Overall: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")

if __name__ == "__main__":
    test_super_simple()