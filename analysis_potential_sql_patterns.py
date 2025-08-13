#!/usr/bin/env python3
"""
Analysis of potential SQL parsing patterns that might be missed in SSIS parser.

This analysis identifies edge cases that could cause similar issues to the 
Categories table extraction problem we just fixed.
"""

import re
from typing import List, Dict, Any

def analyze_potential_edge_cases():
    """
    Comprehensive analysis of SQL patterns that could be missed by the parser.
    
    Based on the Categories table issue, we need to identify other patterns
    that might cause tables to be missing from the graph.
    """
    
    edge_cases = [
        {
            "category": "Common Table Expressions (CTEs)",
            "description": "WITH clauses that define temporary result sets",
            "risk_level": "HIGH", 
            "example_sql": """
                WITH RegionalSales AS (
                    SELECT CategoryID, SUM(UnitPrice) as TotalSales
                    FROM Products p
                    JOIN OrderDetails od ON p.ProductID = od.ProductID
                )
                SELECT rs.CategoryID, c.CategoryName, rs.TotalSales
                FROM RegionalSales rs
                JOIN Categories c ON rs.CategoryID = c.CategoryID
            """,
            "tables_referenced": ["Products", "OrderDetails", "Categories"],
            "potential_issue": "CTE definitions might not be parsed, missing tables in WITH clause"
        },
        
        {
            "category": "Subqueries in FROM clause", 
            "description": "Derived tables using subqueries",
            "risk_level": "HIGH",
            "example_sql": """
                SELECT main.ProductName, sub.AvgPrice
                FROM Products main
                JOIN (
                    SELECT CategoryID, AVG(UnitPrice) as AvgPrice
                    FROM Products 
                    WHERE Discontinued = 0
                ) sub ON main.CategoryID = sub.CategoryID
            """,
            "tables_referenced": ["Products"],
            "potential_issue": "Tables inside subqueries might be missed or double-counted"
        },
        
        {
            "category": "UNION statements",
            "description": "Multiple SELECT statements combined with UNION",
            "risk_level": "MEDIUM", 
            "example_sql": """
                SELECT ProductID, ProductName FROM Products WHERE CategoryID = 1
                UNION ALL
                SELECT ProductID, ProductName FROM ArchivedProducts WHERE CategoryID = 1
            """,
            "tables_referenced": ["Products", "ArchivedProducts"],
            "potential_issue": "Second SELECT in UNION might be missed"
        },
        
        {
            "category": "Temporary tables",
            "description": "Local and global temporary tables", 
            "risk_level": "MEDIUM",
            "example_sql": """
                SELECT p.ProductName, c.CategoryName
                FROM #TempProducts p
                JOIN Categories c ON p.CategoryID = c.CategoryID
            """,
            "tables_referenced": ["#TempProducts", "Categories"],
            "potential_issue": "# and ## prefixed tables might not be handled correctly"
        },
        
        {
            "category": "Table variables",
            "description": "Variables that hold table data",
            "risk_level": "LOW",
            "example_sql": """
                SELECT p.ProductName, tv.Quantity
                FROM Products p
                JOIN @TableVariable tv ON p.ProductID = tv.ProductID
            """, 
            "tables_referenced": ["Products", "@TableVariable"],
            "potential_issue": "@ prefixed table variables might be missed"
        },
        
        {
            "category": "Multiple schemas",
            "description": "Cross-database or cross-schema references",
            "risk_level": "HIGH",
            "example_sql": """
                SELECT p.ProductName, c.CategoryName, s.SupplierName
                FROM Northwind.dbo.Products p
                JOIN Northwind.dbo.Categories c ON p.CategoryID = c.CategoryID
                JOIN OtherDB.sales.Suppliers s ON p.SupplierID = s.SupplierID
            """,
            "tables_referenced": ["Northwind.dbo.Products", "Northwind.dbo.Categories", "OtherDB.sales.Suppliers"],
            "potential_issue": "Complex schema references might be parsed incorrectly"
        },
        
        {
            "category": "Table-valued functions",
            "description": "Functions that return table results",
            "risk_level": "MEDIUM",
            "example_sql": """
                SELECT p.ProductName, f.CalculatedValue
                FROM Products p
                CROSS APPLY dbo.GetProductMetrics(p.ProductID) f
            """,
            "tables_referenced": ["Products", "dbo.GetProductMetrics"],
            "potential_issue": "Functions in FROM/JOIN clauses might not be identified as data sources"
        },
        
        {
            "category": "Complex JOIN syntax",
            "description": "Non-standard JOIN patterns",
            "risk_level": "MEDIUM",
            "example_sql": """
                SELECT p.ProductName, c.CategoryName
                FROM Products p, Categories c
                WHERE p.CategoryID = c.CategoryID
            """,
            "tables_referenced": ["Products", "Categories"],
            "potential_issue": "Old-style comma joins might be missed (no explicit JOIN keyword)"
        },
        
        {
            "category": "Nested brackets",
            "description": "Multiple levels of bracketing",
            "risk_level": "LOW",
            "example_sql": """
                SELECT p.ProductName
                FROM [[Unusual].[Table Name]] p
                JOIN [Another].[Table] a ON p.ID = a.ID
            """,
            "tables_referenced": ["[Unusual].[Table Name]", "Another.Table"],
            "potential_issue": "Nested or unusual bracket patterns might confuse regex"
        },
        
        {
            "category": "Dynamic SQL patterns",
            "description": "SQL built through string concatenation",
            "risk_level": "HIGH",
            "example_sql": """
                DECLARE @TableName NVARCHAR(100) = 'Products'  
                DECLARE @SQL NVARCHAR(MAX) = 'SELECT * FROM ' + @TableName + ' WHERE CategoryID = 1'
                EXEC sp_executesql @SQL
            """,
            "tables_referenced": ["Products"],
            "potential_issue": "Table names in variables or concatenated strings won't be detected"
        }
    ]
    
    return edge_cases

def test_current_parser_against_edge_cases():
    """
    Test our current parser implementation against identified edge cases.
    """
    # Import our current parser
    import sys
    sys.path.append('/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode')
    
    try:
        from metazcode.sdk.ingestion.ssis.ssis_parser import CanonicalSsisParser
        parser = CanonicalSsisParser()
        
        edge_cases = analyze_potential_edge_cases()
        
        print("🔍 Testing Current Parser Against Edge Cases")
        print("=" * 80)
        
        high_risk_failures = []
        medium_risk_failures = []
        
        for i, case in enumerate(edge_cases):
            print(f"\n📋 Test {i+1}: {case['category']} ({case['risk_level']} RISK)")
            print(f"Description: {case['description']}")
            
            # Clean up the SQL for testing
            sql = ' '.join(case['example_sql'].split())
            
            try:
                extracted = parser._extract_tables_from_sql(sql)
                expected = case['tables_referenced']
                
                # For this analysis, we'll be more lenient about exact matches
                # and focus on whether major tables are detected
                missing_major_tables = []
                for expected_table in expected:
                    # Check if any extracted table contains the expected table name
                    found = any(expected_table.split('.')[-1].replace('[', '').replace(']', '').replace('#', '').replace('@', '') 
                              in extracted_table.replace('[', '').replace(']', '') 
                              for extracted_table in extracted)
                    if not found and not expected_table.startswith('@') and not expected_table.startswith('#'):
                        # Only consider it missing if it's a real table (not temp/variable)
                        if '(' not in expected_table:  # Exclude functions
                            missing_major_tables.append(expected_table)
                
                success = len(missing_major_tables) == 0
                
                print(f"Expected tables: {expected}")
                print(f"Extracted tables: {extracted}")
                print(f"Status: {'✅ PASS' if success else '❌ FAIL'}")
                
                if not success:
                    print(f"⚠️  Missing major tables: {missing_major_tables}")
                    if case['risk_level'] == 'HIGH':
                        high_risk_failures.append(case['category'])
                    elif case['risk_level'] == 'MEDIUM':
                        medium_risk_failures.append(case['category'])
                        
            except Exception as e:
                print(f"❌ ERROR: Parser failed on this case: {e}")
                if case['risk_level'] == 'HIGH':
                    high_risk_failures.append(case['category'])
        
        print(f"\n{'='*80}")
        print("🎯 ANALYSIS SUMMARY")
        print(f"{'='*80}")
        
        if high_risk_failures:
            print(f"🚨 HIGH RISK FAILURES ({len(high_risk_failures)}):")
            for failure in high_risk_failures:
                print(f"   - {failure}")
        
        if medium_risk_failures:
            print(f"⚠️  MEDIUM RISK FAILURES ({len(medium_risk_failures)}):")
            for failure in medium_risk_failures:
                print(f"   - {failure}")
                
        if not high_risk_failures and not medium_risk_failures:
            print("✅ All edge cases handled successfully!")
        else:
            print(f"\n📋 RECOMMENDATIONS:")
            print("1. Focus on HIGH RISK failures first")
            print("2. Consider expanding regex patterns for missed cases")
            print("3. Add specific handlers for complex SQL constructs")
            print("4. Test against real SSIS packages that use these patterns")
                
    except ImportError as e:
        print(f"❌ Could not import parser: {e}")

if __name__ == "__main__":
    test_current_parser_against_edge_cases()