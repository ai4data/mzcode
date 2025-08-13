#!/usr/bin/env python3
"""Analyze exactly why the parser fails to extract Categories table"""

import re

def analyze_parser_logic():
    # The exact SQL from Product.dtsx
    sql_query = """SELECT        p.ProductID, p.ProductName, p.SupplierID, p.CategoryID, p.QuantityPerUnit, p.UnitPrice, p.UnitsInStock, p.UnitsOnOrder, p.ReorderLevel, p.Discontinued, c.CategoryID AS Expr1, c.CategoryName, c.Description, c.Picture
FROM            Products AS p INNER JOIN
                         Categories AS c ON p.CategoryID = c.CategoryID"""
    
    print("🔍 Analyzing SSIS Parser Logic on Product.dtsx SQL")
    print("=" * 60)
    print(f"SQL Query:\n{sql_query}")
    print("=" * 60)
    
    # Step 1: Check if it's detected as SELECT statement
    print("\n1️⃣ Step 1: Is it detected as SELECT statement?")
    is_select = "SELECT" in sql_query.upper()
    print(f"   Result: {is_select}")
    
    if is_select:
        print("\n2️⃣ Step 2: Apply the parser's regex pattern")
        
        # This is the EXACT regex from the parser
        parser_regex = r"(?:FROM|JOIN)\s+\[?(\w+)\]?\.\[?(\w+)\]?"
        found_tables = re.findall(parser_regex, sql_query, re.IGNORECASE)
        print(f"   Parser Regex: {parser_regex}")
        print(f"   Result: {found_tables}")
        
        if found_tables:
            table_name = f"{found_tables[0][0]}.{found_tables[0][1]}"
            print(f"   Would extract table: {table_name}")
        else:
            print("   ❌ NO TABLES EXTRACTED!")
            
        print("\n3️⃣ Step 3: Why does the regex fail?")
        print("   Let's break down what the regex expects vs what we have:")
        
        # Show what the regex is looking for
        print("   Regex expects: FROM/JOIN [schema].[table]")
        print("   What we have in SQL:")
        
        # Find all FROM/JOIN clauses
        from_join_matches = re.findall(r"(FROM|JOIN)\s+([^\s]+(?:\s+AS\s+\w+)?)", sql_query, re.IGNORECASE)
        for i, (keyword, table_part) in enumerate(from_join_matches):
            print(f"     {i+1}. {keyword} {table_part}")
            
        print("\n4️⃣ Step 4: Testing improved regex patterns")
        
        # Test different patterns
        test_patterns = [
            (r"(?:FROM|JOIN)\s+\[?(\w+)\]?\.\[?(\w+)\]?", "Original parser pattern (schema.table)"),
            (r"(?:FROM|JOIN)\s+\[?([^\s\[\]]+)\]?(?:\s+AS\s+\w+)?", "Simple table name (with optional AS alias)"),
            (r"(?:FROM|JOIN)\s+(?:\[?([^\s\[\]]+)\]?\.\[?([^\s\[\]]+)\]?|\[?([^\s\[\]]+)\]?)(?:\s+AS\s+\w+)?", "Schema.table OR just table"),
            (r"(?:FROM|JOIN)\s+(?:\[?(\w+)\]?\.\[?(\w+)\]?|\[?(\w+)\]?)(?:\s+AS\s+\w+)?", "Simplified version")
        ]
        
        for pattern, description in test_patterns:
            matches = re.findall(pattern, sql_query, re.IGNORECASE)
            print(f"\n   Pattern: {description}")
            print(f"   Regex: {pattern}")
            print(f"   Matches: {matches}")
            
            # Extract table names from matches
            tables = []
            for match in matches:
                if isinstance(match, tuple):
                    # Handle multiple capture groups
                    if len(match) == 2 and match[0] and match[1]:  # schema.table
                        tables.append(f"{match[0]}.{match[1]}")
                    elif len(match) == 3:  # Three groups: schema, table, or single table
                        if match[0] and match[1]:  # schema.table
                            tables.append(f"{match[0]}.{match[1]}")
                        elif match[2]:  # just table
                            tables.append(match[2])
                    else:
                        # Find first non-empty group
                        for group in match:
                            if group:
                                tables.append(group)
                                break
                else:
                    tables.append(match)
            
            print(f"   Extracted tables: {tables}")
            
        print("\n5️⃣ Step 5: What should be extracted?")
        print("   Expected tables from this SQL:")
        print("   - Products (aliased as 'p')")
        print("   - Categories (aliased as 'c')")
        
        print("\n🎯 CONCLUSION:")
        print("   The parser's regex is too restrictive - it only matches schema.table format")
        print("   but the SQL uses simple table names: 'Products AS p' and 'Categories AS c'")
        print("   This is why Categories (and Products) are not being extracted!")

if __name__ == "__main__":
    analyze_parser_logic()