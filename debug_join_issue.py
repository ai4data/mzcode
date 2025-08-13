#!/usr/bin/env python3
"""Debug the JOIN issue specifically"""

import re

def debug_join_pattern():
    failing_sql = "SELECT * FROM Products JOIN Categories ON Products.CategoryID = Categories.ID"
    
    print(f"🐛 Debugging failing SQL:")
    print(f"SQL: {failing_sql}")
    print()
    
    # Test different patterns
    patterns = [
        r'(?:FROM|(?:\w+\s+)?JOIN)\s+(?:\[([^\]]+)\]|([^\s\[\.]+))(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?',
        r'(?:FROM|JOIN)\s+(?:\[([^\]]+)\]|([^\s\[\.]+))(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?',
        r'(?:FROM|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+OUTER\s+JOIN|\w*\s*JOIN)\s+(?:\[([^\]]+)\]|([^\s\[\.]+))(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?',
    ]
    
    for i, pattern in enumerate(patterns, 1):
        print(f"Pattern {i}: {pattern}")
        matches = re.findall(pattern, failing_sql, re.IGNORECASE)
        print(f"Matches: {matches}")
        
        # Extract table names
        tables = []
        for match in matches:
            if isinstance(match, tuple):
                table = [part for part in match if part][0] if any(match) else None
            else:
                table = match
            if table:
                tables.append(table)
        print(f"Tables: {tables}")
        print()
    
    # Let's also test manually finding FROM and JOIN
    print("Manual approach:")
    keywords = ["FROM", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"]
    
    for keyword in keywords:
        pattern = rf'{keyword}\s+(\w+)'
        matches = re.findall(pattern, failing_sql, re.IGNORECASE)
        if matches:
            print(f"  {keyword}: {matches}")
    
    print()
    print("Let's look at all FROM/JOIN occurrences:")
    all_matches = re.finditer(r'(FROM|JOIN)\s+(\w+)', failing_sql, re.IGNORECASE)
    for match in all_matches:
        print(f"  Found: '{match.group(1)}' -> '{match.group(2)}'")

if __name__ == "__main__":
    debug_join_pattern()