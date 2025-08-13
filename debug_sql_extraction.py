#!/usr/bin/env python3
"""Debug script to show what the SSIS parser extracts from Product.dtsx"""

import re
from lxml import etree

def debug_product_dtsx():
    # Parse the Product.dtsx file directly
    file_path = "/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode/data/ssis/ssis_northwind/SSIS/Product.dtsx"
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Remove BOM if exists
        if content.startswith("\ufeff"):
            content = content[1:]
            
        root = etree.fromstring(content.encode("utf-8"))
        
        # Find OLE DB Source components
        ole_db_sources = root.xpath(".//component[contains(@componentClassID, 'OLEDBSource')]")
        
        print(f"Found {len(ole_db_sources)} OLE DB Source components")
        
        for i, component in enumerate(ole_db_sources):
            component_name = component.get("name", f"Component_{i}")
            print(f"\n🔍 Component: {component_name}")
            
            # Check properties
            properties_tag = component.find("properties")
            if properties_tag is not None:
                print("  📋 Properties found:")
                
                # Check specific properties
                prop_names_to_check = ["OpenRowset", "SqlCommand", "TableName"]
                for prop_name in prop_names_to_check:
                    prop = properties_tag.find(f"property[@name='{prop_name}']")
                    if prop is not None and prop.text:
                        print(f"    ✅ {prop_name}: {prop.text[:100]}...")
                        
                        # Test the regex that the parser uses
                        if "SELECT" in prop.text.upper():
                            print(f"      🧪 Testing regex on SQL...")
                            found_tables = re.findall(
                                r"(?:FROM|JOIN)\s+\[?(\w+)\]?\.\[?(\w+)\]?",
                                prop.text,
                                re.IGNORECASE,
                            )
                            print(f"      📊 Regex results: {found_tables}")
                            
                            # Test for Categories specifically
                            if "categories" in prop.text.lower():
                                print(f"      🎯 FOUND 'Categories' in SQL!")
                                
                                # Try different regex patterns
                                patterns = [
                                    r"(?:FROM|JOIN)\s+\[?(\w+)\]?\.\[?(\w+)\]?",  # Original
                                    r"(?:FROM|JOIN)\s+([^\s]+)",  # Simpler
                                    r"Categories\s+AS\s+(\w+)",  # Table alias
                                    r"(\w+)\s+AS\s+(\w+)"  # All aliases
                                ]
                                
                                for j, pattern in enumerate(patterns):
                                    matches = re.findall(pattern, prop.text, re.IGNORECASE)
                                    print(f"        Pattern {j+1}: {matches}")
                            
                        else:
                            print(f"      📝 Not a SELECT statement")
                    else:
                        print(f"    ❌ {prop_name}: Not found or empty")
            else:
                print("  ❌ No properties tag found")
                
        # Also check for any mention of Categories in the entire file
        print(f"\n🔍 Searching entire file for 'Categories'...")
        categories_count = content.lower().count("categories")
        print(f"Found 'Categories' {categories_count} times in the file")
        
        if categories_count > 0:
            # Find the context around each occurrence
            for match in re.finditer(r'categories', content, re.IGNORECASE):
                start = max(0, match.start() - 100)
                end = min(len(content), match.end() + 100)
                context = content[start:end]
                print(f"  Context: ...{context}...")
                print()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_product_dtsx()