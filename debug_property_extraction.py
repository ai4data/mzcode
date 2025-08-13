#!/usr/bin/env python3
"""Debug what properties are being processed from Product.dtsx"""

from lxml import etree

def debug_product_properties():
    """Check what properties are found in Product.dtsx OLE DB components"""
    
    file_path = "/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode/data/ssis/ssis_northwind/SSIS/Product.dtsx"
    
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    if content.startswith("\ufeff"):
        content = content[1:]
        
    root = etree.fromstring(content.encode("utf-8"))
    
    # Find OLE DB Source components
    ole_db_sources = root.xpath(".//component[contains(@componentClassID, 'OLEDBSource')]")
    
    print("🔍 Debugging Product.dtsx OLE DB Source Properties")
    print("=" * 70)
    
    for component in ole_db_sources:
        component_name = component.get("name", "Unknown")
        print(f"\n📋 Component: {component_name}")
        
        properties_tag = component.find("properties")
        if properties_tag is not None:
            print("   Properties found:")
            
            # Check the specific properties the parser looks for
            prop_names_to_check = ["OpenRowset", "SqlCommand", "TableName"]
            
            for prop_name in prop_names_to_check:
                prop = properties_tag.find(f"property[@name='{prop_name}']")
                if prop is not None and prop.text:
                    print(f"   ✅ {prop_name}:")
                    if len(prop.text) > 200:
                        print(f"      {prop.text[:200]}...")
                    else:
                        print(f"      {prop.text}")
                    
                    # Check if it's a SELECT statement  
                    if "SELECT" in prop.text.upper():
                        print(f"      📊 This is a SQL SELECT statement")
                        
                        # Test extraction
                        if prop_name == "SqlCommand":
                            print(f"      🧪 This should trigger table extraction!")
                            
                else:
                    print(f"   ❌ {prop_name}: Not found or empty")
                    
            print(f"\n   🔍 Processing order (as per parser logic):")
            print(f"   1. OpenRowset: {'✅ Found' if properties_tag.find(f'property[@name=\"OpenRowset\"]') is not None and properties_tag.find(f'property[@name=\"OpenRowset\"]').text else '❌ Empty'}")
            print(f"   2. SqlCommand: {'✅ Found' if properties_tag.find(f'property[@name=\"SqlCommand\"]') is not None and properties_tag.find(f'property[@name=\"SqlCommand\"]').text else '❌ Empty'}")  
            print(f"   3. TableName: {'✅ Found' if properties_tag.find(f'property[@name=\"TableName\"]') is not None and properties_tag.find(f'property[@name=\"TableName\"]').text else '❌ Empty'}")
            
            # Check which one would be processed first
            openrowset_prop = properties_tag.find(f'property[@name="OpenRowset"]')
            sqlcommand_prop = properties_tag.find(f'property[@name="SqlCommand"]')
            
            if openrowset_prop is not None and openrowset_prop.text:
                if "SELECT" not in openrowset_prop.text.upper():
                    print(f"   ⚠️  OpenRowset is NOT a SELECT, so it will be used as table_name = '{openrowset_prop.text}'")
                    print(f"   ⚠️  This will cause the parser to SKIP SqlCommand processing!")
                    print(f"   ⚠️  This explains why Categories table is missing!")

if __name__ == "__main__":
    debug_product_properties()