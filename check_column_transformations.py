#!/usr/bin/env python3
"""
Check if column transformations from SELECT statements are captured in the graph.

This script specifically looks for the Product.dtsx SELECT statement:
SELECT p.ProductID, p.ProductName, p.SupplierID, p.CategoryID, p.QuantityPerUnit, p.UnitPrice, 
       p.UnitsInStock, p.UnitsOnOrder, p.ReorderLevel, p.Discontinued, 
       c.CategoryID AS Expr1, c.CategoryName, c.Description, c.Picture
FROM Products AS p INNER JOIN Categories AS c ON p.CategoryID = c.CategoryID
"""

import json
import sys
sys.path.append('/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode')

def analyze_column_transformations():
    """Check if column transformations are captured in the graph."""
    
    print("🔍 Analyzing Column Transformations in Graph")
    print("=" * 80)
    
    # Load the analysis file
    try:
        with open('enhanced_graph_full_analysis.json', 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("❌ Analysis file not found. Please run analysis first.")
        return
    
    # Look for Product-related operations
    product_operations = []
    for node in data.get('nodes', []):
        if (node.get('node_type') == 'operation' and 
            'Product' in node.get('name', '')):
            product_operations.append(node)
    
    print(f"📋 Found {len(product_operations)} Product-related operations:")
    
    for i, op in enumerate(product_operations, 1):
        print(f"\n{i}. Operation: {op.get('name')}")
        print(f"   Type: {op.get('properties', {}).get('operation_subtype', 'Unknown')}")
        
        # Check for column lineage information
        column_lineage = op.get('properties', {}).get('column_lineage', [])
        if column_lineage:
            print(f"   📊 Column lineage information found ({len(column_lineage)} components)")
            
            for component in column_lineage:
                component_name = component.get('component_name', 'Unknown')
                print(f"   \n   🔧 Component: {component_name}")
                
                # Check output columns for transformations
                output_columns = component.get('output_columns', [])
                if output_columns:
                    print(f"      📤 Output columns ({len(output_columns)}):")
                    for col in output_columns[:5]:  # Show first 5
                        col_name = col.get('column_name', 'Unknown')
                        data_type = col.get('canonical_type', 'Unknown')
                        expression = col.get('expression')
                        
                        print(f"        - {col_name} ({data_type})")
                        if expression:
                            print(f"          Expression: {expression}")
                    
                    if len(output_columns) > 5:
                        print(f"        ... and {len(output_columns) - 5} more columns")
                
                # Check input columns
                input_columns = component.get('input_columns', [])
                if input_columns:
                    print(f"      📥 Input columns ({len(input_columns)}):")
                    for col in input_columns[:3]:  # Show first 3
                        col_name = col.get('column_name', 'Unknown')
                        data_type = col.get('canonical_type', 'Unknown')
                        print(f"        - {col_name} ({data_type})")
                    
                    if len(input_columns) > 3:
                        print(f"        ... and {len(input_columns) - 3} more columns")
        else:
            print("   ❌ No column lineage information found")
    
    print(f"\n{'=' * 80}")
    print("🎯 KEY FINDINGS:")
    
    # Check for specific columns from the SELECT statement
    target_columns = [
        'ProductID', 'ProductName', 'SupplierID', 'CategoryID', 
        'QuantityPerUnit', 'UnitPrice', 'UnitsInStock', 'UnitsOnOrder',
        'ReorderLevel', 'Discontinued', 'CategoryName', 'Description', 'Picture'
    ]
    
    found_columns = set()
    transformation_details = []
    
    for op in product_operations:
        column_lineage = op.get('properties', {}).get('column_lineage', [])
        for component in column_lineage:
            # Check both input and output columns
            all_columns = (component.get('input_columns', []) + 
                          component.get('output_columns', []))
            
            for col in all_columns:
                col_name = col.get('column_name', '')
                if col_name in target_columns:
                    found_columns.add(col_name)
                    
                    # Record transformation details
                    transformation_details.append({
                        'column': col_name,
                        'component': component.get('component_name', 'Unknown'),
                        'data_type': col.get('canonical_type', 'Unknown'),
                        'ssis_type': col.get('ssis_native_type', 'Unknown'),
                        'expression': col.get('expression'),
                        'length': col.get('length'),
                        'target_types': col.get('target_types', {})
                    })
    
    print(f"\n📊 Column Coverage Analysis:")
    print(f"Target columns: {len(target_columns)}")
    print(f"Found columns: {len(found_columns)}")
    print(f"Coverage: {len(found_columns)/len(target_columns)*100:.1f}%")
    
    print(f"\n✅ Found columns: {sorted(found_columns)}")
    
    missing_columns = set(target_columns) - found_columns
    if missing_columns:
        print(f"❌ Missing columns: {sorted(missing_columns)}")
    
    # Check for column aliases and transformations
    print(f"\n🔧 Transformation Details:")
    alias_found = False
    for detail in transformation_details:
        if detail['expression'] or 'AS' in str(detail):
            print(f"   - {detail['column']}: {detail['expression'] or 'No expression'}")
            alias_found = True
    
    if not alias_found:
        print("   ❌ No column expressions/aliases detected")
        print("   ⚠️  This suggests the SELECT statement transformations may not be fully captured")
    
    # Check for JOIN information
    print(f"\n🔗 JOIN Information:")
    join_info_found = False
    
    # Look for lookup operations or JOIN metadata
    for op in product_operations:
        lookups = op.get('properties', {}).get('lookups', [])
        if lookups:
            print(f"   📋 Lookup operations found ({len(lookups)}):")
            for lookup in lookups:
                lookup_name = lookup.get('lookup_name', 'Unknown')
                sql_command = lookup.get('sql_command', '')
                print(f"      - {lookup_name}: {sql_command}")
            join_info_found = True
    
    if not join_info_found:
        print("   ❌ No explicit JOIN metadata found")
        print("   ⚠️  The INNER JOIN between Products and Categories may not be explicitly captured")
    
    return {
        'total_columns': len(target_columns),
        'found_columns': len(found_columns),
        'coverage_percent': len(found_columns)/len(target_columns)*100,
        'missing_columns': list(missing_columns),
        'transformation_details': transformation_details,
        'join_info_captured': join_info_found
    }

if __name__ == "__main__":
    results = analyze_column_transformations()
    
    print(f"\n{'=' * 80}")
    print("📋 SUMMARY FOR MIGRATION PLANNING:")
    
    if results['coverage_percent'] >= 80:
        print("✅ Good column coverage for migration planning")
    else:
        print("⚠️  Limited column coverage - may need enhancement for migration")
    
    if results['join_info_captured']:
        print("✅ JOIN relationships captured")
    else:
        print("❌ JOIN relationships not explicitly captured")
        print("   💡 Consider enhancing parser to capture JOIN semantics")
    
    if not results['transformation_details']:
        print("❌ Column transformations (aliases, expressions) not captured")
        print("   💡 This is critical for migration - the SELECT statement structure is missing")
    else:
        print("✅ Some column transformation information available")