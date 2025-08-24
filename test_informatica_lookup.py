#!/usr/bin/env python3
"""
Test script to verify Informatica lookup transformation parsing.
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from lxml import etree
from metazcode.sdk.ingestion.informatica.informatica_parser import InformaticaParser

def test_lookup_transformation_parsing():
    """Test lookup transformation parsing with the ExploreInformatica example."""
    
    mapping_file = "data/informatica/sazzad-amt/ExploreInformatica Project/Mapping_ExploreInformatica.XML"
    
    if not os.path.exists(mapping_file):
        print(f"✗ Mapping file not found: {mapping_file}")
        return False
    
    print(f"✓ Found mapping file: {mapping_file}")
    
    try:
        # Create parser
        parser = InformaticaParser()
        
        # Read and parse mapping file
        with open(mapping_file, 'r', encoding='iso-8859-1') as f:
            mapping_content = f.read()
        
        print(f"✓ Read mapping file ({len(mapping_content)} characters)")
        
        # Parse the XML
        mapping_root = etree.fromstring(mapping_content.encode('iso-8859-1'))
        print("✓ Parsed mapping XML successfully")
        
        # Test mapping parsing
        mapping_nodes, mapping_edges = parser._parse_mapping(mapping_root, mapping_file)
        print(f"✓ Mapping parsing: {len(mapping_nodes)} nodes, {len(mapping_edges)} edges")
        
        # Find lookup transformation nodes
        lookup_nodes = [node for node in mapping_nodes 
                       if node.node_type.value == 'OPERATION' and 
                       'lookup' in node.name.lower()]
        
        print(f"\n--- Found {len(lookup_nodes)} Lookup Transformation(s) ---")
        
        for i, lookup_node in enumerate(lookup_nodes):
            print(f"\nLookup {i+1}: {lookup_node.name}")
            print(f"  Node ID: {lookup_node.node_id}")
            print(f"  Node Type: {lookup_node.node_type}")
            
            # Check if lookup semantics were parsed
            if 'lookup_semantics' in lookup_node.properties:
                semantics = lookup_node.properties['lookup_semantics']
                print(f"  ✓ Lookup semantics parsed successfully")
                
                # Display key lookup properties
                if 'lookup_table' in semantics:
                    print(f"    Lookup Table: {semantics['lookup_table']}")
                
                if 'lookup_condition' in semantics:
                    print(f"    Lookup Condition: {semantics['lookup_condition']}")
                
                if 'caching_enabled' in semantics:
                    print(f"    Caching Enabled: {semantics['caching_enabled']}")
                
                if 'multiple_match_policy' in semantics:
                    print(f"    Multiple Match Policy: {semantics['multiple_match_policy']}")
                
                if 'input_field_count' in semantics:
                    print(f"    Input Fields: {semantics['input_field_count']}")
                
                if 'return_field_count' in semantics:
                    print(f"    Return Fields: {semantics['return_field_count']}")
                
                if 'sql_equivalent' in semantics:
                    print(f"    SQL Equivalent: {semantics['sql_equivalent']}")
                
                if 'complexity_assessment' in semantics:
                    complexity = semantics['complexity_assessment']
                    print(f"    Complexity Level: {complexity.get('complexity_level', 'UNKNOWN')}")
                    print(f"    Migration Risk: {complexity.get('migration_risk', 'UNKNOWN')}")
                
                if 'migration_considerations' in semantics:
                    migration = semantics['migration_considerations']
                    print(f"    Cache Strategy: {migration.get('cache_strategy', 'UNKNOWN')}")
                    print(f"    Join Equivalent: {migration.get('join_equivalent', 'UNKNOWN')}")
            else:
                print(f"  ✗ No lookup semantics found")
            
            # Show general properties
            print(f"  Properties count: {len(lookup_node.properties)}")
            
            # Show operation subtype
            if 'operation_subtype' in lookup_node.properties:
                print(f"  Operation Subtype: {lookup_node.properties['operation_subtype']}")
        
        # Test specific lookup transformations we know exist
        expected_lookups = ['lkp_REGION_NAME', 'lkp_DEPARTMENT_NAME']
        found_lookups = [node.name for node in lookup_nodes]
        
        print(f"\n--- Verification ---")
        for expected in expected_lookups:
            if expected in found_lookups:
                print(f"✓ Found expected lookup: {expected}")
            else:
                print(f"✗ Missing expected lookup: {expected}")
        
        # Test transformation type detection
        print(f"\n--- Transformation Type Detection ---")
        for node in mapping_nodes:
            if node.node_type.value == 'OPERATION':
                if 'operation_subtype' in node.properties:
                    subtype = node.properties['operation_subtype']
                    if 'Lookup' in subtype:
                        print(f"✓ Detected lookup transformation: {node.name} ({subtype})")
        
        return len(lookup_nodes) > 0
        
    except Exception as e:
        print(f"✗ Error testing lookup transformation parsing: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_lookup_sql_semantics():
    """Test the SQL semantics analysis for lookup transformations."""
    print("\n=== Testing Lookup SQL Semantics ===")
    
    try:
        from metazcode.sdk.ingestion.informatica.sql_semantics import InformaticaSqlParser
        
        parser = InformaticaSqlParser()
        
        # Test lookup condition analysis
        test_conditions = [
            "REGION_ID = in_REGION_ID1",
            "DEPARTMENT_ID = in_DEPARTMENT_ID",
            "EMPLOYEE_ID = in_EMP_ID AND STATUS = 'ACTIVE'",
            "UPPER(LAST_NAME) = UPPER(in_LAST_NAME)"
        ]
        
        for condition in test_conditions:
            print(f"\nTesting condition: {condition}")
            
            # Test lookup semantics analysis
            lookup_semantics = parser.analyze_lookup_semantics(
                lookup_condition=condition,
                lookup_table="TEST_TABLE",
                lookup_fields=[{"name": "input_field", "datatype": "string"}],
                return_fields=[{"name": "return_field", "datatype": "string"}]
            )
            
            print(f"  SQL Equivalent: {lookup_semantics.get('sql_equivalent', 'N/A')}")
            print(f"  Migration Notes: {lookup_semantics.get('migration_notes', [])}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error testing lookup SQL semantics: {e}")
        return False

def main():
    """Main test function."""
    print("=== Informatica Lookup Transformation Test ===\n")
    
    success = True
    
    print("1. Testing lookup transformation parsing...")
    if not test_lookup_transformation_parsing():
        success = False
    
    print("\n2. Testing lookup SQL semantics...")
    if not test_lookup_sql_semantics():
        success = False
    
    if success:
        print("\n=== All Lookup Tests Passed Successfully ===")
        print("✓ Lookup transformation parsing is working correctly!")
        print("✓ SQL semantics analysis is functional!")
        print("✓ Migration assessment is available!")
    else:
        print("\n=== Some Lookup Tests Failed ===")
        print("There are issues with the lookup implementation that need to be addressed.")

if __name__ == "__main__":
    main()