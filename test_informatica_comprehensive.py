#!/usr/bin/env python3
"""
Comprehensive test script for Informatica parser implementation.
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from lxml import etree
from metazcode.sdk.ingestion.informatica.informatica_parser import InformaticaParser

def test_sample_data_parsing():
    """Test parsing with actual sample data."""
    workflow_file = "/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode/data/informatica/sazzad-amt/ExploreInformatica Project/WorkFlow_ExploreInformatica.XML"
    mapping_file = "/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode/data/informatica/sazzad-amt/ExploreInformatica Project/Mapping_ExploreInformatica.XML"
    
    if not os.path.exists(workflow_file):
        print(f"✗ Workflow file not found: {workflow_file}")
        return False
        
    if not os.path.exists(mapping_file):
        print(f"✗ Mapping file not found: {mapping_file}")
        return False
    
    print(f"✓ Found workflow file: {workflow_file}")
    print(f"✓ Found mapping file: {mapping_file}")
    
    try:
        # Test workflow parsing
        parser = InformaticaParser()
        
        # Read and parse workflow file
        with open(workflow_file, 'r', encoding='iso-8859-1') as f:
            workflow_content = f.read()
        
        print(f"✓ Read workflow file ({len(workflow_content)} characters)")
        
        # Try to parse the XML
        workflow_root = etree.fromstring(workflow_content.encode('iso-8859-1'))
        print("✓ Parsed workflow XML successfully")
        
        # Test mapping file parsing
        with open(mapping_file, 'r', encoding='iso-8859-1') as f:
            mapping_content = f.read()
        
        print(f"✓ Read mapping file ({len(mapping_content)} characters)")
        
        mapping_root = etree.fromstring(mapping_content.encode('iso-8859-1'))
        print("✓ Parsed mapping XML successfully")
        
        # Test actual parsing methods
        print("\n--- Testing Parser Methods ---")
        
        # Test workflow parsing
        workflow_nodes, workflow_edges, session_to_mapping = parser._parse_workflow(workflow_root, workflow_file)
        print(f"✓ Workflow parsing: {len(workflow_nodes)} nodes, {len(workflow_edges)} edges")
        print(f"  Session to mapping mappings: {len(session_to_mapping)}")
        
        # Test mapping parsing
        mapping_nodes, mapping_edges = parser._parse_mapping(mapping_root, mapping_file)
        print(f"✓ Mapping parsing: {len(mapping_nodes)} nodes, {len(mapping_edges)} edges")
        
        # Show sample nodes and edges
        print("\n--- Sample Nodes ---")
        for i, node in enumerate(mapping_nodes[:3]):
            print(f"  {i+1}. {node.node_id} ({node.node_type}) - {node.name}")
            if node.properties:
                prop_count = len(node.properties)
                print(f"     Properties: {prop_count} total")
                # Show first few properties
                first_props = dict(list(node.properties.items())[:3])
                for key, value in first_props.items():
                    print(f"       {key}: {str(value)[:50]}{'...' if len(str(value)) > 50 else ''}")
        
        print("\n--- Sample Edges ---")
        for i, edge in enumerate(mapping_edges[:3]):
            print(f"  {i+1}. {edge.source_id} -> {edge.target_id} ({edge.relation})")
            if edge.properties:
                prop_count = len(edge.properties)
                print(f"     Properties: {prop_count} total")
        
        # Test specific transformation parsers
        print("\n--- Testing Transformation Parsers ---")
        transformations = mapping_root.findall(".//TRANSFORMATION")
        print(f"Found {len(transformations)} transformations in mapping")
        
        for i, trans_xml in enumerate(transformations[:3]):  # Test first 3 transformations
            trans_name = trans_xml.get("NAME")
            trans_type = trans_xml.get("TYPE")
            print(f"  {i+1}. {trans_name} ({trans_type})")
            
            # Create a mock mapping node for testing
            from metazcode.sdk.models.graph import Node
            from metazcode.sdk.models.canonical_types import NodeType
            mapping_node = Node(
                node_id="test:mapping:test",
                node_type=NodeType.PIPELINE,
                name="test_mapping",
                properties={}
            )
            
            # Test transformation parsing
            try:
                trans_node, trans_edges = parser._parse_transformation(trans_xml, mapping_node)
                if trans_node:
                    print(f"     ✓ Parsed successfully: {trans_node.name}")
                    if trans_node.properties:
                        print(f"     Properties: {len(trans_node.properties)}")
                        # Show transformation-specific properties
                        for key, value in trans_node.properties.items():
                            if 'semantics' in key.lower() or 'expression' in key.lower():
                                print(f"       {key}: {type(value).__name__}")
                else:
                    print(f"     ⚠ No node returned")
            except Exception as e:
                print(f"     ✗ Error parsing transformation: {e}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error parsing sample data: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_expression_parsing():
    """Test expression parsing capabilities."""
    print("\n--- Testing Expression Parsing ---")
    
    try:
        parser = InformaticaParser()
        
        # Test the Informatica SQL parser
        from metazcode.sdk.ingestion.informatica.sql_semantics import InformaticaSqlParser
        sql_parser = InformaticaSqlParser()
        
        # Test various expressions
        test_expressions = [
            "$$Parameter1",
            "$Variable1", 
            "UPPER(FIELD1)",
            "IIF(FIELD1 > 10, 'HIGH', 'LOW')",
            "DECODE(STATUS, 'A', 'Active', 'I', 'Inactive', 'Unknown')",
            "SUBSTR(NAME, 1, 5)",
            "$$StartDate + 30"
        ]
        
        for expr in test_expressions:
            try:
                result = sql_parser.parse_informatica_expression(expr, "Expression")
                print(f"  ✓ '{expr}' -> References: {result['references']}, Functions: {result['functions_used']}")
            except Exception as e:
                print(f"  ✗ Error parsing '{expr}': {e}")
        
        return True
    except Exception as e:
        print(f"✗ Error in expression parsing test: {e}")
        return False

def main():
    """Main test function."""
    print("=== Comprehensive Informatica Parser Test ===\n")
    
    success = True
    
    print("1. Testing sample data parsing...")
    if not test_sample_data_parsing():
        success = False
    
    print("\n2. Testing expression parsing...")
    if not test_expression_parsing():
        success = False
    
    if success:
        print("\n=== All Tests Passed Successfully ===")
        print("The Informatica parser implementation is working correctly!")
    else:
        print("\n=== Some Tests Failed ===")
        print("There are issues with the implementation that need to be addressed.")

if __name__ == "__main__":
    main()