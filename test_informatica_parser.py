#!/usr/bin/env python3
"""
Basic test for the Informatica parser implementation.

This script tests the parser with sample Informatica files to verify basic functionality.
"""

import os
import sys
import logging

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from metazcode.sdk.ingestion.informatica.informatica_parser import CanonicalInformaticaParser
from metazcode.sdk.ingestion.informatica.informatica_tool import InformaticaIngestionTool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_parser_basic():
    """Test basic parser functionality."""
    print("Testing Informatica Parser...")
    
    # Test paths
    test_workflow = "/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode/data/informatica/hassan-hosny/Q1/wf_m_q1.XML"
    test_mapping = "/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode/data/informatica/hassan-hosny/Q1/m_q1.XML"
    
    if not os.path.exists(test_workflow):
        print(f"Test workflow file not found: {test_workflow}")
        return False
    
    if not os.path.exists(test_mapping):
        print(f"Test mapping file not found: {test_mapping}")
        return False
    
    try:
        # Initialize parser
        parser = CanonicalInformaticaParser()
        
        # Parse files
        print(f"Parsing workflow: {test_workflow}")
        print(f"Parsing mapping: {test_mapping}")
        
        nodes = []
        edges = []
        
        for node_batch, edge_batch in parser.parse(test_workflow, test_mapping):
            nodes.extend(node_batch)
            edges.extend(edge_batch)
        
        print(f"Successfully parsed {len(nodes)} nodes and {len(edges)} edges")
        
        # Print some sample results
        print("\nSample Nodes:")
        for i, node in enumerate(nodes[:5]):  # First 5 nodes
            print(f"  {i+1}. {node.node_id} ({node.node_type}): {node.name}")
        
        print(f"\nSample Edges:")
        for i, edge in enumerate(edges[:5]):  # First 5 edges
            print(f"  {i+1}. {edge.source_id} -> {edge.target_id} ({edge.relation})")
        
        return True
        
    except Exception as e:
        print(f"Parser test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_ingestion_tool():
    """Test the ingestion tool functionality."""
    print("\nTesting Informatica Ingestion Tool...")
    
    test_directory = "/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode/data/informatica/hassan-hosny/Q1"
    
    if not os.path.exists(test_directory):
        print(f"Test directory not found: {test_directory}")
        return False
    
    try:
        # Initialize ingestion tool
        tool = InformaticaIngestionTool(test_directory)
        
        nodes = []
        edges = []
        
        # Process files
        for node_batch, edge_batch in tool.ingest():
            nodes.extend(node_batch)
            edges.extend(edge_batch)
        
        print(f"Ingestion tool processed {len(nodes)} nodes and {len(edges)} edges")
        return True
        
    except Exception as e:
        print(f"Ingestion tool test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_with_complex_project():
    """Test with a more complex Informatica project."""
    print("\nTesting with complex Informatica project...")
    
    test_directory = "/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode/data/informatica/sazzad-amt/ExploreInformatica Project"
    
    if not os.path.exists(test_directory):
        print(f"Complex test directory not found: {test_directory}")
        return False
    
    try:
        # Initialize ingestion tool
        tool = InformaticaIngestionTool(test_directory)
        
        nodes = []
        edges = []
        
        # Process files
        for node_batch, edge_batch in tool.ingest():
            nodes.extend(node_batch)
            edges.extend(edge_batch)
        
        print(f"Complex project processed {len(nodes)} nodes and {len(edges)} edges")
        
        # Analyze node types
        node_types = {}
        for node in nodes:
            node_type = node.node_type
            node_types[node_type] = node_types.get(node_type, 0) + 1
        
        print("Node types found:")
        for node_type, count in node_types.items():
            print(f"  {node_type}: {count}")
        
        # Analyze edge types
        edge_types = {}
        for edge in edges:
            edge_type = edge.relation
            edge_types[edge_type] = edge_types.get(edge_type, 0) + 1
        
        print("Edge types found:")
        for edge_type, count in edge_types.items():
            print(f"  {edge_type}: {count}")
        
        return True
        
    except Exception as e:
        print(f"Complex project test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("INFORMATICA PARSER TESTS")
    print("=" * 60)
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Basic parser functionality
    if test_parser_basic():
        tests_passed += 1
        print("✓ Basic parser test PASSED")
    else:
        print("✗ Basic parser test FAILED")
    
    # Test 2: Ingestion tool functionality  
    if test_ingestion_tool():
        tests_passed += 1
        print("✓ Ingestion tool test PASSED")
    else:
        print("✗ Ingestion tool test FAILED")
    
    # Test 3: Complex project test
    if test_with_complex_project():
        tests_passed += 1
        print("✓ Complex project test PASSED")
    else:
        print("✗ Complex project test FAILED")
    
    print("\n" + "=" * 60)
    print(f"RESULTS: {tests_passed}/{total_tests} tests passed")
    print("=" * 60)
    
    return tests_passed == total_tests


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)