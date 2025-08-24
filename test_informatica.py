#!/usr/bin/env python3
"""
Test script to verify Informatica parser implementation.
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from metazcode.sdk.ingestion.informatica.informatica_loader import InformaticaLoader
from metazcode.sdk.ingestion.informatica.informatica_parser import InformaticaParser

def test_informatica_parsing():
    """Test Informatica parsing with sample data."""
    
    # Test with Hassan-Hosny example
    hassan_hosny_path = project_root / "data" / "informatica" / "hassan-hosny" / "Q1"
    
    if hassan_hosny_path.exists():
        print("Testing Hassan-Hosny example...")
        loader = InformaticaLoader()
        loader.project_path = str(hassan_hosny_path)
        
        try:
            # Test discovery
            workflow_files = loader.discover_files("wf_*.xml")
            print(f"Found {len(workflow_files)} workflow files")
            
            param_files = loader.discover_files("*.param")
            print(f"Found {len(param_files)} parameter files")
            
            # Test parsing
            results = list(loader.ingest())
            print(f"Parsing returned {len(results)} result sets")
            
            for i, (nodes, edges) in enumerate(results):
                print(f"Result set {i+1}: {len(nodes)} nodes, {len(edges)} edges")
                
                # Show some sample nodes
                for node in nodes[:5]:  # Show first 5 nodes
                    print(f"  Node: {node.node_id} ({node.node_type}) - {node.name}")
                    # Show a few properties
                    prop_count = len(node.properties)
                    print(f"    Properties: {prop_count} total")
                    if prop_count > 0:
                        first_props = dict(list(node.properties.items())[:3])
                        print(f"    Sample props: {first_props}")
                
                # Show some sample edges
                for edge in edges[:5]:  # Show first 5 edges
                    print(f"  Edge: {edge.source_id} -> {edge.target_id} ({edge.relation})")
                    
        except Exception as e:
            print(f"Error testing Hassan-Hosny example: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("Hassan-Hosny example not found")
    
    # Test with Sazzad-AMT example
    sazzad_amt_path = project_root / "data" / "informatica" / "sazzad-amt" / "ExploreInformatica Project"
    
    if sazzad_amt_path.exists():
        print("\nTesting Sazzad-AMT example...")
        loader = InformaticaLoader()
        loader.project_path = str(sazzad_amt_path)
        
        try:
            # Test discovery
            workflow_files = loader.discover_files("WorkFlow_*.xml")
            print(f"Found {len(workflow_files)} workflow files")
            
            mapping_files = loader.discover_files("Mapping_*.xml")
            print(f"Found {len(mapping_files)} mapping files")
            
            param_files = loader.discover_files("*.param")
            print(f"Found {len(param_files)} parameter files")
            
            # Test parsing
            results = list(loader.ingest())
            print(f"Parsing returned {len(results)} result sets")
            
            for i, (nodes, edges) in enumerate(results):
                print(f"Result set {i+1}: {len(nodes)} nodes, {len(edges)} edges")
                
                # Count node types
                node_types = {}
                for node in nodes:
                    node_type = node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type)
                    node_types[node_type] = node_types.get(node_type, 0) + 1
                
                print(f"  Node types: {node_types}")
                
                # Count edge types
                edge_types = {}
                for edge in edges:
                    edge_type = edge.relation.value if hasattr(edge.relation, 'value') else str(edge.relation)
                    edge_types[edge_type] = edge_types.get(edge_type, 0) + 1
                
                print(f"  Edge types: {edge_types}")
                
        except Exception as e:
            print(f"Error testing Sazzad-AMT example: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("Sazzad-AMT example not found")

if __name__ == "__main__":
    test_informatica_parsing()