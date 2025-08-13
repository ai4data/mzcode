#!/usr/bin/env python3
"""
Quick test to visually inspect traceability properties in the generated graph.
"""

import json
from pathlib import Path

def test_traceability_visual():
    """Show examples of traceability in action"""
    graph_file = "enhanced_graph_full_analysis.json"
    
    if not Path(graph_file).exists():
        print(f"❌ Graph file not found: {graph_file}")
        return
    
    with open(graph_file, 'r') as f:
        graph = json.load(f)
    
    print("🔍 TRACEABILITY TEST RESULTS")
    print("=" * 60)
    
    # Test 1: Show a pipeline node
    print("\n📁 PIPELINE NODE TRACEABILITY:")
    print("-" * 40)
    pipeline_node = next((n for n in graph["nodes"] if n["node_type"] == "pipeline"), None)
    if pipeline_node:
        props = pipeline_node["properties"]
        print(f"Node ID: {pipeline_node['id']}")
        print(f"Source File: {props.get('source_file_path', 'MISSING')}")
        print(f"File Type: {props.get('source_file_type', 'MISSING')}")
        print(f"XML Location: {props.get('xml_path', 'MISSING')}")
    
    # Test 2: Show an operation node
    print("\n⚙️  OPERATION NODE TRACEABILITY:")
    print("-" * 40)
    operation_node = next((n for n in graph["nodes"] if n["node_type"] == "operation"), None)
    if operation_node:
        props = operation_node["properties"]
        print(f"Node ID: {operation_node['id']}")
        print(f"Source File: {props.get('source_file_path', 'MISSING')}")
        print(f"Parent Package: {props.get('parent_package', 'MISSING')}")
        print(f"XML Location: {props.get('xml_path', 'MISSING')}")
    
    # Test 3: Show a table node
    print("\n📊 TABLE NODE TRACEABILITY:")
    print("-" * 40)
    table_node = next((n for n in graph["nodes"] if "table:" in n["id"]), None)
    if table_node:
        props = table_node["properties"]
        print(f"Node ID: {table_node['id']}")
        print(f"Source File: {props.get('source_file_path', 'MISSING')}")
        print(f"Table Name: {props.get('table_name', 'MISSING')}")
        print(f"XML Location: {props.get('xml_path', 'MISSING')}")
    
    # Test 4: Show edge traceability
    print("\n🔗 EDGE TRACEABILITY:")
    print("-" * 40)
    edges = graph.get("links", [])
    for i, edge in enumerate(edges[:3]):  # Show first 3 edges
        props = edge.get("properties", {})
        print(f"\nEdge {i+1}: {edge['source']} -> {edge['target']}")
        print(f"  Relation: {edge['relation']}")
        print(f"  Source File: {props.get('source_file_path', 'MISSING')}")
        print(f"  Derivation: {props.get('derivation_method', 'MISSING')}")
        print(f"  Confidence: {props.get('confidence_level', 'MISSING')}")
        if 'context_info' in props:
            print(f"  Context: {props['context_info']}")
    
    # Test 5: Summary
    print("\n📈 TRACEABILITY SUMMARY:")
    print("-" * 40)
    total_nodes = len(graph["nodes"])
    total_edges = len(edges)
    
    nodes_with_path = sum(1 for n in graph["nodes"] if "source_file_path" in n.get("properties", {}))
    edges_with_path = sum(1 for e in edges if "source_file_path" in e.get("properties", {}))
    
    print(f"Nodes with source_file_path: {nodes_with_path}/{total_nodes}")
    print(f"Edges with source_file_path: {edges_with_path}/{total_edges}")
    print(f"Total traceability: {(nodes_with_path + edges_with_path)}/{total_nodes + total_edges}")

if __name__ == "__main__":
    test_traceability_visual()