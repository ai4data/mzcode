#!/usr/bin/env python3
"""
Verify traceability in the Northwind enhanced graph
"""

import json

def verify_northwind_traceability():
    print("🔍 VERIFYING NORTHWIND TRACEABILITY")
    print("=" * 50)
    
    # Load the enhanced graph
    with open('enhanced_graph_full_analysis.json', 'r') as f:
        graph = json.load(f)
    
    nodes = graph['nodes']
    edges = graph['links']
    
    print(f"📊 Graph Summary:")
    print(f"   Total Nodes: {len(nodes)}")
    print(f"   Total Edges: {len(edges)}")
    print()
    
    # Check node traceability
    nodes_with_source_path = 0
    nodes_with_xml_path = 0
    
    for node in nodes:
        props = node.get('properties', {})
        if 'source_file_path' in props:
            nodes_with_source_path += 1
        if 'xml_path' in props:
            nodes_with_xml_path += 1
    
    print(f"🔎 NODE TRACEABILITY:")
    print(f"   Nodes with source_file_path: {nodes_with_source_path}/{len(nodes)}")
    print(f"   Nodes with xml_path: {nodes_with_xml_path}/{len(nodes)}")
    print()
    
    # Check edge traceability
    edges_with_source_path = 0
    edges_with_derivation = 0
    edges_with_confidence = 0
    edges_with_context = 0
    
    for edge in edges:
        props = edge.get('properties', {})
        if 'source_file_path' in props:
            edges_with_source_path += 1
        if 'derivation_method' in props:
            edges_with_derivation += 1
        if 'confidence_level' in props:
            edges_with_confidence += 1
        if 'context_info' in props:
            edges_with_context += 1
    
    print(f"🔗 EDGE TRACEABILITY:")
    print(f"   Edges with source_file_path: {edges_with_source_path}/{len(edges)}")
    print(f"   Edges with derivation_method: {edges_with_derivation}/{len(edges)}")
    print(f"   Edges with confidence_level: {edges_with_confidence}/{len(edges)}")
    print(f"   Edges with context_info: {edges_with_context}/{len(edges)}")
    print()
    
    # Show examples
    print("📋 EXAMPLE NODE (first pipeline):")
    print("-" * 30)
    first_pipeline = next(n for n in nodes if n['node_type'] == 'pipeline')
    props = first_pipeline['properties']
    print(f"Node ID: {first_pipeline['id']}")
    print(f"Source File: {props['source_file_path'].split('/')[-1]}")
    print(f"File Type: {props['source_file_type']}")
    print(f"XML Path: {props['xml_path']}")
    print()
    
    print("📋 EXAMPLE EDGE (first edge):")
    print("-" * 30)
    first_edge = edges[0]
    props = first_edge['properties']
    print(f"Relationship: {first_edge['source']} -> {first_edge['target']}")
    print(f"Relation: {first_edge['relation']}")
    print(f"Source File: {props['source_file_path'].split('/')[-1]}")
    print(f"Derivation: {props['derivation_method']}")
    print(f"Confidence: {props['confidence_level']}")
    print(f"XML Location: {props.get('xml_location', 'N/A')}")
    print()
    
    # Summary
    total_elements = len(nodes) + len(edges)
    traceable_elements = nodes_with_source_path + edges_with_source_path
    percentage = (traceable_elements / total_elements) * 100
    
    print(f"🎯 FINAL RESULT:")
    print(f"   Total Elements: {total_elements}")
    print(f"   Traceable Elements: {traceable_elements}")
    print(f"   Traceability Rate: {percentage:.1f}%")
    print()
    
    if percentage >= 95:
        print("✅ SUCCESS: Traceability enhancement is working perfectly!")
    else:
        print("⚠️  WARNING: Some elements are missing traceability")
    
    return percentage >= 95

if __name__ == "__main__":
    verify_northwind_traceability()