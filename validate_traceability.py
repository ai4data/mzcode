#!/usr/bin/env python3
"""
Validation script for enhanced traceability in the generated graph.
This script verifies that all nodes and edges have proper source file traceability.
"""

import json
import sys
from pathlib import Path


def validate_node_traceability(node):
    """Validate that a node has proper traceability information"""
    node_id = node.get("id", "unknown")
    properties = node.get("properties", {})
    
    results = {
        "node_id": node_id,
        "has_source_file_path": "source_file_path" in properties,
        "has_source_file_type": "source_file_type" in properties,
        "has_technology": "technology" in properties,
        "has_xml_path": "xml_path" in properties,
        "source_file_path": properties.get("source_file_path", "MISSING"),
        "source_file_type": properties.get("source_file_type", "MISSING"),
        "technology": properties.get("technology", "MISSING"),
    }
    
    results["is_fully_traceable"] = all([
        results["has_source_file_path"],
        results["has_source_file_type"], 
        results["has_technology"]
    ])
    
    return results


def validate_edge_traceability(edge):
    """Validate that an edge has proper traceability information"""
    edge_key = f"{edge.get('source', 'unknown')} -> {edge.get('target', 'unknown')} [{edge.get('relation', 'unknown')}]"
    properties = edge.get("properties", {})
    
    results = {
        "edge_key": edge_key,
        "has_source_file_path": "source_file_path" in properties,
        "has_derivation_method": "derivation_method" in properties,
        "has_confidence_level": "confidence_level" in properties,
        "has_technology": "technology" in properties,
        "source_file_path": properties.get("source_file_path", "MISSING"),
        "derivation_method": properties.get("derivation_method", "MISSING"),
        "confidence_level": properties.get("confidence_level", "MISSING"),
        "technology": properties.get("technology", "MISSING"),
    }
    
    results["is_fully_traceable"] = all([
        results["has_source_file_path"],
        results["has_derivation_method"],
        results["has_confidence_level"],
        results["has_technology"]
    ])
    
    return results


def validate_graph_traceability(graph_file):
    """Validate traceability for an entire graph"""
    print(f"🔍 Validating traceability in: {graph_file}")
    print("=" * 80)
    
    with open(graph_file, 'r') as f:
        graph = json.load(f)
    
    nodes = graph.get("nodes", [])
    edges = graph.get("links", [])
    
    print(f"📊 Graph Summary:")
    print(f"   Nodes: {len(nodes)}")
    print(f"   Edges: {len(edges)}")
    print()
    
    # Validate nodes
    print("🔎 Validating Node Traceability:")
    print("-" * 40)
    
    fully_traceable_nodes = 0
    node_results = []
    
    for node in nodes:
        result = validate_node_traceability(node)
        node_results.append(result)
        
        if result["is_fully_traceable"]:
            fully_traceable_nodes += 1
            print(f"✅ {result['node_id'][:50]:<50} | {result['source_file_path'].split('/')[-1]}")
        else:
            print(f"❌ {result['node_id'][:50]:<50} | MISSING TRACEABILITY")
            missing = []
            if not result["has_source_file_path"]: missing.append("source_file_path")
            if not result["has_source_file_type"]: missing.append("source_file_type")
            if not result["has_technology"]: missing.append("technology")
            print(f"   Missing: {', '.join(missing)}")
    
    print()
    print(f"Node Traceability Summary: {fully_traceable_nodes}/{len(nodes)} nodes fully traceable")
    print()
    
    # Validate edges
    print("🔗 Validating Edge Traceability:")
    print("-" * 40)
    
    fully_traceable_edges = 0
    edge_results = []
    
    for edge in edges:
        result = validate_edge_traceability(edge)
        edge_results.append(result)
        
        if result["is_fully_traceable"]:
            fully_traceable_edges += 1
            print(f"✅ {result['derivation_method']:<15} | {result['source_file_path'].split('/')[-1]}")
        else:
            print(f"❌ {result['edge_key'][:70]:<70} | MISSING TRACEABILITY")
            missing = []
            if not result["has_source_file_path"]: missing.append("source_file_path")
            if not result["has_derivation_method"]: missing.append("derivation_method")
            if not result["has_confidence_level"]: missing.append("confidence_level")
            if not result["has_technology"]: missing.append("technology")
            print(f"   Missing: {', '.join(missing)}")
    
    print()
    print(f"Edge Traceability Summary: {fully_traceable_edges}/{len(edges)} edges fully traceable")
    print()
    
    # Overall results
    print("🎯 Overall Traceability Results:")
    print("-" * 40)
    total_elements = len(nodes) + len(edges)
    fully_traceable = fully_traceable_nodes + fully_traceable_edges
    percentage = (fully_traceable / total_elements * 100) if total_elements > 0 else 0
    
    print(f"Total Elements: {total_elements}")
    print(f"Fully Traceable: {fully_traceable}")
    print(f"Traceability Rate: {percentage:.1f}%")
    
    if percentage == 100:
        print("🎉 PERFECT TRACEABILITY ACHIEVED!")
        return True
    else:
        print(f"⚠️  {total_elements - fully_traceable} elements need traceability fixes")
        return False
    
    print()


if __name__ == "__main__":
    graph_file = "/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode/enhanced_graph_full_analysis.json"
    
    if not Path(graph_file).exists():
        print(f"❌ Graph file not found: {graph_file}")
        sys.exit(1)
    
    success = validate_graph_traceability(graph_file)
    sys.exit(0 if success else 1)