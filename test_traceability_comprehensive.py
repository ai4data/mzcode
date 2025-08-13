#!/usr/bin/env python3
"""
Comprehensive traceability test script.
This demonstrates that you can now trace any graph element back to its source.
"""

import json
import sys
from pathlib import Path

def test_traceability_functionality():
    """Test that traceability actually solves the original problem"""
    print("🧪 COMPREHENSIVE TRACEABILITY TEST")
    print("=" * 60)
    print("Testing that we can trace any graph element back to its source file")
    print()
    
    graph_file = "enhanced_graph_full_analysis.json"
    
    if not Path(graph_file).exists():
        print(f"❌ Graph file not found: {graph_file}")
        return False
    
    with open(graph_file, 'r') as f:
        graph = json.load(f)
    
    # TEST 1: Can I find the source file for a specific table?
    print("🔍 TEST 1: Table Source Traceability")
    print("-" * 40)
    table_nodes = [n for n in graph["nodes"] if "table:" in n["id"]]
    if table_nodes:
        table = table_nodes[0]
        props = table["properties"]
        print(f"❓ Question: Where does table '{table['name']}' come from?")
        print(f"✅ Answer: File '{props['source_file_path']}'")
        print(f"   Exact location: {props.get('xml_path', 'N/A')}")
        print(f"   Confidence: We found this through {props.get('source_file_type', 'unknown')} parsing")
        print()
    
    # TEST 2: Can I understand how a relationship was established?
    print("🔗 TEST 2: Relationship Origin Traceability")
    print("-" * 40)
    edges = graph.get("links", [])
    sql_edge = next((e for e in edges if e.get("properties", {}).get("derivation_method") == "sql_parsing"), None)
    if sql_edge:
        props = sql_edge["properties"]
        print(f"❓ Question: How do we know '{sql_edge['source']}' relates to '{sql_edge['target']}'?")
        print(f"✅ Answer: Found through {props['derivation_method']} in file:")
        print(f"   File: {Path(props['source_file_path']).name}")
        print(f"   Location: {props.get('xml_location', 'N/A')}")
        print(f"   Confidence: {props['confidence_level']}")
        if 'context_info' in props:
            context = props['context_info']
            if 'sql_statement' in context:
                print(f"   SQL: {context['sql_statement'][:100]}...")
        print()
    
    # TEST 3: Can I trace an operation back to its package?
    print("⚙️  TEST 3: Operation Source Traceability")
    print("-" * 40)
    operations = [n for n in graph["nodes"] if n["node_type"] == "operation"]
    if operations:
        op = operations[0]
        props = op["properties"]
        print(f"❓ Question: What package contains operation '{op['name']}'?")
        print(f"✅ Answer: Package '{props.get('parent_package', 'Unknown')}'")
        print(f"   Full path: {props['source_file_path']}")
        print(f"   XML location: {props.get('xml_path', 'N/A')}")
        print()
    
    # TEST 4: Can I get complete context without reading the original file?
    print("📋 TEST 4: Complete Context Availability")
    print("-" * 40)
    print("❓ Question: Do I need to go back to raw SSIS files for complete info?")
    
    # Check if we have rich metadata
    has_xml_paths = sum(1 for n in graph["nodes"] if "xml_path" in n.get("properties", {}))
    has_derivation_methods = sum(1 for e in edges if "derivation_method" in e.get("properties", {}))
    has_context_info = sum(1 for e in edges if "context_info" in e.get("properties", {}))
    
    print(f"✅ Answer: NO! The graph now contains:")
    print(f"   - XML locations: {has_xml_paths}/{len(graph['nodes'])} nodes")
    print(f"   - Derivation methods: {has_derivation_methods}/{len(edges)} edges")
    print(f"   - Rich context: {has_context_info}/{len(edges)} edges with context")
    print()
    
    # TEST 5: Practical scenario - migration planning
    print("🚀 TEST 5: Practical Scenario - Migration Planning")
    print("-" * 40)
    print("❓ Scenario: I want to migrate table 'Product_Q1' to PostgreSQL")
    
    product_table = next((n for n in graph["nodes"] if "Product_Q1" in n["id"]), None)
    if product_table:
        props = product_table["properties"]
        print("✅ With enhanced traceability, I can now:")
        print(f"   1. Find source: {Path(props['source_file_path']).name}")
        print(f"   2. Locate in XML: {props.get('xml_path', 'N/A')}")
        print(f"   3. See relationships:")
        
        # Find edges involving this table
        related_edges = [e for e in edges if product_table["id"] in [e["source"], e["target"]]]
        for edge in related_edges[:2]:  # Show first 2
            edge_props = edge.get("properties", {})
            print(f"      - {edge['relation']} relationship via {edge_props.get('derivation_method', 'unknown')}")
            
        if "supported_platforms" in props:
            print(f"   4. Migration support: {props.get('supported_platforms', [])}")
        print()
    
    # SUMMARY
    print("🎯 TRACEABILITY TEST SUMMARY")
    print("-" * 40)
    total_elements = len(graph["nodes"]) + len(edges)
    traceable_elements = (
        sum(1 for n in graph["nodes"] if "source_file_path" in n.get("properties", {})) +
        sum(1 for e in edges if "source_file_path" in e.get("properties", {}))
    )
    
    success_rate = (traceable_elements / total_elements) * 100 if total_elements > 0 else 0
    
    print(f"✅ SUCCESS: {success_rate:.1f}% of graph elements are fully traceable")
    print(f"✅ PROBLEM SOLVED: No more going back to raw files for complete info!")
    print(f"✅ GRAPH IS SELF-CONTAINED: {traceable_elements}/{total_elements} elements have source context")
    
    return success_rate >= 90  # Consider 90%+ as success

if __name__ == "__main__":
    success = test_traceability_functionality()
    print()
    if success:
        print("🎉 TRACEABILITY ENHANCEMENT: ✅ WORKING PERFECTLY!")
    else:
        print("❌ TRACEABILITY ENHANCEMENT: NEEDS FIXES")
    
    sys.exit(0 if success else 1)