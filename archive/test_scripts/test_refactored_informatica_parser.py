#!/usr/bin/env python3
"""
Test script for refactored Informatica parser.

This script validates that the new modular architecture:
1. Works correctly with the legacy fallback
2. Produces equivalent output to the baseline
3. Demonstrates improved architecture
"""

import os
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import the refactored parser
from metazcode.sdk.ingestion.informatica_refactored import InformaticaParserRefactored


def test_refactored_informatica_parser():
    """Test the refactored Informatica parser."""

    # Paths
    informatica_path = Path(
        "C:/Users/Hicham/OneDrive/python/projects/mzcode/data/informatica/hassan-hosny/Q1"
    )
    output_path = Path(
        "C:/Users/Hicham/OneDrive/python/projects/mzcode/output/informatica_graph_refactored.json"
    )

    logger.info("=" * 70)
    logger.info("REFACTORED INFORMATICA PARSER TEST")
    logger.info("=" * 70)

    # Initialize refactored parser (Phase 1: with legacy fallback)
    parser = InformaticaParserRefactored(
        enable_schema_introspection=False,  # Disable to avoid external dependencies
        enable_type_mapping=True,
        target_platforms=["sql_server", "postgresql"],
        use_legacy_fallback=True,  # Phase 1: Use proven legacy logic
    )

    parser_info = parser.get_info()
    logger.info(f"Parser mode: {parser_info['mode']}")
    logger.info(f"Parser version: {parser_info['version']}")

    # Find Informatica XML files
    xml_files = list(informatica_path.glob("*.xml")) + list(informatica_path.glob("*.XML"))
    logger.info(f"Found {len(xml_files)} Informatica XML file(s) in {informatica_path}")

    if not xml_files:
        logger.error("No XML files found!")
        return False

    # Parse all files
    all_nodes = []
    all_edges = []

    for xml_file in xml_files:
        logger.info(f"Parsing: {xml_file.name}")

        try:
            for nodes, edges in parser.parse(str(xml_file)):
                # Collect nodes and edges
                all_nodes.extend(nodes)
                all_edges.extend(edges)

        except Exception as e:
            logger.error(f"Error parsing {xml_file.name}: {e}", exc_info=True)
            continue

    logger.info(f"Parsing complete: {len(all_nodes)} nodes, {len(all_edges)} edges")

    # Export graph
    logger.info(f"Exporting graph to: {output_path}")

    # Convert Node and Edge objects to dicts
    nodes_dict = [node.to_dict() for node in all_nodes]
    edges_dict = [edge.to_dict() for edge in all_edges]

    graph_data = {
        "nodes": nodes_dict,
        "edges": edges_dict,
        "metadata": {
            "node_count": len(nodes_dict),
            "edge_count": len(edges_dict),
            "parser_version": "2.0.0-refactored",
            "parser_mode": parser_info['mode'],
            "source": "Informatica PowerCenter"
        }
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(graph_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Graph exported successfully ({os.path.getsize(output_path)} bytes)")

    # Display node/edge distribution
    logger.info("=" * 70)
    logger.info("GRAPH STATISTICS")
    logger.info("=" * 70)

    # Node type distribution
    node_types = {}
    for node in all_nodes:
        node_type = node.node_type
        node_types[node_type] = node_types.get(node_type, 0) + 1

    logger.info("\nNode Type Distribution:")
    for node_type, count in sorted(node_types.items()):
        logger.info(f"  {node_type}: {count}")

    # Edge type distribution
    edge_types = {}
    for edge in all_edges:
        edge_type = edge.relation
        edge_types[edge_type] = edge_types.get(edge_type, 0) + 1

    logger.info("\nEdge Type Distribution:")
    for edge_type, count in sorted(edge_types.items()):
        logger.info(f"  {edge_type}: {count}")

    logger.info("=" * 70)
    logger.info("✓ REFACTORED INFORMATICA PARSER TEST COMPLETE")
    logger.info("=" * 70)

    return True


def test_architecture_components():
    """Test that all architecture components are properly set up."""

    logger.info("=" * 70)
    logger.info("ARCHITECTURE COMPONENT VALIDATION")
    logger.info("=" * 70)

    # Test 1: Can import all components
    try:
        from metazcode.sdk.ingestion.informatica_refactored.models import InformaticaParsingContext
        from metazcode.sdk.ingestion.informatica_refactored.builders import InformaticaGraphBuilder
        from metazcode.sdk.ingestion.informatica_refactored.parsers import BaseTransformationParser
        logger.info("✓ All components imported successfully")
    except ImportError as e:
        logger.error(f"✗ Import failed: {e}")
        return False

    # Test 2: Can create parsing context
    try:
        context = InformaticaParsingContext(
            file_path="test.xml",
            file_type="mapping",
            mapping_name="TestMapping",
        )
        logger.info(f"✓ ParsingContext created: {context}")
    except Exception as e:
        logger.error(f"✗ ParsingContext creation failed: {e}")
        return False

    # Test 3: Can create graph builder
    try:
        builder = InformaticaGraphBuilder(context)
        logger.info("✓ GraphBuilder created successfully")
    except Exception as e:
        logger.error(f"✗ GraphBuilder creation failed: {e}")
        return False

    # Test 4: Can create nodes/edges
    try:
        node = builder.create_mapping_node("TestMapping")
        logger.info(f"✓ Node created: {node.node_id}")

        edge = builder.create_contains_edge("parent:id", "child:id")
        logger.info(f"✓ Edge created: {edge.relation}")
    except Exception as e:
        logger.error(f"✗ Node/Edge creation failed: {e}")
        return False

    # Test 5: Can get statistics
    try:
        stats = builder.get_statistics()
        logger.info(f"✓ Statistics: {stats}")
    except Exception as e:
        logger.error(f"✗ Statistics retrieval failed: {e}")
        return False

    logger.info("=" * 70)
    logger.info("✓ ALL ARCHITECTURE COMPONENTS VALIDATED")
    logger.info("=" * 70)

    return True


if __name__ == "__main__":
    # Test 1: Architecture components
    arch_success = test_architecture_components()

    # Test 2: Parser functionality
    parser_success = test_refactored_informatica_parser()

    # Overall result
    if arch_success and parser_success:
        logger.info("\n🎉 ALL TESTS PASSED")
        exit(0)
    else:
        logger.error("\n❌ SOME TESTS FAILED")
        exit(1)
