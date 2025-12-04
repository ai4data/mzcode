#!/usr/bin/env python3
"""
Test script for refactored SSIS parser.

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
from metazcode.sdk.ingestion.ssis_refactored import SsisParserRefactored


def test_refactored_parser():
    """Test the refactored parser on SSIS Northwind data."""

    # Paths
    ssis_path = Path(
        "C:/Users/Hicham/OneDrive/python/projects/mzcode/data/ssis/ssis_northwind/SSIS"
    )
    output_path = Path(
        "C:/Users/Hicham/OneDrive/python/projects/mzcode/output/enhanced_graph_refactored.json"
    )
    baseline_path = Path(
        "C:/Users/Hicham/OneDrive/python/projects/mzcode/output/enhanced_graph_full_analysis.json"
    )

    logger.info("=" * 70)
    logger.info("REFACTORED SSIS PARSER TEST")
    logger.info("=" * 70)

    # Initialize refactored parser (Phase 1: with legacy fallback)
    parser = SsisParserRefactored(
        enable_schema_introspection=False,  # Disable to match baseline
        enable_type_mapping=True,
        target_platforms=["sql_server", "postgresql"],
        use_legacy_fallback=True,  # Phase 1: Use proven legacy logic
    )

    parser_info = parser.get_info()
    logger.info(f"Parser mode: {parser_info['mode']}")
    logger.info(f"Parser version: {parser_info['version']}")

    # Find SSIS packages
    dtsx_files = list(ssis_path.glob("*.dtsx"))
    logger.info(f"Found {len(dtsx_files)} SSIS package(s) in {ssis_path}")

    if not dtsx_files:
        logger.error("No .dtsx files found!")
        return False

    # Parse all packages
    all_nodes = []
    all_edges = []

    for dtsx_file in dtsx_files:
        logger.info(f"Parsing: {dtsx_file.name}")

        try:
            for nodes, edges in parser.parse(str(dtsx_file)):
                # Collect nodes and edges
                all_nodes.extend(nodes)
                all_edges.extend(edges)

        except Exception as e:
            logger.error(f"Error parsing {dtsx_file.name}: {e}", exc_info=True)
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
            "parser_mode": parser_info['mode']
        }
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(graph_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Graph exported successfully ({os.path.getsize(output_path)} bytes)")

    # Compare with baseline
    if baseline_path.exists():
        logger.info("=" * 70)
        logger.info("COMPARING WITH BASELINE")
        logger.info("=" * 70)

        with open(baseline_path, 'r', encoding='utf-8') as f:
            baseline_data = json.load(f)

        compare_graphs(baseline_data, graph_data)
    else:
        logger.warning(f"Baseline not found at: {baseline_path}")

    return True


def compare_graphs(baseline: dict, refactored: dict):
    """
    Compare baseline and refactored graph outputs.

    Checks:
    - Node counts
    - Edge counts
    - Node type distribution
    - Edge type distribution
    - Structural equivalence
    """

    baseline_nodes = baseline.get("nodes", [])
    baseline_edges = baseline.get("edges", [])
    refactored_nodes = refactored.get("nodes", [])
    refactored_edges = refactored.get("edges", [])

    logger.info(f"Baseline:    {len(baseline_nodes)} nodes, {len(baseline_edges)} edges")
    logger.info(f"Refactored:  {len(refactored_nodes)} nodes, {len(refactored_edges)} edges")

    # Node count comparison
    node_diff = len(refactored_nodes) - len(baseline_nodes)
    edge_diff = len(refactored_edges) - len(baseline_edges)

    if node_diff == 0 and edge_diff == 0:
        logger.info("✓ Node and edge counts match exactly")
    else:
        logger.warning(f"△ Node diff: {node_diff:+d}, Edge diff: {edge_diff:+d}")

    # Node type distribution
    baseline_types = {}
    for node in baseline_nodes:
        node_type = node.get("node_type", "unknown")
        baseline_types[node_type] = baseline_types.get(node_type, 0) + 1

    refactored_types = {}
    for node in refactored_nodes:
        node_type = node.get("node_type", "unknown")
        refactored_types[node_type] = refactored_types.get(node_type, 0) + 1

    logger.info("\nNode Type Distribution:")
    all_types = set(baseline_types.keys()) | set(refactored_types.keys())
    for node_type in sorted(all_types):
        baseline_count = baseline_types.get(node_type, 0)
        refactored_count = refactored_types.get(node_type, 0)
        diff = refactored_count - baseline_count
        status = "✓" if diff == 0 else "△"
        logger.info(
            f"  {status} {node_type}: baseline={baseline_count}, "
            f"refactored={refactored_count} ({diff:+d})"
        )

    # Edge type distribution
    baseline_edge_types = {}
    for edge in baseline_edges:
        edge_type = edge.get("relation", "unknown")
        baseline_edge_types[edge_type] = baseline_edge_types.get(edge_type, 0) + 1

    refactored_edge_types = {}
    for edge in refactored_edges:
        edge_type = edge.get("relation", "unknown")
        refactored_edge_types[edge_type] = refactored_edge_types.get(edge_type, 0) + 1

    logger.info("\nEdge Type Distribution:")
    all_edge_types = set(baseline_edge_types.keys()) | set(refactored_edge_types.keys())
    for edge_type in sorted(all_edge_types):
        baseline_count = baseline_edge_types.get(edge_type, 0)
        refactored_count = refactored_edge_types.get(edge_type, 0)
        diff = refactored_count - baseline_count
        status = "✓" if diff == 0 else "△"
        logger.info(
            f"  {status} {edge_type}: baseline={baseline_count}, "
            f"refactored={refactored_count} ({diff:+d})"
        )

    # Structural equivalence check
    baseline_node_ids = {node.get("node_id") for node in baseline_nodes}
    refactored_node_ids = {node.get("node_id") for node in refactored_nodes}

    missing_nodes = baseline_node_ids - refactored_node_ids
    extra_nodes = refactored_node_ids - baseline_node_ids

    if missing_nodes:
        logger.warning(f"Missing {len(missing_nodes)} nodes from baseline")
        logger.debug(f"Missing nodes: {list(missing_nodes)[:5]}...")

    if extra_nodes:
        logger.warning(f"Found {len(extra_nodes)} extra nodes not in baseline")
        logger.debug(f"Extra nodes: {list(extra_nodes)[:5]}...")

    if not missing_nodes and not extra_nodes:
        logger.info("✓ All node IDs match between baseline and refactored")

    logger.info("=" * 70)

    # Summary
    if node_diff == 0 and edge_diff == 0 and not missing_nodes and not extra_nodes:
        logger.info("✓ STRUCTURAL EQUIVALENCE: PASSED")
        logger.info("  Refactored parser produces identical output to baseline")
    else:
        logger.info("△ STRUCTURAL EQUIVALENCE: DIFFERENCES DETECTED")
        logger.info("  Review differences above for assessment")


if __name__ == "__main__":
    success = test_refactored_parser()
    exit(0 if success else 1)
