"""
Post-Processor for SSIS Refactored Parser

Coordinates all post-processing steps to transform raw extraction into
fully enriched, analyzed graph with:
1. Node de-duplication and cleanup
2. Cross-package dependency analysis
3. LLM-powered node enrichment
4. LLM-powered edge enrichment
"""

import logging
from typing import List, Dict, Any, Optional

from ....models.graph import Node, Edge
from ....graph.graph_client_interface import GraphClientInterface
from ....analysis.cross_package_analyzer import CrossPackageAnalyzer
from ....enrichment.node_enricher import NodeEnricher
from ....enrichment.edge_enricher import EdgeEnricher
from .graph_cleaner import GraphCleaner

logger = logging.getLogger(__name__)


class PostProcessor:
    """
    Orchestrates post-processing pipeline for SSIS parsed graphs.

    Pipeline stages:
    1. Graph Cleaning - De-duplicate and normalize nodes/edges
    2. Cross-Package Analysis - Identify dependencies and execution order
    3. Node Enrichment - Add LLM summaries to operations and pipelines
    4. Edge Enrichment - Add LLM summaries to semantic edges
    """

    def __init__(
        self,
        graph_client: GraphClientInterface,
        enable_cross_package_analysis: bool = True,
        enable_llm_enrichment: bool = True,
        llm_client: Optional[Any] = None,
        skip_enriched: bool = True,
    ):
        """
        Initialize the post-processor.

        Args:
            graph_client: Graph client interface for accessing and updating the graph
            enable_cross_package_analysis: Whether to run cross-package analysis
            enable_llm_enrichment: Whether to run LLM enrichment
            llm_client: LLM client for enrichment (required if enable_llm_enrichment=True)
            skip_enriched: Whether to skip already enriched nodes/edges
        """
        self.graph_client = graph_client
        self.enable_cross_package_analysis = enable_cross_package_analysis
        self.enable_llm_enrichment = enable_llm_enrichment
        self.skip_enriched = skip_enriched

        # Initialize components
        self.graph_cleaner = GraphCleaner()

        if self.enable_cross_package_analysis:
            self.cross_package_analyzer = CrossPackageAnalyzer(graph_client)

        if self.enable_llm_enrichment:
            if llm_client is None:
                logger.warning("LLM enrichment enabled but no LLM client provided. Disabling enrichment.")
                self.enable_llm_enrichment = False
            else:
                self.node_enricher = NodeEnricher(graph_client, llm_client, skip_enriched)
                self.edge_enricher = EdgeEnricher(graph_client, llm_client, skip_enriched)

        # Statistics
        self.stats = {
            "total_processing_time": 0.0,
            "cleanup_stats": {},
            "cross_package_stats": {},
            "node_enrichment_stats": {},
            "edge_enrichment_stats": {},
        }

    def process(self, nodes: List[Node], edges: List[Edge]) -> Dict[str, Any]:
        """
        Run the full post-processing pipeline.

        Args:
            nodes: Raw nodes from parser
            edges: Raw edges from parser

        Returns:
            Statistics about the post-processing
        """
        import time
        start_time = time.time()

        logger.info("="*70)
        logger.info("STARTING POST-PROCESSING PIPELINE")
        logger.info("="*70)

        # Stage 1: Graph Cleaning
        logger.info("\n[Stage 1/4] Graph Cleaning...")
        cleaned_nodes, cleaned_edges = self.graph_cleaner.clean(nodes, edges)
        self.stats["cleanup_stats"] = self.graph_cleaner.get_statistics()

        # Update graph with cleaned nodes and edges
        self._update_graph_with_cleaned_data(cleaned_nodes, cleaned_edges)

        # Stage 2: Cross-Package Analysis
        if self.enable_cross_package_analysis:
            logger.info("\n[Stage 2/4] Cross-Package Dependency Analysis...")
            try:
                cross_package_results = self.cross_package_analyzer.analyze()
                self.stats["cross_package_stats"] = cross_package_results
            except Exception as e:
                logger.error(f"Cross-package analysis failed: {e}", exc_info=True)
                self.stats["cross_package_stats"] = {"error": str(e)}
        else:
            logger.info("\n[Stage 2/4] Cross-Package Analysis (SKIPPED)")

        # Stage 3: Node Enrichment
        if self.enable_llm_enrichment:
            logger.info("\n[Stage 3/4] LLM-Powered Node Enrichment...")
            try:
                # Get all operation and pipeline nodes
                enrichable_nodes = self._get_enrichable_nodes(cleaned_nodes)
                node_ids = [node.node_id for node in enrichable_nodes]
                logger.info(f"Found {len(node_ids)} nodes to enrich")

                node_enrichment_results = self.node_enricher.enrich_nodes(node_ids)
                self.stats["node_enrichment_stats"] = node_enrichment_results
            except Exception as e:
                logger.error(f"Node enrichment failed: {e}", exc_info=True)
                self.stats["node_enrichment_stats"] = {"error": str(e)}
        else:
            logger.info("\n[Stage 3/4] LLM Node Enrichment (SKIPPED)")

        # Stage 4: Edge Enrichment
        if self.enable_llm_enrichment:
            logger.info("\n[Stage 4/4] LLM-Powered Edge Enrichment...")
            try:
                edge_enrichment_results = self.edge_enricher.enrich_semantic_edges()
                self.stats["edge_enrichment_stats"] = edge_enrichment_results
            except Exception as e:
                logger.error(f"Edge enrichment failed: {e}", exc_info=True)
                self.stats["edge_enrichment_stats"] = {"error": str(e)}
        else:
            logger.info("\n[Stage 4/4] LLM Edge Enrichment (SKIPPED)")

        elapsed_time = time.time() - start_time
        self.stats["total_processing_time"] = elapsed_time

        logger.info("\n" + "="*70)
        logger.info("POST-PROCESSING COMPLETE")
        logger.info("="*70)
        logger.info(f"Total processing time: {elapsed_time:.2f}s")
        logger.info(f"Nodes: {self.stats['cleanup_stats'].get('nodes_before', 0)} → "
                   f"{self.stats['cleanup_stats'].get('nodes_after', 0)}")
        logger.info(f"Edges: {self.stats['cleanup_stats'].get('edges_before', 0)} → "
                   f"{self.stats['cleanup_stats'].get('edges_after', 0)}")

        if self.enable_cross_package_analysis:
            cross_stats = self.stats.get("cross_package_stats", {})
            logger.info(f"Cross-package edges added: {cross_stats.get('cross_package_edges_added', 0)}")

        if self.enable_llm_enrichment:
            node_stats = self.stats.get("node_enrichment_stats", {})
            edge_stats = self.stats.get("edge_enrichment_stats", {})
            logger.info(f"Nodes enriched: {node_stats.get('successfully_enriched', 0)}/{node_stats.get('total_processed', 0)}")
            logger.info(f"Edges enriched: {edge_stats.get('successfully_enriched', 0)}/{edge_stats.get('semantic_edges', 0)}")

        return self.stats

    def _update_graph_with_cleaned_data(self, nodes: List[Node], edges: List[Edge]):
        """
        Update the graph client with cleaned nodes and edges.

        This replaces the raw extraction data with cleaned, deduplicated data.
        """
        try:
            # Get the underlying graph
            graph = self.graph_client.get_graph()

            # Clear existing graph data
            graph.clear()

            # Add cleaned nodes
            for node in nodes:
                # Handle both string and enum values for node_type
                node_type_value = node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type)

                graph.add_node(
                    node.node_id,
                    node_type=node_type_value,
                    name=node.name,
                    **node.properties
                )

            # Add cleaned edges
            for edge in edges:
                # Handle both string and enum values for relation
                relation_value = edge.relation.value if hasattr(edge.relation, 'value') else str(edge.relation)

                graph.add_edge(
                    edge.source_id,
                    edge.target_id,
                    relation=relation_value,
                    **edge.properties
                )

            logger.debug(f"Updated graph with {len(nodes)} cleaned nodes and {len(edges)} cleaned edges")

        except Exception as e:
            logger.error(f"Failed to update graph with cleaned data: {e}", exc_info=True)

    def _get_enrichable_nodes(self, nodes: List[Node]) -> List[Node]:
        """
        Filter nodes to only those that should be enriched.

        Enrichable nodes:
        - OPERATION nodes (data flow tasks, execute SQL tasks, etc.)
        - PIPELINE nodes (packages)
        """
        from ....models.canonical_types import NodeType

        enrichable = []
        for node in nodes:
            if node.node_type in [NodeType.OPERATION, NodeType.PIPELINE]:
                enrichable.append(node)

        return enrichable

    def get_statistics(self) -> Dict[str, Any]:
        """Get post-processing statistics."""
        return self.stats.copy()
