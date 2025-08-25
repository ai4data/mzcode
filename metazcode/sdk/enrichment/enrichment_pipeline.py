"""
Technology-Agnostic Enrichment Pipeline for LLM-based Node Summary Generation

This module orchestrates the complete enrichment process for any ETL/Data Pipeline
platform, managing the flow from node selection through batch processing to final
updates. It works with SSIS, Informatica, Talend, Airflow, and other platforms.
"""

import os
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from metazcode.sdk.graph.graph_client_interface import GraphClientInterface
from metazcode.sdk.models.canonical_types import NodeType
from .llm_client import OpenAIEnricher
from .llm_factory import LLMClientFactory
from .node_enricher import NodeEnricher
from .edge_enricher import EdgeEnricher
from .batch_processor import BatchProcessor

logger = logging.getLogger(__name__)


class EnrichmentPipeline:
    """
    Orchestrates the LLM enrichment process for graph nodes from any ETL platform.
    
    This pipeline manages the complete enrichment workflow for SSIS, Informatica,
    Talend, Airflow, and other data integration platforms, including:
    - Technology-neutral node selection and filtering
    - Batch processing for efficiency across platforms
    - Progress tracking and reporting
    - Error handling and recovery
    """
    
    def __init__(
        self, 
        graph_client: GraphClientInterface,
        provider: str = "openai",
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        batch_size: int = 10,
        **provider_kwargs
    ):
        """
        Initialize the enrichment pipeline.
        
        Args:
            graph_client: Interface to the graph database
            provider: LLM provider to use (openai, openrouter)
            model: LLM model to use for enrichment (uses provider default if not specified)
            api_key: API key for the LLM provider (defaults to env var)
            batch_size: Number of nodes to process in parallel
            **provider_kwargs: Additional provider-specific arguments
        """
        self.graph_client = graph_client
        self.provider = provider
        self.model = model
        self.batch_size = batch_size
        
        # Initialize LLM client using factory
        self.llm_client = LLMClientFactory.create_client(
            provider=provider,
            model=model,
            api_key=api_key,
            **provider_kwargs
        )
        
        # Initialize components
        self.node_enricher = NodeEnricher(graph_client, self.llm_client)
        self.edge_enricher = EdgeEnricher(graph_client, self.llm_client)
        self.batch_processor = BatchProcessor(self.node_enricher, batch_size)
        
        # Track pipeline statistics
        self.pipeline_stats = {
            "start_time": None,
            "end_time": None,
            "total_nodes": 0,
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0
        }
    
    def enrich_graph(self, node_types: Optional[List[str]] = None, include_edges: bool = True) -> Dict[str, Any]:
        """
        Enrich all eligible nodes and edges in the graph with LLM summaries.
        
        Args:
            node_types: Optional list of node types to enrich (defaults to operation/pipeline)
            include_edges: Whether to also enrich semantic edges (defaults to True)
            
        Returns:
            Summary of the enrichment process
        """
        self.pipeline_stats["start_time"] = datetime.utcnow()
        logger.info("Starting enrichment pipeline")
        
        try:
            # Phase 1: Node Enrichment
            logger.info("Phase 1: Node enrichment")
            nodes_to_enrich = self._select_nodes_for_enrichment(node_types)
            self.pipeline_stats["total_nodes"] = len(nodes_to_enrich)
            
            if nodes_to_enrich:
                logger.info(f"Found {len(nodes_to_enrich)} nodes to enrich")
                
                # Estimate processing time
                estimated_time = self._estimate_processing_time(len(nodes_to_enrich))
                logger.info(f"Estimated node processing time: {estimated_time:.1f} minutes")
                
                # Process nodes in batches
                print(f"Processing {len(nodes_to_enrich)} nodes...")
                batch_results = self.batch_processor.process_nodes(nodes_to_enrich)
                
                # Update statistics
                self.pipeline_stats.update(batch_results)
            else:
                logger.info("No nodes selected for enrichment")
            
            # Phase 2: Edge Enrichment (if enabled)
            if include_edges:
                logger.info("Phase 2: Edge enrichment")
                print("Enriching semantic edges...")
                edge_results = self.edge_enricher.enrich_semantic_edges()
                
                # Add edge stats to pipeline stats
                self.pipeline_stats["total_edges"] = edge_results.get("total_edges", 0)
                self.pipeline_stats["semantic_edges"] = edge_results.get("semantic_edges", 0)
                self.pipeline_stats["edges_enriched"] = edge_results.get("successfully_enriched", 0)
                self.pipeline_stats["edges_failed"] = edge_results.get("failed", 0)
                
                logger.info(f"Enriched {edge_results.get('successfully_enriched', 0)} out of {edge_results.get('semantic_edges', 0)} semantic edges")
            else:
                logger.info("Skipping edge enrichment")
            
            return self._create_summary()
            
        except Exception as e:
            logger.error(f"Enrichment pipeline failed: {e}")
            raise
        finally:
            self.pipeline_stats["end_time"] = datetime.utcnow()
            self._log_summary()
    
    def validate_configuration(self) -> Dict[str, Any]:
        """
        Validate that the enrichment pipeline is properly configured.
        
        Returns:
            Configuration validation results
        """
        validation = {
            "provider": self.provider,
            "model": self.model or "default",
            "api_key_present": bool(getattr(self.llm_client, 'api_key', None)),
            "graph_client_connected": self._validate_graph_connection(),
            "estimated_cost_per_node": self._estimate_cost_per_node()
        }
        
        validation["ready"] = all([
            validation["api_key_present"],
            validation["graph_client_connected"]
        ])
        
        return validation
    
    def _select_nodes_for_enrichment(self, node_types: Optional[List[str]] = None) -> List[str]:
        """
        Select nodes that should be enriched with LLM summaries.
        
        Args:
            node_types: Optional list of node types to include
            
        Returns:
            List of node IDs to enrich
        """
        if node_types is None:
            node_types = ["operation", "pipeline"]
        
        enrichable_nodes = []
        
        try:
            # Get all nodes from the graph
            all_nodes = self.graph_client.get_all_nodes()
            
            for node in all_nodes:
                # Convert node to dict format
                node_dict = node.to_dict()
                node_type = node_dict.get("node_type")
                
                # Filter by node type
                if node_type in node_types:
                    enrichable_nodes.append(node_dict.get("id"))
                    
        except Exception as e:
            logger.error(f"Error selecting nodes for enrichment: {e}")
            
        return enrichable_nodes
    
    def _estimate_processing_time(self, node_count: int) -> float:
        """
        Estimate processing time based on node count and batch size.
        
        Args:
            node_count: Number of nodes to process
            
        Returns:
            Estimated time in minutes
        """
        # Estimate ~1.5 seconds per node (includes API call + processing)
        seconds_per_node = 1.5
        
        # Account for batch processing efficiency
        batch_efficiency = 0.7 if self.batch_size > 1 else 1.0
        
        total_seconds = (node_count * seconds_per_node * batch_efficiency)
        return total_seconds / 60.0
    
    def _validate_api_key_format(self) -> bool:
        """Validate that the API key has the correct format."""
        api_key = getattr(self.llm_client, 'api_key', None)
        if not api_key:
            return False
        
        # Different providers have different key formats
        if self.provider == "openai":
            return api_key.startswith('sk-')
        elif self.provider == "openrouter":
            return len(api_key) > 10  # Basic length check
        else:
            return True  # Generic validation
    
    def _validate_graph_connection(self) -> bool:
        """Validate that we can connect to the graph."""
        try:
            node_count = self.graph_client.get_node_count()
            return node_count >= 0
        except:
            return False
    
    def _estimate_cost_per_node(self) -> float:
        """Estimate the cost per node for the selected model."""
        # Rough estimates for OpenAI models (as of 2024)
        cost_per_1k_tokens = {
            "gpt-4o-mini": 0.00015,  # $0.15 per 1M tokens
            "gpt-4": 0.03,
            "gpt-3.5-turbo": 0.0015
        }
        
        # Estimate ~500 tokens per node (prompt + response)
        tokens_per_node = 500
        model_cost = cost_per_1k_tokens.get(self.model, 0.001)
        
        return (tokens_per_node / 1000) * model_cost
    
    def _create_summary(self) -> Dict[str, Any]:
        """Create a summary of the enrichment process."""
        duration = None
        if self.pipeline_stats["start_time"] and self.pipeline_stats["end_time"]:
            duration = (self.pipeline_stats["end_time"] - self.pipeline_stats["start_time"]).total_seconds()
        
        # Get final stats from components
        enrichment_stats = self.node_enricher.get_enrichment_stats()
        
        # Calculate edge success rate
        total_edges = self.pipeline_stats.get("semantic_edges", 0)
        edges_enriched = self.pipeline_stats.get("edges_enriched", 0)
        edge_success_rate = (edges_enriched / max(total_edges, 1)) * 100 if total_edges > 0 else 0
        
        return {
            "status": "completed",
            "pipeline_config": {
                "provider": "openai",
                "model": self.model,
                "batch_size": self.batch_size
            },
            "processing": {
                "total_nodes": self.pipeline_stats["total_nodes"],
                "processed": self.pipeline_stats["processed"],
                "successful": self.pipeline_stats["successful"],
                "failed": self.pipeline_stats["failed"],
                "success_rate": (self.pipeline_stats["successful"] / max(self.pipeline_stats["processed"], 1)) * 100
            },
            "nodes": {
                "total_nodes": self.pipeline_stats["total_nodes"],
                "successfully_enriched": enrichment_stats["successfully_enriched"],
                "failed": enrichment_stats["failed"],
                "skipped": enrichment_stats["skipped"],
                "success_rate": (enrichment_stats["successfully_enriched"] / max(self.pipeline_stats["total_nodes"], 1)) * 100
            },
            "edges": {
                "total_edges": self.pipeline_stats.get("total_edges", 0),
                "semantic_edges": self.pipeline_stats.get("semantic_edges", 0),
                "successfully_enriched": self.pipeline_stats.get("edges_enriched", 0),
                "failed": self.pipeline_stats.get("edges_failed", 0),
                "success_rate": edge_success_rate
            },
            "enrichment": enrichment_stats
        }
    
    def _log_summary(self):
        """Log a summary of the enrichment process."""
        summary = self._create_summary()
        logger.info(f"Enrichment pipeline completed: {summary}")
    
    def check_api_key(self) -> Dict[str, Any]:
        """
        Quick check to validate API key without making expensive calls.
        
        Returns:
            Validation results including key presence and format
        """
        api_key = os.getenv('OPENAI_API_KEY')
        
        if not api_key:
            return {
                "status": "failed",
                "reason": "Missing API key",
                "message": "No OPENAI_API_KEY found in environment"
            }
        
        if not api_key.startswith('sk-'):
            return {
                "status": "failed", 
                "reason": "Invalid API key format",
                "message": "API key should start with 'sk-'"
            }
        
        return {
            "status": "ready",
            "provider": "openai",
            "model": self.model,
            "message": "LLM client configured successfully"
        }