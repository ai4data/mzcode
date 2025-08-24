"""
Enrichment Pipeline for LLM-based Node Summary Generation

This module orchestrates the complete enrichment process, managing
the flow from node selection through batch processing to final updates.
"""

import os
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from metazcode.sdk.graph.graph_client_interface import GraphClientInterface
from metazcode.sdk.models.canonical_types import NodeType
from .llm_client import OpenAIEnricher
from .node_enricher import NodeEnricher
from .batch_processor import BatchProcessor

logger = logging.getLogger(__name__)


class EnrichmentPipeline:
    """
    Orchestrates the LLM enrichment process for graph nodes.
    
    This pipeline manages the complete enrichment workflow including:
    - Node selection and filtering
    - Batch processing for efficiency
    - Progress tracking and reporting
    - Error handling and recovery
    """
    
    def __init__(
        self, 
        graph_client: GraphClientInterface,
        api_key: Optional[str] = None,
        model: str = "gpt-4o-mini",
        batch_size: int = 10
    ):
        """
        Initialize the enrichment pipeline.
        
        Args:
            graph_client: Interface to the graph database
            api_key: API key for the LLM provider (defaults to env var)
            model: LLM model to use for enrichment
            batch_size: Number of nodes to process in parallel
        """
        self.graph_client = graph_client
        self.model = model
        self.batch_size = batch_size
        
        # Initialize LLM client
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("No API key provided. Set OPENAI_API_KEY environment variable.")
            
        self.llm_client = OpenAIEnricher(api_key=self.api_key, model=model)
        
        # Initialize components
        self.node_enricher = NodeEnricher(graph_client, self.llm_client)
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
    
    def enrich_graph(self, node_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Enrich all eligible nodes in the graph with LLM summaries.
        
        Args:
            node_types: Optional list of node types to enrich (defaults to operation/pipeline)
            
        Returns:
            Summary of the enrichment process
        """
        self.pipeline_stats["start_time"] = datetime.utcnow()
        logger.info("Starting enrichment pipeline")
        
        try:
            # Select nodes for enrichment
            nodes_to_enrich = self._select_nodes_for_enrichment(node_types)
            self.pipeline_stats["total_nodes"] = len(nodes_to_enrich)
            
            if not nodes_to_enrich:
                logger.info("No nodes selected for enrichment")
                return self._create_summary()
            
            logger.info(f"Found {len(nodes_to_enrich)} nodes to enrich")
            
            # Estimate processing time
            estimated_time = self._estimate_processing_time(len(nodes_to_enrich))
            logger.info(f"Estimated processing time: {estimated_time:.1f} minutes")
            
            # Process nodes in batches
            print(f"Processing {len(nodes_to_enrich)} nodes...")
            batch_results = self.batch_processor.process_nodes(nodes_to_enrich)
            
            # Update statistics
            self.pipeline_stats.update(batch_results)
            
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
            "api_key_present": bool(self.api_key),
            "api_key_format": self._validate_api_key_format(),
            "model_supported": self.model in ["gpt-4o-mini", "gpt-4", "gpt-3.5-turbo"],
            "graph_client_connected": self._validate_graph_connection(),
            "estimated_cost_per_node": self._estimate_cost_per_node()
        }
        
        validation["ready"] = all([
            validation["api_key_present"],
            validation["api_key_format"],
            validation["model_supported"],
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
        if not self.api_key:
            return False
        
        # OpenAI keys start with 'sk-'
        return self.api_key.startswith('sk-')
    
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
            "enrichment": enrichment_stats,
            "summary": {
                "total_nodes": self.pipeline_stats["total_nodes"],
                "successfully_enriched": enrichment_stats["successfully_enriched"],
                "failed": enrichment_stats["failed"],
                "skipped": enrichment_stats["skipped"],
                "success_rate": (enrichment_stats["successfully_enriched"] / max(self.pipeline_stats["total_nodes"], 1)) * 100
            }
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