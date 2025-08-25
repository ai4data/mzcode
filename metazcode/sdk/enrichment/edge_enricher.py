"""
Technology-Agnostic Edge Enricher for LLM-based Edge Summary Generation

This module handles the enrichment of graph edges from any ETL/Data Pipeline
platform with LLM-generated summaries. It focuses on semantic edges that contain
business logic such as joins, filters, transformations, and data flows.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

from metazcode.sdk.graph.graph_client_interface import GraphClientInterface
from metazcode.sdk.context.prompt_factory import PromptFactory
from .llm_client import OpenAIEnricher

logger = logging.getLogger(__name__)


class EdgeEnricher:
    """
    Enriches graph edges with LLM-generated summaries for any ETL platform.
    
    This class focuses on semantic edges that contain business logic:
    - Join conditions and relationships
    - Filter expressions and data quality rules
    - Transformation logic and aggregations
    - Data flow patterns
    """
    
    def __init__(
        self, 
        graph_client: GraphClientInterface, 
        llm_client: OpenAIEnricher,
        skip_enriched: bool = True
    ):
        """
        Initialize the edge enricher.
        
        Args:
            graph_client: Interface to the graph database
            llm_client: LLM client for generating summaries
            skip_enriched: Whether to skip already enriched edges
        """
        self.graph_client = graph_client
        self.llm_client = llm_client
        self.prompt_factory = PromptFactory()
        self.skip_enriched = skip_enriched
        
        # Track enrichment statistics
        self.stats = {
            "total_edges": 0,
            "semantic_edges": 0,
            "successfully_enriched": 0,
            "failed": 0,
            "skipped": 0
        }
    
    def enrich_semantic_edges(self) -> Dict[str, Any]:
        """
        Enrich all semantic edges in the graph with LLM summaries.
        
        Returns:
            Summary statistics of the enrichment process
        """
        try:
            # Use graph client interface instead of direct graph access
            all_edges = self.graph_client.get_all_edges()
            semantic_edges = self._identify_semantic_edges_from_list(all_edges)
            
            self.stats["total_edges"] = len(all_edges)
            self.stats["semantic_edges"] = len(semantic_edges)
            
            logger.info(f"Found {len(semantic_edges)} semantic edges to enrich out of {len(all_edges)} total edges")
            
            for edge_id, source, target in semantic_edges:
                self.enrich_edge_by_id(edge_id, source, target)
                
            logger.info(f"Edge enrichment complete: {self.stats}")
            return self.stats
            
        except Exception as e:
            logger.error(f"Error during edge enrichment: {e}")
            return self.stats
    
    def enrich_edge(self, source_id: str, target_id: str, edge_key: int = 0) -> bool:
        """
        Enrich a single edge with an LLM-generated summary.
        
        Args:
            source_id: Source node ID
            target_id: Target node ID  
            edge_key: Edge key (for multigraphs)
            
        Returns:
            True if successfully enriched, False otherwise
        """
        try:
            graph = self.graph_client.get_graph()
            # Handle both MultiGraph and regular Graph
            if hasattr(graph, 'is_multigraph') and graph.is_multigraph():
                edge_data = graph.get_edge_data(source_id, target_id, key=edge_key, default={})
            else:
                edge_data = graph.get_edge_data(source_id, target_id, default={})
            
            if not edge_data:
                logger.warning(f"Edge {source_id} -> {target_id} not found")
                return False
            
            # Check if already enriched
            if self.skip_enriched and edge_data.get("llm_summary"):
                logger.debug(f"Edge {source_id} -> {target_id} already enriched, skipping")
                self.stats["skipped"] += 1
                return True
            
            # Generate summary based on edge type
            summary = self._generate_edge_summary(source_id, target_id, edge_data)
            
            if summary:
                self._update_edge_with_summary(source_id, target_id, edge_key, summary)
                self.stats["successfully_enriched"] += 1
                return True
            else:
                self.stats["failed"] += 1
                return False
                
        except Exception as e:
            logger.error(f"Error enriching edge {source_id} -> {target_id}: {e}")
            self.stats["failed"] += 1
            return False
    
    def enrich_edge_by_id(self, edge_id: str, source_id: str, target_id: str) -> bool:
        """
        Enrich a single edge using graph client interface (backend-agnostic).
        
        Args:
            edge_id: Unique edge identifier
            source_id: Source node ID
            target_id: Target node ID
            
        Returns:
            True if successfully enriched, False otherwise
        """
        try:
            # Get all edges and find the one we want
            all_edges = self.graph_client.get_all_edges()
            target_edge = None
            
            for edge in all_edges:
                edge_dict = edge.to_dict()
                if (edge_dict.get("source_id") == source_id and 
                    edge_dict.get("target_id") == target_id):
                    target_edge = edge_dict
                    break
            
            if not target_edge:
                logger.warning(f"Edge {source_id} -> {target_id} not found")
                return False
            
            # Check if already enriched
            properties = target_edge.get("properties", {})
            if self.skip_enriched and properties.get("llm_summary"):
                logger.debug(f"Edge {source_id} -> {target_id} already enriched, skipping")
                self.stats["skipped"] += 1
                return True
            
            # Generate summary based on edge type
            summary = self._generate_edge_summary_from_dict(source_id, target_id, target_edge)
            
            if summary:
                self._update_edge_with_summary_dict(target_edge, summary)
                self.stats["successfully_enriched"] += 1
                return True
            else:
                self.stats["failed"] += 1
                return False
                
        except Exception as e:
            logger.error(f"Error enriching edge {source_id} -> {target_id}: {e}")
            self.stats["failed"] += 1
            return False
    
    def _identify_semantic_edges(self, graph) -> List[Tuple[str, str, int]]:
        """
        Identify edges that contain semantic information worth enriching.
        
        Args:
            graph: NetworkX graph object
            
        Returns:
            List of (source, target, key) tuples for semantic edges
        """
        semantic_edges = []
        
        # Handle both MultiGraph and Graph cases
        if hasattr(graph, 'is_multigraph') and graph.is_multigraph():
            edge_iter = graph.edges(data=True, keys=True)
        else:
            # For regular graphs, simulate key=0
            edge_iter = [(source, target, 0, data) for source, target, data in graph.edges(data=True)]
        
        for source, target, key, data in edge_iter:
            if self._is_semantic_edge(data):
                semantic_edges.append((source, target, key))
        
        return semantic_edges
    
    def _identify_semantic_edges_from_list(self, all_edges) -> List[Tuple[str, str, str]]:
        """
        Identify edges that contain semantic information worth enriching from edge list.
        
        Args:
            all_edges: List of Edge objects from graph client
            
        Returns:
            List of (edge_id, source, target) tuples for semantic edges
        """
        semantic_edges = []
        
        for edge in all_edges:
            edge_dict = edge.to_dict()
            edge_data = edge_dict.get("properties", {})
            
            if self._is_semantic_edge_dict(edge_dict):
                edge_id = f"{edge_dict.get('source_id')}_{edge_dict.get('target_id')}_{edge_dict.get('relation', 'unknown')}"
                semantic_edges.append((edge_id, edge_dict.get("source_id"), edge_dict.get("target_id")))
        
        return semantic_edges
    
    def _is_semantic_edge_dict(self, edge_dict: Dict[str, Any]) -> bool:
        """
        Determine if an edge contains semantic information worth enriching.
        
        Args:
            edge_dict: Edge dictionary from graph client
            
        Returns:
            True if edge is semantic, False otherwise
        """
        # Skip structural relationships
        relation = edge_dict.get("relation", "").lower()
        if relation in ["contains", "depends_on"]:
            return False
            
        # Focus on semantic relationships
        semantic_relations = [
            "reads_from", "writes_to", "joins", "filters", 
            "transforms", "aggregates", "validates", "routes"
        ]
        
        if relation in semantic_relations:
            return True
            
        # Check if edge has business logic (SQL, expressions, etc.)
        properties = edge_dict.get("properties", {})
        has_logic = any(key in properties for key in [
            "sql_query", "join_condition", "filter_expression", 
            "transformation", "validation_rule", "aggregation"
        ])
        
        return has_logic
    
    def _is_semantic_edge(self, edge_data: Dict[str, Any]) -> bool:
        """
        Determine if an edge contains semantic information worth enriching.
        
        Args:
            edge_data: Edge data dictionary
            
        Returns:
            True if edge is semantic, False otherwise
        """
        # Skip structural relationships
        relation = edge_data.get("relation", "").lower()
        if relation in ["contains", "depends_on"]:
            return False
        
        # Include data flow relationships
        if relation in ["reads_from", "writes_to"]:
            return True
            
        # Check for semantic properties in edge
        properties = edge_data.get("properties", {})
        
        # Edges with SQL or transformation logic
        semantic_indicators = [
            "join_condition", "filter_condition", "sql_query", 
            "transformation_logic", "aggregate_function",
            "where_clause", "having_clause"
        ]
        
        for indicator in semantic_indicators:
            if properties.get(indicator):
                return True
        
        # Edges with business context
        if properties.get("relationship") and "business" in str(properties.get("relationship")).lower():
            return True
            
        return False
    
    def _generate_edge_summary(self, source_id: str, target_id: str, edge_data: Dict[str, Any]) -> Optional[str]:
        """
        Generate an LLM summary for an edge.
        
        Args:
            source_id: Source node ID
            target_id: Target node ID
            edge_data: Edge data dictionary
            
        Returns:
            Generated summary or None
        """
        try:
            # Get source and target node information
            source_node = self.graph_client.get_node(source_id)
            target_node = self.graph_client.get_node(target_id)
            
            if not source_node or not target_node:
                logger.warning(f"Could not get node data for edge {source_id} -> {target_id}")
                return None
            
            # Build context for the edge
            context = self._build_edge_context(source_node, target_node, edge_data)
            
            # Generate summary using LLM
            relation = edge_data.get("relation", "unknown")
            summary = self.llm_client.enrich_edge(relation, context)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary for edge {source_id} -> {target_id}: {e}")
            return None
    
    def _build_edge_context(self, source_node: Dict[str, Any], target_node: Dict[str, Any], edge_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build context information for edge enrichment.
        
        Args:
            source_node: Source node data
            target_node: Target node data
            edge_data: Edge data
            
        Returns:
            Context dictionary
        """
        source_attrs = source_node.get("attributes", {})
        target_attrs = target_node.get("attributes", {})
        properties = edge_data.get("properties", {})
        
        context = {
            "relation_type": edge_data.get("relation", "unknown"),
            "source_name": source_attrs.get("name", "unknown"),
            "source_type": source_attrs.get("node_type", "unknown"),
            "target_name": target_attrs.get("name", "unknown"), 
            "target_type": target_attrs.get("node_type", "unknown"),
            "properties": properties
        }
        
        # Add specific semantic information
        if properties.get("join_condition"):
            context["join_condition"] = properties["join_condition"]
        
        if properties.get("sql_query"):
            context["sql_query"] = properties["sql_query"]
            
        if properties.get("transformation_logic"):
            context["transformation_logic"] = properties["transformation_logic"]
        
        # Add field information if available
        if source_attrs.get("fields"):
            context["source_fields"] = [f["name"] for f in source_attrs["fields"][:5]]  # Limit to first 5
            
        if target_attrs.get("fields"):
            context["target_fields"] = [f["name"] for f in target_attrs["fields"][:5]]  # Limit to first 5
        
        return context
    
    def _update_edge_with_summary(self, source_id: str, target_id: str, edge_key: int, summary: str):
        """
        Update edge with LLM summary.
        
        Args:
            source_id: Source node ID
            target_id: Target node ID
            edge_key: Edge key
            summary: Generated summary text
        """
        enrichment_properties = {
            "llm_summary": summary,
            "llm_enriched_at": datetime.utcnow().isoformat(),
            "llm_model": self.llm_client.model
        }
        
        try:
            graph = self.graph_client.get_graph()
            
            if hasattr(graph, 'edges'):
                # NetworkX graph - update edge attributes
                if hasattr(graph, 'is_multigraph') and graph.is_multigraph():
                    if graph.has_edge(source_id, target_id, key=edge_key):
                        edge_data = graph.get_edge_data(source_id, target_id, key=edge_key)
                        if edge_data:
                            for key, value in enrichment_properties.items():
                                edge_data[key] = value
                            logger.debug(f"Updated edge {source_id} -> {target_id} with LLM summary")
                    else:
                        logger.warning(f"Edge {source_id} -> {target_id} not found for updating")
                else:
                    # Regular graph
                    if graph.has_edge(source_id, target_id):
                        edge_data = graph.get_edge_data(source_id, target_id)
                        if edge_data:
                            for key, value in enrichment_properties.items():
                                edge_data[key] = value
                            logger.debug(f"Updated edge {source_id} -> {target_id} with LLM summary")
                    else:
                        logger.warning(f"Edge {source_id} -> {target_id} not found for updating")
            else:
                # For other graph backends, implement as needed
                logger.warning(f"Graph client doesn't support edge updates for {source_id} -> {target_id}")
                
        except Exception as e:
            logger.error(f"Failed to update edge {source_id} -> {target_id}: {e}")
    
    def get_enrichment_stats(self) -> Dict[str, Any]:
        """Get edge enrichment statistics."""
        return self.stats.copy()
    
    def _generate_edge_summary_from_dict(self, source_id: str, target_id: str, edge_dict: Dict[str, Any]) -> Optional[str]:
        """
        Generate edge summary from edge dictionary (backend-agnostic).
        
        Args:
            source_id: Source node ID
            target_id: Target node ID
            edge_dict: Edge dictionary from graph client
            
        Returns:
            Generated summary or None if failed
        """
        try:
            relation_type = edge_dict.get("relation", "unknown")
            properties = edge_dict.get("properties", {})
            
            # Build context for LLM
            context = {
                "source_node": source_id,
                "target_node": target_id,
                "relation_type": relation_type,
                "edge_properties": properties
            }
            
            # Get source and target node information
            source_node = self.graph_client.get_node(source_id)
            target_node = self.graph_client.get_node(target_id)
            
            if source_node:
                context["source_details"] = source_node.get("attributes", {})
            if target_node:
                context["target_details"] = target_node.get("attributes", {})
            
            # Use LLM client to generate summary
            summary = self.llm_client.enrich_edge(relation_type, context)
            
            if summary:
                logger.debug(f"Generated edge summary for {source_id} -> {target_id}: {summary[:100]}...")
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating edge summary for {source_id} -> {target_id}: {e}")
            return None
    
    def _update_edge_with_summary_dict(self, edge_dict: Dict[str, Any], summary: str):
        """
        Update edge with LLM summary using backend-agnostic approach.
        
        Args:
            edge_dict: Edge dictionary
            summary: Generated summary text
        """
        try:
            enrichment_properties = {
                "llm_summary": summary,
                "llm_enriched_at": datetime.utcnow().isoformat(),
                "llm_model": self.llm_client.model
            }
            
            # For now, we'll try to update via the graph client interface
            # This may need to be enhanced based on specific backend capabilities
            
            source_id = edge_dict.get("source_id")
            target_id = edge_dict.get("target_id")
            
            if source_id and target_id:
                # Try to update using existing NetworkX method as fallback
                try:
                    graph = self.graph_client.get_graph()
                    if hasattr(graph, 'edges') and hasattr(graph, 'get_edge_data'):
                        edge_data = graph.get_edge_data(source_id, target_id)
                        if edge_data:
                            for key, value in enrichment_properties.items():
                                edge_data[key] = value
                            logger.debug(f"Updated edge {source_id} -> {target_id} with LLM summary")
                        else:
                            logger.warning(f"Edge data not found for {source_id} -> {target_id}")
                    else:
                        logger.debug(f"Backend doesn't support direct edge updates for {source_id} -> {target_id}")
                except Exception as e:
                    logger.warning(f"Could not update edge {source_id} -> {target_id}: {e}")
            else:
                logger.warning("Edge dictionary missing source or target IDs")
                
        except Exception as e:
            logger.error(f"Failed to update edge with summary: {e}")