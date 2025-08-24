"""
Node Enricher for LLM-based Summary Generation

This module handles the enrichment of individual graph nodes with LLM-generated
business summaries, leveraging existing context collection and prompt generation.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from metazcode.sdk.graph.graph_client_interface import GraphClientInterface
from metazcode.sdk.models.canonical_types import NodeType
from metazcode.sdk.context.context_collector import ContextCollector
from metazcode.sdk.context.prompt_factory import OperationContext
from .llm_client import OpenAIEnricher

logger = logging.getLogger(__name__)


class NodeEnricher:
    """
    Enriches graph nodes with LLM-generated business summaries.
    
    This class coordinates between the graph, context collection, and LLM
    to add meaningful summaries to SSIS operations and pipelines.
    """
    
    def __init__(
        self, 
        graph_client: GraphClientInterface, 
        llm_client: OpenAIEnricher,
        skip_enriched: bool = True
    ):
        """
        Initialize the node enricher.
        
        Args:
            graph_client: Interface to the graph database
            llm_client: LLM client for generating summaries
            skip_enriched: Whether to skip already enriched nodes
        """
        self.graph_client = graph_client
        self.llm_client = llm_client
        self.context_collector = ContextCollector(graph_client)
        self.skip_enriched = skip_enriched
        
        # Track enrichment statistics
        self.stats = {
            "total_processed": 0,
            "successfully_enriched": 0,
            "failed": 0,
            "skipped": 0
        }
    
    def enrich_node(self, node_id: str) -> bool:
        """
        Enrich a single node with an LLM-generated summary.
        
        Args:
            node_id: ID of the node to enrich
            
        Returns:
            True if successfully enriched, False otherwise
        """
        try:
            # Get the node (returns Dict[str, Any])
            node_data = self.graph_client.get_node(node_id)
            if not node_data:
                logger.warning(f"Node {node_id} not found")
                return False
            
            # Get node attributes (this is where the actual properties are stored)
            attributes = node_data.get("attributes", {})
            
            # Check if already enriched
            if self.skip_enriched and attributes.get("llm_summary"):
                logger.debug(f"Node {node_id} already enriched, skipping")
                self.stats["skipped"] += 1
                return True
            
            # Generate summary based on node type
            summary = None
            node_type = attributes.get("node_type")
            if node_type == "operation":
                summary = self._enrich_operation_node(node_data)
            elif node_type == "pipeline":  
                summary = self._enrich_pipeline_node(node_data)
            else:
                logger.debug(f"Node type '{node_type}' not supported for enrichment. Node attributes: {list(attributes.keys())[:10]}")
                return False
            
            # Update node if summary generated
            if summary:
                self._update_node_with_summary(node_id, summary)
                self.stats["successfully_enriched"] += 1
                return True
            else:
                self.stats["failed"] += 1
                return False
                
        except Exception as e:
            logger.error(f"Error enriching node {node_id}: {e}")
            self.stats["failed"] += 1
            return False
        finally:
            self.stats["total_processed"] += 1
    
    def enrich_nodes(self, node_ids: List[str]) -> Dict[str, Any]:
        """
        Enrich multiple nodes.
        
        Args:
            node_ids: List of node IDs to enrich
            
        Returns:
            Summary statistics of the enrichment process
        """
        logger.info(f"Starting enrichment of {len(node_ids)} nodes")
        
        for node_id in node_ids:
            self.enrich_node(node_id)
        
        logger.info(f"Enrichment complete: {self.stats}")
        return self.stats
    
    def _enrich_operation_node(self, node_data: Dict[str, Any]) -> Optional[str]:
        """
        Generate summary for an operation node.
        
        Args:
            node_data: The operation node data dictionary
            
        Returns:
            Generated summary or None
        """
        try:
            node_id = node_data["id"]
            attributes = node_data.get("attributes", {})
            
            # Build simplified context directly from node attributes
            operation_context = OperationContext(
                operation_name=attributes.get("name", "Unknown Operation"),
                operation_type=attributes.get("operation_subtype", "Unknown"),
                pipeline_name=self._get_pipeline_name_from_id(node_id),
                source_connections=self._get_simple_sources(attributes),
                destination_connections=self._get_simple_destinations(attributes),
                transformation_summary=self._create_transformation_summary(node_data, {})
            )
            
            # Generate summary using LLM
            context = {
                "operation_type": operation_context.operation_type,
                "pipeline_name": operation_context.pipeline_name,
                "sources": operation_context.source_connections,
                "destinations": operation_context.destination_connections,
                "transformation_summary": operation_context.transformation_summary
            }
            
            summary = self.llm_client.enrich_operation(
                operation_context.operation_name,
                context
            )
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary for operation {node_data.get('id', 'unknown')}: {e}")
            return None
    
    def _enrich_pipeline_node(self, node_data: Dict[str, Any]) -> Optional[str]:
        """
        Generate summary for a pipeline node.
        
        Args:
            node_data: The pipeline node data dictionary
            
        Returns:
            Generated summary or None
        """
        try:
            attributes = node_data.get("attributes", {})
            pipeline_name = attributes.get("name", "Unknown Pipeline")
            
            # Get operations within this pipeline
            operations = self._get_pipeline_operations(node_data["id"])
            
            # Get source and destination tables
            sources = self._get_pipeline_sources(node_data["id"])
            destinations = self._get_pipeline_destinations(node_data["id"])
            
            # Build context
            context = {
                "operation_count": len(operations),
                "operations": operations,
                "source_tables": sources,
                "destination_tables": destinations
            }
            
            # Generate summary
            summary = self.llm_client.enrich_pipeline(pipeline_name, context)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary for pipeline {node_data.get('id', 'unknown')}: {e}")
            return None
    
    def _update_node_with_summary(self, node_id: str, summary: str):
        """
        Update node properties with LLM summary.
        
        Args:
            node_id: ID of the node to update
            summary: Generated summary text
        """
        enrichment_properties = {
            "llm_summary": summary,
            "llm_enriched_at": datetime.utcnow().isoformat(),
            "llm_model": self.llm_client.model
        }
        
        try:
            # Check if we're using Memgraph or NetworkX
            graph = self.graph_client.get_graph()
            
            # If it's a NetworkX graph, use NetworkX methods
            if hasattr(graph, 'has_node') and hasattr(graph, 'nodes'):
                if graph.has_node(node_id):
                    # Update the existing node attributes
                    for key, value in enrichment_properties.items():
                        graph.nodes[node_id][key] = value
                    logger.debug(f"Updated node {node_id} with LLM summary")
                else:
                    logger.warning(f"Node {node_id} not found in graph for updating")
            else:
                # For Memgraph, we need to use Cypher query to update
                # First check if the client has execute_query method
                if hasattr(self.graph_client, 'execute_query'):
                    query = """
                    MATCH (n {id: $node_id})
                    SET n.llm_summary = $summary,
                        n.llm_enriched_at = $enriched_at,
                        n.llm_model = $model
                    RETURN n
                    """
                    params = {
                        "node_id": node_id,
                        "summary": summary,
                        "enriched_at": enrichment_properties["llm_enriched_at"],
                        "model": enrichment_properties["llm_model"]
                    }
                    result = self.graph_client.execute_query(query, params)
                    logger.debug(f"Updated node {node_id} with LLM summary via Cypher")
                else:
                    logger.warning(f"Graph client doesn't support updates for node {node_id}")
        except Exception as e:
            logger.error(f"Failed to update node {node_id}: {e}")
    
    def _extract_connection_names(self, connections: List[Dict[str, Any]]) -> List[str]:
        """
        Extract connection names from connection details.
        
        Args:
            connections: List of connection dictionaries
            
        Returns:
            List of connection names
        """
        names = []
        for conn in connections:
            if isinstance(conn, dict):
                # Try different possible name fields
                name = conn.get("name") or conn.get("table_name") or conn.get("file_path")
                if name:
                    names.append(str(name))
        return names
    
    def _create_transformation_summary(self, node_data: Dict[str, Any], context: Dict[str, Any]) -> str:
        """
        Create a human-readable summary of the transformation.
        
        Args:
            node_data: The node data
            context: Additional context information
            
        Returns:
            Human-readable transformation summary
        """
        attributes = node_data.get("attributes", {})
        
        # Check for SQL operations
        sql = attributes.get("sql", "")
        if sql:
            # Extract key SQL operations
            sql_lower = sql.lower()
            operations = []
            
            if "join" in sql_lower:
                operations.append("joins data")
            if "group by" in sql_lower:
                operations.append("aggregates")
            if "sum(" in sql_lower or "count(" in sql_lower or "avg(" in sql_lower:
                operations.append("calculates metrics")
            if "where" in sql_lower:
                operations.append("filters")
            if "order by" in sql_lower:
                operations.append("sorts")
                
            if operations:
                return f"Operation that {', '.join(operations)}"
        
        # Check for operation type specific summaries
        op_type = attributes.get("operation_subtype", "").lower()
        if "data_flow" in op_type:
            return "Transforms and moves data between systems"
        elif "execute_sql" in op_type:
            return "Executes SQL operations"
        elif "file" in op_type:
            return "Processes file data"
        
        return "Performs data transformation"
    
    def _get_pipeline_operations(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """Get all operations within a pipeline."""
        operations = []
        # Simplified implementation - get from underlying graph directly
        try:
            graph = self.graph_client.get_graph()
            if hasattr(graph, 'successors'):
                for successor in graph.successors(pipeline_id):
                    node_data = self.graph_client.get_node(successor)
                    if node_data and node_data.get("attributes", {}).get("node_type") == "operation":
                        attributes = node_data.get("attributes", {})
                        operations.append({
                            "name": attributes.get("name", ""),
                            "type": attributes.get("operation_subtype", "")
                        })
        except Exception as e:
            logger.warning(f"Could not get operations for pipeline {pipeline_id}: {e}")
        
        return operations
    
    def _get_pipeline_sources(self, pipeline_id: str) -> List[str]:
        """Get all source tables/files for a pipeline."""
        sources = set()
        # This would traverse the graph to find all READ_FROM relationships
        # For now, return a simplified version
        return list(sources)
    
    def _get_pipeline_destinations(self, pipeline_id: str) -> List[str]:
        """Get all destination tables/files for a pipeline."""
        destinations = set()
        # This would traverse the graph to find all WRITES_TO relationships
        # For now, return a simplified version
        return list(destinations)
    
    def _get_pipeline_name_from_id(self, operation_id: str) -> str:
        """Extract pipeline name from operation ID."""
        # Operation IDs are typically like "pipeline:Q1.dtsx:operation:Data Flow Task"
        if "pipeline:" in operation_id:
            parts = operation_id.split(":")
            if len(parts) >= 2:
                return parts[1]  # e.g., "Q1.dtsx"
        return "Unknown Pipeline"
    
    def _get_simple_sources(self, attributes: Dict[str, Any]) -> List[str]:
        """Get simple source connections from attributes."""
        sources = []
        # Look for common source indicators in attributes
        sql = attributes.get("sql", "")
        if sql:
            # Simple SQL parsing for table names
            sql_lower = sql.lower()
            if " from " in sql_lower:
                # Extract table names after FROM (very basic)
                try:
                    from_part = sql_lower.split(" from ")[1].split(" where")[0].split(" group")[0].split(" order")[0]
                    table = from_part.strip().split()[0]
                    sources.append(table)
                except:
                    pass
        return sources
    
    def _get_simple_destinations(self, attributes: Dict[str, Any]) -> List[str]:
        """Get simple destination connections from attributes."""
        destinations = []
        # Look for common destination indicators
        sql = attributes.get("sql", "")
        if sql:
            sql_lower = sql.lower()
            if "insert into" in sql_lower:
                try:
                    table = sql_lower.split("insert into")[1].split("(")[0].strip().split()[0]
                    destinations.append(table)
                except:
                    pass
            elif "update " in sql_lower:
                try:
                    table = sql_lower.split("update")[1].split("set")[0].strip().split()[0]
                    destinations.append(table)
                except:
                    pass
        return destinations
    
    def get_enrichment_stats(self) -> Dict[str, Any]:
        """Get enrichment statistics."""
        return self.stats.copy()