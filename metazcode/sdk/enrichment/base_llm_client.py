"""
Abstract Base Class for LLM Clients

This module defines the interface that all LLM providers must implement
for ETL/Data Pipeline enrichment across any platform.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class BaseLLMClient(ABC):
    """
    Abstract base class for LLM clients used in data pipeline enrichment.
    
    All LLM providers (OpenAI, OpenRouter, Anthropic, etc.) must implement
    this interface to work with the enrichment pipeline.
    """
    
    def __init__(self, api_key: str, model: str):
        """
        Initialize the LLM client.
        
        Args:
            api_key: API key for the LLM provider
            model: Model identifier to use
        """
        self.api_key = api_key
        self.model = model
        
        # Track usage statistics
        self.stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "total_tokens": 0
        }
    
    @abstractmethod
    def enrich_operation(self, operation_name: str, context: Dict[str, Any]) -> Optional[str]:
        """
        Generate a business summary for an operation node.
        
        Args:
            operation_name: Name of the operation
            context: Operation context including sources, destinations, etc.
            
        Returns:
            Generated business summary or None if failed
        """
        pass
    
    @abstractmethod 
    def enrich_pipeline(self, pipeline_name: str, context: Dict[str, Any]) -> Optional[str]:
        """
        Generate a business summary for a pipeline node.
        
        Args:
            pipeline_name: Name of the pipeline
            context: Pipeline context including operations, tables, etc.
            
        Returns:
            Generated business summary or None if failed
        """
        pass
    
    @abstractmethod
    def enrich_edge(self, relation_type: str, context: Dict[str, Any]) -> Optional[str]:
        """
        Generate a business summary for an edge/relationship.
        
        Args:
            relation_type: Type of relationship (reads_from, writes_to, etc.)
            context: Edge context including source/target nodes and properties
            
        Returns:
            Generated business summary or None if failed
        """
        pass
    
    @abstractmethod
    def _call_llm(self, prompt: str) -> Optional[str]:
        """
        Make a call to the LLM API.
        
        Args:
            prompt: The prompt to send to the model
            
        Returns:
            Generated text or None if failed
        """
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get usage statistics for this client.
        
        Returns:
            Dictionary of usage statistics
        """
        return self.stats.copy()
    
    def reset_stats(self) -> None:
        """Reset usage statistics."""
        self.stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "total_tokens": 0
        }