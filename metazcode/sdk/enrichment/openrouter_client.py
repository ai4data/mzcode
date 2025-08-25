"""
OpenRouter LLM Client for ETL Platform Node Enrichment

This module provides an OpenRouter-specific implementation of the LLM client interface
for generating business-focused summaries from any ETL/Data Pipeline platform.
OpenRouter provides access to multiple LLM providers through a unified API.
"""

import logging
from typing import Optional, Dict, Any
from openai import OpenAI

from metazcode.sdk.context.prompt_factory import PromptFactory, OperationContext, PipelineContext
from .base_llm_client import BaseLLMClient

logger = logging.getLogger(__name__)


class OpenRouterEnricher(BaseLLMClient):
    """
    OpenRouter-based enricher for generating business summaries from any ETL platform.
    
    OpenRouter provides access to multiple LLM models (OpenAI, Anthropic, Meta, Google, etc.)
    through a single API, offering more model choices and competitive pricing.
    """
    
    def __init__(self, api_key: str, model: str, site_url: Optional[str] = None, site_name: Optional[str] = None):
        """
        Initialize the OpenRouter enricher.
        
        Args:
            api_key: OpenRouter API key
            model: Model to use (e.g., "deepseek/deepseek-chat", "anthropic/claude-3.5-sonnet")
            site_url: Optional site URL for rankings on openrouter.ai
            site_name: Optional site name for rankings on openrouter.ai
        """
        super().__init__(api_key, model)
        
        # Configure OpenAI client to use OpenRouter endpoint
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key
        )
        
        # Optional headers for OpenRouter rankings
        self.extra_headers = {}
        if site_url:
            self.extra_headers["HTTP-Referer"] = site_url
        if site_name:
            self.extra_headers["X-Title"] = site_name
            
        self.prompt_factory = PromptFactory()
    
    def enrich_operation(self, operation_name: str, context: Dict[str, Any]) -> Optional[str]:
        """
        Generate a business summary for an operation node.
        
        Args:
            operation_name: Name of the operation
            context: Operation context including sources, destinations, etc.
            
        Returns:
            Generated business summary or None if failed
        """
        try:
            # Build structured context
            operation_context = OperationContext(
                operation_name=operation_name,
                operation_type=context.get("operation_type", "Unknown"),
                pipeline_name=context.get("pipeline_name", "Unknown"),
                source_connections=context.get("sources", []),
                destination_connections=context.get("destinations", []),
                transformation_summary=context.get("transformation_summary", "")
            )
            
            # Generate prompt
            prompt = self.prompt_factory.create_business_prompt(operation_context)
            
            # Call OpenRouter
            summary = self._call_llm(prompt)
            
            if summary:
                logger.debug(f"Generated summary for {operation_name}: {summary[:100]}...")
                
            return summary
            
        except Exception as e:
            logger.error(f"Error enriching operation {operation_name}: {e}")
            self.stats["failed_calls"] += 1
            return None
    
    def enrich_pipeline(self, pipeline_name: str, context: Dict[str, Any]) -> Optional[str]:
        """
        Generate a business summary for a pipeline node.
        
        Args:
            pipeline_name: Name of the pipeline
            context: Pipeline context including operations, tables, etc.
            
        Returns:
            Generated business summary or None if failed
        """
        try:
            # Build structured context
            pipeline_context = PipelineContext(
                pipeline_name=pipeline_name,
                operation_count=context.get("operation_count", 0),
                source_tables=context.get("source_tables", []),
                destination_tables=context.get("destination_tables", []),
                operations=context.get("operations", [])
            )
            
            # Generate prompt
            prompt = self.prompt_factory.create_pipeline_business_prompt(pipeline_context)
            
            # Call OpenRouter
            summary = self._call_llm(prompt)
            
            if summary:
                logger.debug(f"Generated summary for pipeline {pipeline_name}: {summary[:100]}...")
                
            return summary
            
        except Exception as e:
            logger.error(f"Error enriching pipeline {pipeline_name}: {e}")
            self.stats["failed_calls"] += 1
            return None
    
    def enrich_edge(self, relation_type: str, context: Dict[str, Any]) -> Optional[str]:
        """
        Generate a business summary for an edge/relationship.
        
        Args:
            relation_type: Type of relationship (reads_from, writes_to, etc.)
            context: Edge context including source/target nodes and properties
            
        Returns:
            Generated business summary or None if failed
        """
        try:
            # Generate prompt for edge enrichment
            prompt = self.prompt_factory.create_edge_summary_prompt(relation_type, context)
            
            # Call OpenRouter
            summary = self._call_llm(prompt)
            
            if summary:
                logger.debug(f"Generated edge summary for {relation_type}: {summary[:100]}...")
                
            return summary
            
        except Exception as e:
            logger.error(f"Error enriching edge {relation_type}: {e}")
            self.stats["failed_calls"] += 1
            return None
    
    def _call_llm(self, prompt: str) -> Optional[str]:
        """
        Make a call to the OpenRouter API.
        
        Args:
            prompt: The prompt to send to the model
            
        Returns:
            Generated text or None if failed
        """
        try:
            self.stats["total_calls"] += 1
            
            # Prepare request arguments
            request_args = {
                "model": self.model,
                "messages": [
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                "temperature": 0.1,
                "max_tokens": 500
            }
            
            # Add optional headers if provided
            if self.extra_headers:
                request_args["extra_headers"] = self.extra_headers
                request_args["extra_body"] = {}
            
            response = self.client.chat.completions.create(**request_args)
            
            if response.choices and len(response.choices) > 0:
                content = response.choices[0].message.content
                if content:
                    self.stats["successful_calls"] += 1
                    if hasattr(response, 'usage') and response.usage:
                        self.stats["total_tokens"] += response.usage.total_tokens
                    return content.strip()
            
            logger.warning("Empty response from OpenRouter")
            self.stats["failed_calls"] += 1
            return None
            
        except Exception as e:
            logger.error(f"OpenRouter API call failed: {e}")
            self.stats["failed_calls"] += 1
            return None