"""
OpenAI LLM Client for ETL Platform Node Enrichment

This module provides an OpenAI-specific implementation of the LLM client interface
for generating business-focused summaries from any ETL/Data Pipeline platform.
"""

import logging
from typing import Optional, Dict, Any
import openai
from openai import OpenAI

from metazcode.sdk.context.prompt_factory import PromptFactory, OperationContext, PipelineContext
from .base_llm_client import BaseLLMClient

logger = logging.getLogger(__name__)


class OpenAIEnricher(BaseLLMClient):
    """
    OpenAI-based enricher for generating business summaries from any ETL platform.
    
    This class handles all interactions with the OpenAI API using technology-neutral
    prompts that work with SSIS, Informatica, Talend, Airflow, and other data
    integration platforms. It focuses on business purpose rather than technical details.
    """
    
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        """
        Initialize the OpenAI enricher.
        
        Args:
            api_key: OpenAI API key
            model: Model to use for generation (default: gpt-4o-mini)
        """
        super().__init__(api_key, model)
        self.client = OpenAI(api_key=api_key)
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
            
            # Call OpenAI
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
            
            # Call OpenAI
            summary = self._call_llm(prompt)
            
            if summary:
                logger.debug(f"Generated summary for pipeline {pipeline_name}: {summary[:100]}...")
                
            return summary
            
        except Exception as e:
            logger.error(f"Error enriching pipeline {pipeline_name}: {e}")
            self.stats["failed_calls"] += 1
            return None
    
    def _call_llm(self, prompt: str) -> Optional[str]:
        """
        Make a call to the OpenAI API.
        
        Args:
            prompt: The prompt to send to the model
            
        Returns:
            Generated text or None if failed
        """
        try:
            self.stats["total_calls"] += 1
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert AI Data Architect analyzing ETL and Data Pipeline metadata from any technology platform."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=150,
                n=1
            )
            
            # Extract the generated text
            summary = response.choices[0].message.content.strip()
            
            # Update statistics
            self.stats["successful_calls"] += 1
            if hasattr(response, 'usage'):
                self.stats["total_tokens"] += response.usage.total_tokens
            
            return summary
            
        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            self.stats["failed_calls"] += 1
            return None
        except Exception as e:
            logger.error(f"Unexpected error calling OpenAI: {e}")
            self.stats["failed_calls"] += 1
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get usage statistics."""
        return self.stats.copy()
    
    def estimate_cost(self) -> float:
        """
        Estimate the cost based on token usage.
        
        Returns:
            Estimated cost in USD
        """
        # GPT-4o-mini pricing (as of 2024)
        cost_per_1k_tokens = 0.00015  # $0.15 per 1M tokens
        
        if self.model == "gpt-4":
            cost_per_1k_tokens = 0.03
        elif self.model == "gpt-3.5-turbo":
            cost_per_1k_tokens = 0.0015
            
        total_cost = (self.stats["total_tokens"] / 1000) * cost_per_1k_tokens
        return round(total_cost, 4)
    
    def enrich_edge(self, relation_type: str, context: Dict[str, Any]) -> Optional[str]:
        """
        Generate a business summary for a graph edge/relationship.
        
        Args:
            relation_type: Type of relationship (reads_from, writes_to, joins, etc.)
            context: Edge context including source/target nodes and properties
            
        Returns:
            Generated business summary or None if failed
        """
        try:
            logger.debug(f"Enriching edge {relation_type}: {context.get('source_name')} -> {context.get('target_name')}")
            
            # Generate prompt using the edge template
            prompt = self.prompt_factory.create_edge_summary_prompt(relation_type, context)
            
            # Call OpenAI
            summary = self._call_llm(prompt)
            
            if summary:
                logger.debug(f"Generated edge summary for {relation_type}: {summary[:100]}...")
                
            return summary
            
        except Exception as e:
            logger.error(f"Error enriching edge {relation_type}: {e}")
            self.stats["failed_calls"] += 1
            return None