"""
Azure OpenAI LLM Client for ETL Platform Node Enrichment

This module provides an Azure OpenAI-specific implementation of the LLM client interface
for generating business-focused summaries from any ETL/Data Pipeline platform.
"""

import logging
from typing import Optional, Dict, Any
import openai
from openai import AzureOpenAI

from metazcode.sdk.context.prompt_factory import PromptFactory, OperationContext, PipelineContext
from .base_llm_client import BaseLLMClient

logger = logging.getLogger(__name__)


class AzureOpenAIEnricher(BaseLLMClient):
    """
    Azure OpenAI-based enricher for generating business summaries from any ETL platform.

    This class handles all interactions with the Azure OpenAI API using technology-neutral
    prompts that work with SSIS, Informatica, Talend, Airflow, and other data
    integration platforms. It focuses on business purpose rather than technical details.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        azure_endpoint: Optional[str] = None,
        api_version: str = "2024-12-01-preview",
        deployment: Optional[str] = None
    ):
        """
        Initialize the Azure OpenAI enricher.

        Args:
            api_key: Azure OpenAI API key (subscription key)
            model: Model name for reference (default: gpt-4o-mini)
            azure_endpoint: Azure OpenAI endpoint URL (e.g., https://your-resource.openai.azure.com/)
            api_version: API version to use (default: 2024-12-01-preview)
            deployment: Deployment name in Azure (defaults to model name if not specified)
        """
        # Determine the actual deployment name
        actual_deployment = deployment or model

        # Initialize base class with deployment name as the model identifier
        super().__init__(api_key, actual_deployment)

        if not azure_endpoint:
            raise ValueError("azure_endpoint is required for Azure OpenAI. Set AZURE_OPENAI_ENDPOINT environment variable or pass azure_endpoint parameter.")

        self.azure_endpoint = azure_endpoint
        self.api_version = api_version
        self.deployment = actual_deployment

        self.client = AzureOpenAI(
            api_key=api_key,
            api_version=api_version,
            azure_endpoint=azure_endpoint
        )
        self.prompt_factory = PromptFactory()

        logger.info(f"Initialized Azure OpenAI client with endpoint: {azure_endpoint}, deployment: {self.deployment}")

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

            # Call Azure OpenAI
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

            # Call Azure OpenAI
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
        Make a call to the Azure OpenAI API.

        Args:
            prompt: The prompt to send to the model

        Returns:
            Generated text or None if failed
        """
        try:
            self.stats["total_calls"] += 1

            # Build request parameters
            request_params = {
                "model": self.deployment,  # Use deployment name for Azure
                "messages": [
                    {"role": "system", "content": "You are an expert AI Data Architect analyzing ETL and Data Pipeline metadata from any technology platform."},
                    {"role": "user", "content": prompt}
                ],
                "n": 1
            }

            # Newer models (gpt-5, o1, o3) don't support temperature or max_tokens
            # They use max_completion_tokens and default temperature only
            is_reasoning_model = any(x in self.deployment.lower() for x in ["gpt-5", "o1-", "o3-"])
            is_new_model = any(x in self.deployment.lower() for x in ["gpt-4o"])

            if is_reasoning_model:
                # Reasoning models: no temperature, use max_completion_tokens
                request_params["max_completion_tokens"] = 150
            elif is_new_model:
                # GPT-4o models: support temperature, use max_completion_tokens
                request_params["temperature"] = 0.7
                request_params["max_completion_tokens"] = 150
            else:
                # Older models (gpt-35-turbo, gpt-4): support temperature, use max_tokens
                request_params["temperature"] = 0.7
                request_params["max_tokens"] = 150

            response = self.client.chat.completions.create(**request_params)

            # Extract the generated text
            summary = response.choices[0].message.content.strip()

            # Update statistics
            self.stats["successful_calls"] += 1
            if hasattr(response, 'usage'):
                self.stats["total_tokens"] += response.usage.total_tokens

            return summary

        except openai.APIError as e:
            logger.error(f"Azure OpenAI API error: {e}")
            self.stats["failed_calls"] += 1
            return None
        except Exception as e:
            logger.error(f"Unexpected error calling Azure OpenAI: {e}")
            self.stats["failed_calls"] += 1
            return None

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

            # Call Azure OpenAI
            summary = self._call_llm(prompt)

            if summary:
                logger.debug(f"Generated edge summary for {relation_type}: {summary[:100]}...")

            return summary

        except Exception as e:
            logger.error(f"Error enriching edge {relation_type}: {e}")
            self.stats["failed_calls"] += 1
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Get usage statistics."""
        stats = self.stats.copy()
        stats["azure_endpoint"] = self.azure_endpoint
        stats["deployment"] = self.deployment
        stats["api_version"] = self.api_version
        return stats

    def estimate_cost(self) -> float:
        """
        Estimate the cost based on token usage.

        Note: Azure OpenAI pricing varies by region and agreement.
        This provides a rough estimate based on standard pricing.

        Returns:
            Estimated cost in USD
        """
        # Azure OpenAI pricing (approximate, varies by region/agreement)
        cost_per_1k_tokens = 0.00015  # Similar to OpenAI gpt-4o-mini

        if "gpt-4" in self.model.lower() and "mini" not in self.model.lower():
            cost_per_1k_tokens = 0.03
        elif "gpt-35" in self.model.lower() or "gpt-3.5" in self.model.lower():
            cost_per_1k_tokens = 0.0015

        total_cost = (self.stats["total_tokens"] / 1000) * cost_per_1k_tokens
        return round(total_cost, 4)
