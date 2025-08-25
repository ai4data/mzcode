"""
LLM Client Factory for Managing Multiple Providers

This module provides a factory for creating different LLM clients
(OpenAI, OpenRouter, Anthropic, etc.) with a unified interface.
"""

import os
import logging
from typing import Dict, Any, Optional, Type
from enum import Enum

from .base_llm_client import BaseLLMClient
from .llm_client import OpenAIEnricher
from .openrouter_client import OpenRouterEnricher

logger = logging.getLogger(__name__)


class LLMProvider(Enum):
    """Supported LLM providers."""
    OPENAI = "openai"
    OPENROUTER = "openrouter"


class LLMClientFactory:
    """
    Factory for creating LLM clients with different providers.
    
    Supports multiple LLM providers with automatic configuration
    from environment variables and explicit parameters.
    """
    
    # Registry of available providers
    _providers: Dict[LLMProvider, Type[BaseLLMClient]] = {
        LLMProvider.OPENAI: OpenAIEnricher,
        LLMProvider.OPENROUTER: OpenRouterEnricher,
    }
    
    # Default models for each provider
    _default_models = {
        LLMProvider.OPENAI: "gpt-4o-mini",
        LLMProvider.OPENROUTER: "deepseek/deepseek-chat",
    }
    
    # Environment variable mappings
    _env_keys = {
        LLMProvider.OPENAI: "OPENAI_API_KEY",
        LLMProvider.OPENROUTER: "OPENROUTER_API_KEY",
    }
    
    @classmethod
    def create_client(
        cls,
        provider: str,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        **kwargs
    ) -> BaseLLMClient:
        """
        Create an LLM client for the specified provider.
        
        Args:
            provider: Provider name (openai, openrouter)
            model: Model to use (optional, uses default if not specified)
            api_key: API key (optional, reads from environment if not specified)
            **kwargs: Additional provider-specific arguments
            
        Returns:
            Configured LLM client instance
            
        Raises:
            ValueError: If provider is not supported or configuration is invalid
        """
        # Validate provider
        try:
            provider_enum = LLMProvider(provider.lower())
        except ValueError:
            supported = [p.value for p in LLMProvider]
            raise ValueError(f"Unsupported provider '{provider}'. Supported providers: {supported}")
        
        # Get client class
        client_class = cls._providers[provider_enum]
        
        # Determine API key
        if not api_key:
            env_key = cls._env_keys[provider_enum]
            api_key = os.getenv(env_key)
            if not api_key:
                raise ValueError(f"No API key provided. Set {env_key} environment variable or pass api_key parameter.")
        
        # Determine model
        if not model:
            model = cls._default_models[provider_enum]
        
        # Create client with provider-specific arguments
        if provider_enum == LLMProvider.OPENAI:
            return client_class(api_key=api_key, model=model)
        
        elif provider_enum == LLMProvider.OPENROUTER:
            # OpenRouter supports additional configuration
            site_url = kwargs.get("site_url", os.getenv("OPENROUTER_SITE_URL"))
            site_name = kwargs.get("site_name", os.getenv("OPENROUTER_SITE_NAME", "MetaZCode"))
            return client_class(
                api_key=api_key,
                model=model,
                site_url=site_url,
                site_name=site_name
            )
        
        else:
            # Generic fallback
            return client_class(api_key=api_key, model=model)
    
    @classmethod
    def create_from_config(cls, config: Dict[str, Any]) -> BaseLLMClient:
        """
        Create an LLM client from a configuration dictionary.
        
        Args:
            config: Configuration dictionary with provider, model, api_key, etc.
            
        Returns:
            Configured LLM client instance
        """
        provider = config.get("provider", "openai")
        model = config.get("model")
        api_key = config.get("api_key")
        
        # Extract additional kwargs
        kwargs = {k: v for k, v in config.items() if k not in ["provider", "model", "api_key"]}
        
        return cls.create_client(
            provider=provider,
            model=model,
            api_key=api_key,
            **kwargs
        )
    
    @classmethod
    def get_supported_providers(cls) -> list[str]:
        """
        Get list of supported provider names.
        
        Returns:
            List of supported provider names
        """
        return [provider.value for provider in LLMProvider]
    
    @classmethod
    def get_default_model(cls, provider: str) -> str:
        """
        Get the default model for a provider.
        
        Args:
            provider: Provider name
            
        Returns:
            Default model name
        """
        try:
            provider_enum = LLMProvider(provider.lower())
            return cls._default_models[provider_enum]
        except ValueError:
            return "unknown"
    
    @classmethod
    def validate_configuration(cls, provider: str, api_key: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate configuration for a provider.
        
        Args:
            provider: Provider name to validate
            api_key: Optional API key to validate
            
        Returns:
            Validation results dictionary
        """
        result = {
            "provider": provider,
            "supported": False,
            "api_key_available": False,
            "default_model": "unknown",
            "env_var": None
        }
        
        try:
            provider_enum = LLMProvider(provider.lower())
            result["supported"] = True
            result["default_model"] = cls._default_models[provider_enum]
            result["env_var"] = cls._env_keys[provider_enum]
            
            # Check API key availability
            if api_key:
                result["api_key_available"] = True
            else:
                env_key = cls._env_keys[provider_enum]
                result["api_key_available"] = bool(os.getenv(env_key))
                
        except ValueError:
            pass
        
        return result


# Convenience functions for common use cases
def create_openai_client(model: str = "gpt-4o-mini", api_key: Optional[str] = None) -> OpenAIEnricher:
    """Create an OpenAI client with default settings."""
    return LLMClientFactory.create_client("openai", model=model, api_key=api_key)


def create_openrouter_client(
    model: str = "deepseek/deepseek-chat", 
    api_key: Optional[str] = None,
    site_url: Optional[str] = None,
    site_name: Optional[str] = None
) -> OpenRouterEnricher:
    """Create an OpenRouter client with default settings."""
    return LLMClientFactory.create_client(
        "openrouter", 
        model=model, 
        api_key=api_key,
        site_url=site_url,
        site_name=site_name
    )