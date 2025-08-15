"""Configuration models for MetaZenseCode."""

from typing import Optional, Literal
from pydantic import BaseModel, Field
import os


class DatabaseConfig(BaseModel):
    """Database configuration for graph storage."""
    
    backend: Literal["networkx", "memgraph"] = Field(
        default="networkx",
        description="Graph database backend to use"
    )
    
    # Memgraph connection settings
    host: str = Field(
        default="localhost",
        description="Memgraph server host"
    )
    
    port: int = Field(
        default=7687,
        description="Memgraph server port"
    )
    
    username: Optional[str] = Field(
        default=None,
        description="Memgraph username"
    )
    
    password: Optional[str] = Field(
        default=None,
        description="Memgraph password"
    )
    
    database: str = Field(
        default="memgraph",
        description="Database name"
    )
    
    connection_timeout: int = Field(
        default=30,
        description="Connection timeout in seconds"
    )
    
    @classmethod
    def from_environment(cls) -> "DatabaseConfig":
        """Create configuration from environment variables."""
        return cls(
            backend=os.getenv("METAZCODE_DB_BACKEND", "networkx"),
            host=os.getenv("MEMGRAPH_HOST", "localhost"),
            port=int(os.getenv("MEMGRAPH_PORT", "7687")),
            username=os.getenv("MEMGRAPH_USERNAME"),
            password=os.getenv("MEMGRAPH_PASSWORD"),
            database=os.getenv("MEMGRAPH_DATABASE", "memgraph"),
            connection_timeout=int(os.getenv("MEMGRAPH_TIMEOUT", "30"))
        )


class MetaZenseConfig(BaseModel):
    """Main configuration for MetaZenseCode."""
    
    database: DatabaseConfig = Field(
        default_factory=DatabaseConfig,
        description="Database configuration"
    )
    
    # Analysis settings
    enable_cross_package_analysis: bool = Field(
        default=True,
        description="Enable cross-package dependency analysis"
    )
    
    enable_enhanced_indexing: bool = Field(
        default=True,
        description="Enable enhanced SSIS indexing"
    )
    
    # Logging settings
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level"
    )
    
    # LLM Enrichment settings
    enable_llm_enrichment: bool = Field(
        default=False,
        description="Enable LLM enrichment for nodes"
    )
    
    llm_provider: Literal["openai", "anthropic", "openrouter"] = Field(
        default="openai",
        description="LLM provider to use"
    )
    
    llm_model: str = Field(
        default="gpt-4o-mini",
        description="LLM model to use for enrichment"
    )
    
    llm_api_key: Optional[str] = Field(
        default=None,
        description="API key for LLM provider (can also use environment variables)"
    )
    
    llm_batch_size: int = Field(
        default=10,
        description="Number of nodes to process in each batch"
    )
    
    llm_max_retries: int = Field(
        default=3,
        description="Maximum retries for failed LLM requests"
    )
    
    llm_timeout: int = Field(
        default=30,
        description="Timeout in seconds for LLM requests"
    )
    
    @classmethod
    def from_environment(cls) -> "MetaZenseConfig":
        """Create configuration from environment variables."""
        return cls(
            database=DatabaseConfig.from_environment(),
            enable_cross_package_analysis=os.getenv("METAZCODE_ENABLE_CROSS_ANALYSIS", "true").lower() == "true",
            enable_enhanced_indexing=os.getenv("METAZCODE_ENABLE_INDEXING", "true").lower() == "true",
            log_level=os.getenv("METAZCODE_LOG_LEVEL", "INFO"),
            enable_llm_enrichment=os.getenv("METAZCODE_ENABLE_LLM_ENRICHMENT", "false").lower() == "true",
            llm_provider=os.getenv("METAZCODE_LLM_PROVIDER", "openai"),
            llm_model=os.getenv("METAZCODE_LLM_MODEL", "gpt-4o-mini"),
            llm_api_key=os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY") or os.getenv("OPENROUTER_API_KEY"),
            llm_batch_size=int(os.getenv("METAZCODE_LLM_BATCH_SIZE", "10")),
            llm_max_retries=int(os.getenv("METAZCODE_LLM_MAX_RETRIES", "3")),
            llm_timeout=int(os.getenv("METAZCODE_LLM_TIMEOUT", "30"))
        )