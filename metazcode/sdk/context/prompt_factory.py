"""
Technology-Agnostic Prompt Factory for ETL Platform LLM Summaries

This module creates context-aware prompt templates that work with any ETL/Data Pipeline technology:
- SSIS (Microsoft SQL Server Integration Services)
- Informatica PowerCenter/Cloud
- Talend Open Studio/Enterprise
- Apache Airflow
- AWS Glue, Azure Data Factory, Google Dataflow
- Custom ETL solutions

Key Features:
- Technology-neutral language and concepts
- Generic node types (pipelines, operations, transformations)
- Universal ETL patterns recognition
- Business-focused summaries for any platform
- Migration-ready metadata for cross-platform projects

This is part of the optional LLM enrichment layer.
"""

from typing import Dict, List, Any, Optional, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from .summarizer import PipelineContext


@dataclass
class OperationContext:
    """Structured representation of operation context for prompt generation."""

    operation_name: str
    operation_type: str
    pipeline_name: str
    source_connections: List[str]
    destination_connections: List[str]
    transformation_summary: str
    business_domain: str = "data processing"


@dataclass
class PipelineContext:
    """Structured representation of pipeline context for prompt generation."""
    
    pipeline_name: str
    operation_count: int
    source_tables: List[str]
    destination_tables: List[str]
    operations: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.operations is None:
            self.operations = []


class PromptFactory:
    """
    Factory for generating context-aware prompts for LLM operation summaries.

    This class creates business-focused prompts that help LLMs understand the
    purpose and context of SSIS operations rather than just their technical details.
    """

    def __init__(self):
        """Initialize the prompt factory with predefined templates."""
        self.templates = {
            "business_summary": self._get_business_summary_template(),
            "technical_summary": self._get_technical_summary_template(),
            "context_analysis": self._get_context_analysis_template(),
            "edge_summary": self._get_edge_summary_template(),
        }

    def _get_business_summary_template(self) -> str:
        """
        Get the comprehensive AI Data Architect prompt template for operation enrichment.
        
        This template implements a structured persona-based approach with clear rules
        and specific instructions for different node types.
        """
        return """You are an expert AI Data Architect. Your specialized task is to analyze individual nodes from an ETL/Data Pipeline knowledge graph and generate a concise, human-readable `llm_summary` for each one.

**Your Goal:** The `llm_summary` must translate the technical metadata into a clear statement of **business purpose and intent**. This works across all ETL platforms (SSIS, Informatica, Talend, Airflow, AWS Glue, etc.). The primary audience is downstream AI migration agents and human data engineers who need to quickly understand the role of each component in the overall data flow.

**Core Principles:**
1. **Source of Truth:** You will derive your summary ONLY from the information provided. Do not invent or infer information that isn't supported by the provided data.
2. **Acknowledge Missing Information:** If critical information (like SQL queries, transformation logic) is missing or null, explicitly state that (e.g., "The specific transformation logic for this operation was not captured.").
3. **Translate, Don't Just Repeat:** Do not simply restate property names. Synthesize values into meaningful narrative. For example, instead of "name is dbo.Fact_SalesOrder," write "This operation loads sales order metrics into the data warehouse."
4. **Technology-Neutral Language:** Use universal ETL concepts rather than platform-specific terms (e.g., "data transformation" instead of "SSIS Data Flow" or "Informatica Mapping").
5. **Style Guide:**
   - Use **active voice** (e.g., "This operation loads..." not "Data is loaded by this operation...").
   - Be **concise**. Aim for 1-3 sentences.
   - **Identify Universal Patterns:** Recognize common data integration patterns across platforms (e.g., "staging area," "fact table load," "dimension processing," "initial load," "incremental update," "data quality check").

**Analysis Context:**
Operation: {operation_name}
Operation Type: {operation_subtype}  
Pipeline/Workflow: {pipeline_name}
Data Sources: {sources}
Data Destinations: {destinations}
Transformations: {transformation_summary}

**Instructions for Operation Nodes:**
Describe the specific action this operation performs and its place in the data transformation journey. Look for:
- The human-given name as a strong business indicator (e.g., "Load Customer Dimension," "Extract Sales Data," "Cleanse Address Data")
- Operation patterns common across ETL platforms (extract, transform, load, validate, merge, aggregate)
- Data flow intent (staging, enrichment, integration, archival)
- Business process support (reporting, analytics, operational systems)

**Universal ETL Patterns to Recognize:**
- **Extract**: Reading from source systems (databases, files, APIs, queues)
- **Transform**: Data cleansing, validation, enrichment, aggregation, formatting
- **Load**: Writing to destinations (data warehouses, data marts, operational systems)
- **Control Flow**: Orchestration, scheduling, error handling, notifications
- **Data Quality**: Validation, profiling, cleansing, standardization

Generate a concise llm_summary that explains what this operation accomplishes in business terms:"""

    def _get_technical_summary_template(self) -> str:
        """
        Get a technical summary template for detailed analysis.
        """
        return """Analyze this ETL operation and provide a technical summary of its data processing approach:

Operation: {operation_name} (Type: {operation_type})
Pipeline/Workflow: {pipeline_name}
Data Flow: {sources} → {transformation_summary} → {destinations}

Provide a brief technical summary focusing on:
- Data transformation approach and algorithms
- Integration pattern used (batch, real-time, micro-batch)
- Platform-agnostic design considerations
- Performance and scalability factors

Technical Summary:"""

    def _get_context_analysis_template(self) -> str:
        """
        Get a context analysis template for understanding operation relationships.
        """
        return """Analyze the context and relationships of this ETL operation:

Operation: {operation_name}
Pipeline/Workflow Context: {pipeline_name}
Upstream Systems: {sources}
Downstream Systems: {destinations}
Processing: {transformation_summary}

Analyze:
- How this operation fits in the overall data integration architecture
- Dependencies and relationships with other systems and processes
- Business impact if this operation fails
- Cross-platform migration considerations

Context Analysis:"""

    def create_business_prompt(self, context: OperationContext) -> str:
        """
        Create a business-focused prompt for operation summary.

        Args:
            context: Structured operation context

        Returns:
            Formatted prompt ready for LLM
        """
        return self.templates["business_summary"].format(
            operation_name=context.operation_name,
            operation_subtype=context.operation_type,
            pipeline_name=context.pipeline_name,
            sources=self._format_connections(context.source_connections),
            destinations=self._format_connections(context.destination_connections),
            transformation_summary=context.transformation_summary
            or "No transformations",
        )

    def create_technical_prompt(self, context: OperationContext) -> str:
        """
        Create a technical-focused prompt for operation analysis.

        Args:
            context: Structured operation context

        Returns:
            Formatted prompt ready for LLM
        """
        return self.templates["technical_summary"].format(
            operation_name=context.operation_name,
            operation_type=context.operation_type,
            pipeline_name=context.pipeline_name,
            sources=self._format_connections(context.source_connections),
            destinations=self._format_connections(context.destination_connections),
            transformation_summary=context.transformation_summary
            or "No transformations",
        )

    def create_context_prompt(self, context: OperationContext) -> str:
        """
        Create a context analysis prompt for understanding operation relationships.

        Args:
            context: Structured operation context

        Returns:
            Formatted prompt ready for LLM
        """
        return self.templates["context_analysis"].format(
            operation_name=context.operation_name,
            pipeline_name=context.pipeline_name,
            sources=self._format_connections(context.source_connections),
            destinations=self._format_connections(context.destination_connections),
            transformation_summary=context.transformation_summary
            or "No transformations",
        )

    def create_context_aware_prompt(
        self, context: OperationContext, context_hints: Dict[str, str]
    ) -> str:
        """
        Create a context-aware business prompt with additional context clues.

        Args:
            context: Structured operation context
            context_hints: Additional context information (e.g., custom domain, business patterns)

        Returns:
            Enhanced business prompt with contextual information
        """
        base_prompt = self.create_business_prompt(context)

        # Add pattern-aware context (optional)
        pattern_context = self._build_pattern_context(context, context_hints)
        if pattern_context:
            enhanced_prompt = (
                base_prompt
                + f"\n\nContext: {pattern_context}\n\nBusiness Summary:"
            )
            return enhanced_prompt

        return base_prompt

    def _format_connections(self, connections: List[str]) -> str:
        """
        Format connection lists for readable prompt inclusion.

        Args:
            connections: List of connection names

        Returns:
            Formatted string representation
        """
        if not connections:
            return "None"

        if len(connections) == 1:
            return connections[0]

        if len(connections) <= 3:
            return ", ".join(connections)

        # For many connections, show first few and count
        return f"{', '.join(connections[:3])} and {len(connections) - 3} others"

    def _build_pattern_context(
        self, context: OperationContext, pattern_hints: Dict[str, str]
    ) -> str:
        """
        Build pattern-aware context hints for enhanced prompts.

        Args:
            context: Operation context
            pattern_hints: Data pipeline pattern hints and custom context

        Returns:
            Pattern context string
        """
        context_parts = []

        # Optionally detect data patterns from context (only if no custom patterns provided)
        if not pattern_hints.get("detected_patterns"):
            detected_patterns = self._detect_data_patterns(context)
            if detected_patterns:
                context_parts.append(f"Data patterns: {', '.join(detected_patterns)}")

        # Add custom pattern hints (allows user-provided domain context)
        for key, value in pattern_hints.items():
            if value and key != "detected_patterns":
                context_parts.append(f"{key}: {value}")

        return "; ".join(context_parts)

    def _detect_data_patterns(self, context: OperationContext) -> List[str]:
        """
        Detect likely data processing patterns from context clues.

        Args:
            context: Operation context to analyze

        Returns:
            List of detected data pipeline pattern keywords
        """
        patterns = []

        # Combine all text for analysis
        all_text = " ".join(
            [
                context.operation_name.lower(),
                context.pipeline_name.lower(),
                " ".join(context.source_connections).lower(),
                " ".join(context.destination_connections).lower(),
                context.transformation_summary.lower(),
            ]
        )

        # Universal data pipeline pattern keyword mapping
        pattern_keywords = {
            "extraction": ["extract", "read", "pull", "fetch", "source", "ingest", "collect"],
            "transformation": ["transform", "convert", "aggregate", "calculate", "derive", "process", "enrich"],
            "loading": ["load", "write", "insert", "update", "merge", "push", "sink"],
            "validation": ["validate", "check", "verify", "quality", "cleanse", "profile", "audit"],
            "orchestration": ["schedule", "trigger", "workflow", "dependency", "sequence", "control", "manage"],
            "integration": ["integrate", "combine", "join", "merge", "consolidate", "unify", "blend"],
            "distribution": ["distribute", "replicate", "broadcast", "publish", "stream", "route"],
            "archival": ["archive", "backup", "history", "snapshot", "retention", "preserve"],
            "staging": ["staging", "temp", "intermediate", "buffer", "cache", "temporary"],
            "warehouse": ["warehouse", "dwh", "dimension", "fact", "mart", "cube", "analytics"],
            "operational": ["ods", "operational", "transactional", "real-time", "live", "current"],
            "monitoring": ["monitor", "log", "audit", "track", "observe", "measure", "alert"],
        }

        # Check for data pipeline patterns
        for pattern, keywords in pattern_keywords.items():
            if any(keyword in all_text for keyword in keywords):
                patterns.append(pattern)

        return patterns

    def test_prompt_variations(self, context: OperationContext) -> Dict[str, str]:
        """
        Generate multiple prompt variations for testing and comparison.

        Args:
            context: Operation context to test with

        Returns:
            Dictionary of prompt variations for testing
        """
        variations = {
            "business_standard": self.create_business_prompt(context),
            "technical_detailed": self.create_technical_prompt(context),
            "context_relationship": self.create_context_prompt(context),
        }

        # Add pattern-aware variation if patterns detected
        detected_patterns = self._detect_data_patterns(context)
        if detected_patterns:
            pattern_hints = {"detected_patterns": ", ".join(detected_patterns)}
            variations["business_pattern_aware"] = self.create_context_aware_prompt(
                context, pattern_hints
            )

        return variations

    def create_pipeline_business_prompt(
        self, pipeline_context: "PipelineContext"
    ) -> str:
        """
        Create a structured AI Data Architect prompt for pipeline summary.

        Args:
            pipeline_context: Structured pipeline context

        Returns:
            Formatted prompt ready for LLM
        """
        template = """You are an expert AI Data Architect. Your specialized task is to analyze individual pipeline/workflow nodes from an ETL/Data Integration knowledge graph and generate a concise, human-readable `llm_summary`.

**Your Goal:** Translate the technical metadata into a clear statement of **business purpose and intent**. This works across all ETL platforms (SSIS, Informatica, Talend, Airflow, AWS Glue, etc.). The primary audience is downstream AI migration agents and human data engineers.

**Core Principles:**
1. **Source of Truth:** Derive your summary ONLY from the provided information.
2. **Acknowledge Missing Information:** If critical details are missing, explicitly state that.
3. **Translate, Don't Repeat:** Synthesize into meaningful narrative, don't just restate properties.
4. **Technology-Neutral Language:** Use universal data integration concepts rather than platform-specific terms.
5. **Style Guide:**
   - Use **active voice** (e.g., "This pipeline processes..." not "Data is processed...")
   - Be **concise**. Aim for 1-3 sentences.
   - **Identify Universal Patterns:** Recognize common data integration patterns across platforms (initial load, incremental update, staging, dimension processing, real-time streaming).

**Pipeline/Workflow Analysis Context:**
Pipeline/Workflow: {pipeline_name}
Operations: {operation_count} data processing steps
Data Sources: {sources}
Data Destinations: {destinations}

**Instructions for Pipeline/Workflow Nodes:**
Explain the overall business process this pipeline/workflow accomplishes and its role in the data integration architecture. Look for:
- Name indicators of processing patterns ("initial," "incremental," "daily," "realtime," "batch")
- Business domains from data sources/destinations (Sales, Customer, Finance, Product, etc.)
- Data integration intent (consolidation, distribution, synchronization, archival)
- Overall data journey from sources through transformations to destinations

**Universal Data Integration Patterns to Recognize:**
- **Batch Processing**: Scheduled, bulk data processing (nightly loads, weekly aggregations)
- **Real-time/Streaming**: Continuous data processing (change data capture, event streaming)
- **Initial Load**: Full data migration or first-time population
- **Incremental Update**: Processing only changed or new data
- **Data Consolidation**: Combining data from multiple sources
- **Data Distribution**: Replicating data to multiple destinations

Focus on what business process this pipeline supports and what value it provides to the organization.

Business Summary:"""

        return template.format(
            pipeline_name=pipeline_context.pipeline_name,
            operation_count=pipeline_context.operation_count,
            sources=self._format_connections(pipeline_context.source_tables),
            destinations=self._format_connections(pipeline_context.destination_tables),
        )

    def create_pipeline_context_aware_prompt(
        self, pipeline_context: "PipelineContext", context_hints: Dict[str, str]
    ) -> str:
        """
        Create a context-aware business prompt for pipeline with additional context clues.

        Args:
            pipeline_context: Structured pipeline context
            context_hints: Additional contextual information (patterns, custom domains)

        Returns:
            Enhanced business prompt with contextual information
        """
        base_prompt = self.create_pipeline_business_prompt(pipeline_context)

        # Add pattern-aware context
        pattern_context = self._build_pipeline_pattern_context(
            pipeline_context, context_hints
        )
        if pattern_context:
            enhanced_prompt = (
                base_prompt
                + f"\n\nPattern Context: {pattern_context}\n\nBusiness Summary:"
            )
            return enhanced_prompt

        return base_prompt

    def _detect_pipeline_patterns(
        self, pipeline_context: "PipelineContext"
    ) -> List[str]:
        """
        Detect data processing patterns from pipeline context for pattern-aware prompting.

        Args:
            pipeline_context: Structured pipeline context

        Returns:
            List of detected data pipeline pattern keywords
        """
        patterns = []

        # Check pipeline name
        pipeline_name_lower = pipeline_context.pipeline_name.lower()

        # Check tables
        all_tables = (
            pipeline_context.source_tables + pipeline_context.destination_tables
        )
        all_tables_str = " ".join(all_tables).lower()

        # Combine all text for analysis
        all_text = f"{pipeline_name_lower} {all_tables_str}"

        # Universal data pipeline pattern detection
        pattern_keywords = {
            "extraction": ["extract", "read", "pull", "fetch", "source", "ingest", "collect"],
            "transformation": ["transform", "convert", "aggregate", "calculate", "derive", "process", "enrich"],
            "loading": ["load", "write", "insert", "update", "merge", "push", "sink"],
            "validation": ["validate", "check", "verify", "quality", "cleanse", "profile", "audit"],
            "integration": ["integrate", "combine", "join", "merge", "consolidate", "unify", "blend"],
            "staging": ["staging", "stg", "temp", "ods", "intermediate", "buffer"],
            "warehouse": ["warehouse", "dwh", "fact", "dim", "mart", "cube", "analytics"],
            "reporting": ["report", "dashboard", "analytics", "bi", "kpi", "metrics"],
            "operational": ["operational", "transactional", "real-time", "live", "current"],
            "archival": ["archive", "backup", "history", "snapshot", "retention", "preserve"],
        }

        for pattern, keywords in pattern_keywords.items():
            if any(keyword in all_text for keyword in keywords):
                patterns.append(pattern)

        return patterns

    def _build_pipeline_pattern_context(
        self, pipeline_context: "PipelineContext", pattern_hints: Dict[str, str]
    ) -> str:
        """
        Build pattern-aware context string for pipeline prompting.

        Args:
            pipeline_context: Structured pipeline context
            pattern_hints: Data pipeline pattern hints

        Returns:
            Formatted pattern context string
        """
        detected_patterns = pattern_hints.get("detected_patterns", "")
        if not detected_patterns:
            detected_patterns = ", ".join(
                self._detect_pipeline_patterns(pipeline_context)
            )

        if not detected_patterns:
            return ""

        pattern_context_parts = [f"This appears to be a {detected_patterns} pipeline"]

        # Add generic pattern guidance based on detected patterns
        pattern_descriptions = {
            "extraction": "focusing on data extraction and ingestion from various sources",
            "transformation": "emphasizing data processing, cleansing, and enrichment operations", 
            "loading": "concentrating on data loading and persistence to target systems",
            "validation": "prioritizing data quality, validation, and integrity checks",
            "integration": "combining and consolidating data from multiple sources",
            "staging": "providing temporary data storage and intermediate processing",
            "warehouse": "building analytical and reporting data capabilities",
            "operational": "supporting real-time transactional data processing",
            "archival": "managing data retention, backup, and historical preservation",
            "monitoring": "tracking data flow, quality, and system performance"
        }

        # Add description for the most relevant pattern
        for pattern in ["warehouse", "integration", "transformation", "extraction", "loading"]:
            if pattern in detected_patterns:
                if pattern in pattern_descriptions:
                    pattern_context_parts.append(pattern_descriptions[pattern])
                break

        return ". ".join(pattern_context_parts) + "."

    def _get_edge_summary_template(self) -> str:
        """
        Get the edge summary template for relationship enrichment.
        
        Returns:
            Edge summary template string
        """
        return """You are an expert AI Data Architect. Your specialized task is to analyze relationships/edges from an ETL/Data Pipeline knowledge graph and generate a concise, human-readable `llm_summary` for each relationship.

**Your Goal:** Translate the technical relationship metadata into a clear statement of **data flow purpose and business logic**. This works across all ETL platforms (SSIS, Informatica, Talend, Airflow, AWS Glue, etc.). The primary audience is downstream AI migration agents and human data engineers.

**Core Principles:**
1. **Source of Truth:** Derive your summary ONLY from the provided relationship information.
2. **Acknowledge Missing Information:** If critical details are missing, explicitly state that.
3. **Focus on Data Flow:** Explain HOW and WHY data moves or transforms between these components.
4. **Technology-Neutral Language:** Use universal data integration concepts.
5. **Style Guide:**
   - Use **active voice** (e.g., "This relationship transfers..." not "Data is transferred...")
   - Be **concise**. Aim for 1-2 sentences.
   - **Identify Data Patterns:** Recognize data integration patterns (joins, filters, aggregations, lookups).

**Relationship Analysis Context:**
Relationship Type: {relation_type}
Source: {source_name} ({source_type})
Target: {target_name} ({target_type})
Properties: {properties}

**Instructions by Relationship Type:**
- **reads_from/writes_to**: Explain the data extraction/loading purpose and business context
- **joins**: Describe how data is combined and what business relationship is established
- **filters**: Explain the business rules or data quality logic being applied
- **transforms**: Describe the data transformation and its business purpose

**Business Logic Patterns to Recognize:**
- **Data Integration**: Combining data from multiple sources for complete business view
- **Data Quality**: Filtering or validating data to ensure business accuracy
- **Business Rules**: Implementing organizational policies through data transformations
- **Reporting/Analytics**: Aggregating or reshaping data for business intelligence

Generate a concise llm_summary that explains what this relationship accomplishes in business terms:"""

    def create_edge_summary_prompt(self, relation_type: str, context: Dict[str, Any]) -> str:
        """
        Create a business-focused prompt for edge/relationship enrichment.
        
        Args:
            relation_type: Type of relationship (reads_from, writes_to, etc.)
            context: Edge context including source/target nodes and properties
            
        Returns:
            Formatted prompt ready for LLM
        """
        # Format properties for display
        properties_str = self._format_edge_properties(context.get('properties', {}))
        
        return self.templates["edge_summary"].format(
            relation_type=relation_type,
            source_name=context.get('source_name', 'Unknown'),
            source_type=context.get('source_type', 'Unknown'),
            target_name=context.get('target_name', 'Unknown'),
            target_type=context.get('target_type', 'Unknown'),
            properties=properties_str
        )
    
    def _format_edge_properties(self, properties: Dict[str, Any]) -> str:
        """
        Format edge properties for readable prompt inclusion.
        
        Args:
            properties: Dictionary of edge properties
            
        Returns:
            Formatted string representation
        """
        if not properties:
            return "None"
        
        # Prioritize semantic properties
        semantic_props = []
        
        # Join conditions
        if properties.get('join_condition'):
            semantic_props.append(f"Join condition: {properties['join_condition']}")
        
        # SQL queries
        if properties.get('sql_query'):
            sql = properties['sql_query']
            if len(sql) > 100:
                sql = sql[:100] + "..."
            semantic_props.append(f"SQL: {sql}")
        
        # Filter conditions  
        if properties.get('filter_condition'):
            semantic_props.append(f"Filter: {properties['filter_condition']}")
        
        # Transformation logic
        if properties.get('transformation_logic'):
            semantic_props.append(f"Transform: {properties['transformation_logic']}")
        
        # Relationship description
        if properties.get('relationship'):
            semantic_props.append(f"Relationship: {properties['relationship']}")
        
        # Field mappings if available
        if properties.get('source_fields') and properties.get('target_fields'):
            source_fields = ', '.join(properties['source_fields'][:3])
            target_fields = ', '.join(properties['target_fields'][:3])
            semantic_props.append(f"Fields: {source_fields} → {target_fields}")
        
        if semantic_props:
            return '; '.join(semantic_props)
        
        # Fallback to generic properties
        generic_props = []
        for key, value in properties.items():
            if key not in ['llm_summary', 'llm_enriched_at', 'llm_model', 'source_context']:
                if isinstance(value, str) and len(value) > 50:
                    value = value[:50] + "..."
                generic_props.append(f"{key}: {value}")
        
        return '; '.join(generic_props[:3]) if generic_props else "No semantic properties"
