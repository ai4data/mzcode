# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development Setup
```bash
# Install dependencies (requires uv package manager)
uv sync

# For Memgraph database support (enterprise/large-scale analysis)
uv sync --extra memgraph

# Start Memgraph database (optional)
docker-compose up -d

# Quick setup validation
uv run python test_installation.py
./test.sh
```

### Main Commands
```bash
# Complete SSIS analysis (recommended workflow)
uv run python -m metazcode full --path <ssis_project_path>

# Complete SSIS analysis with LLM enrichment
uv run python -m metazcode full --path <ssis_project_path> --enable-llm

# Step-by-step analysis workflow
uv run python -m metazcode ingest --path <ssis_project_path>
uv run python -m metazcode analyze --path <ssis_project_path>

# LLM enrichment on existing graph
uv run python -m metazcode enrich --path <ssis_project_path>

# Export and visualization
uv run python -m metazcode dump --path <ssis_project_path> --output results.json
uv run python -m metazcode visualize --path <ssis_project_path>

# Combined ingestion and indexing
uv run python -m metazcode ingest-n-index --path <ssis_project_path>
```

### Testing and Development
```bash
# Run comprehensive validation script (primary test command)
./test.sh

# Test with sample data
uv run python -m metazcode full --path data/ssis/dataWH_ssis

# Install dev dependencies for code quality tools
uv sync --extra dev

# Run all tests (pytest in dev dependencies) 
uv run python -m pytest

# Test specific components with sample data
uv run python -m metazcode ingest --path data/ssis/dataWH_ssis
uv run python -m metazcode analyze --path data/ssis/dataWH_ssis

# Run a single test file
uv run python -m pytest tests/test_specific.py -v

# Run tests matching a pattern
uv run python -m pytest -k "test_sql_semantics" -v
```

### Code Quality
Currently no linting or formatting tools configured. Consider adding:
- `ruff` for linting and formatting
- `mypy` for type checking
- `pre-commit` for automated quality checks

### SQL Semantics and Analysis Scripts
```bash
# Test SQL semantics extraction
uv run python scripts/sql_semantics_parser_prototype.py

# Run impact analysis for migration planning
uv run python scripts/migration_impact_analysis.py --path <ssis_project_path>

# These scripts help ensure the graph output contains all necessary metadata
# for external migration tools to consume
```

### CLI Command Reference
- `full` / `complete`: Complete analysis pipeline (ingest + analyze + [enrich] + index)
- `ingest`: Extract business logic from SSIS packages
- `analyze`: Perform cross-package dependency analysis
- `enrich`: Add LLM-generated business summaries to nodes (requires API key)
- `visualize`: Generate graph visualization
- `dump`: Export graph data to JSON
- `ingest-n-index`: Run ingestion and indexing together

### Environment Configuration
```bash
# Copy example environment file
cp .env.example .env

# Key environment variables:
# METAZCODE_DB_BACKEND=networkx|memgraph (default: networkx)
# MEMGRAPH_HOST=localhost (for database backend)
# METAZCODE_ENABLE_CROSS_ANALYSIS=true
# METAZCODE_ENABLE_INDEXING=true
# METAZCODE_ENABLE_LLM_ENRICHMENT=true (optional LLM enrichment)
# OPENAI_API_KEY=your-api-key (for LLM enrichment)
```

### Memgraph Database Commands
```bash
# Start Memgraph database
docker-compose up -d

# Check Memgraph status
docker ps | grep memgraph

# View Memgraph logs
docker logs mzcode-memgraph

# Access Memgraph Lab (web interface)
# Visit http://localhost:3000 in browser

# Run analysis with Memgraph backend
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path <ssis_project_path>

# Test Memgraph connection
MEMGRAPH_USERNAME=admin MEMGRAPH_PASSWORD=admin uv run python -c "
from metazcode.sdk.graph.graph_constructor import GraphClientBuilder
from metazcode.sdk.models.config import DatabaseConfig
import os
os.environ['MEMGRAPH_USERNAME'] = 'admin'
os.environ['MEMGRAPH_PASSWORD'] = 'admin'
config = DatabaseConfig.from_environment()
config.backend = 'memgraph'
is_valid = GraphClientBuilder.validate_connection(config)
print(f'Connection valid: {is_valid}')
"

# Check database content after analysis
MEMGRAPH_USERNAME=admin MEMGRAPH_PASSWORD=admin uv run python -c "
from metazcode.sdk.models.config import DatabaseConfig
from metazcode.sdk.graph.client_memgraph import MemgraphClient
import os
os.environ['MEMGRAPH_USERNAME'] = 'admin'
os.environ['MEMGRAPH_PASSWORD'] = 'admin'
config = DatabaseConfig.from_environment()
config.backend = 'memgraph'
client = MemgraphClient(config)
print(f'Node count: {client.get_node_count()}')
print(f'Edge count: {client.get_edge_count()}')
client.close()
"
```

## Architecture

### Core Components

**SDK Structure (`metazcode/sdk/`)**:
- `ingestion/`: ETL package parsing and data extraction
- `graph/`: Graph construction and management (NetworkX-based)
- `analysis/`: Cross-package dependency analysis
- `enrichment/`: AI-powered business summary generation (LLM client, node enricher, batch processor)
- `context/`: Context collection and prompt engineering for LLM enrichment
- `models/`: Canonical data types and graph structures
- `indexing/`: Enhanced search capabilities and metadata indexing
- `integration/`: Coordination between components

**Key Classes**:
- `CanonicalSsisParser`: Parses SSIS .dtsx files using lxml
- `CrossPackageAnalyzer`: Analyzes dependencies between SSIS packages
- `GraphClientInterface`: Abstracts graph operations (NetworkX implementation)
- `Orchestrator`: Discovers and runs ingestion tools
- `EnrichmentPipeline`: Orchestrates AI-powered business summary generation
- `OpenAIEnricher`: LLM client for generating business summaries
- `NodeEnricher`: Enriches individual nodes with context-aware summaries
- `PromptFactory`: Technology-agnostic prompt engineering for AI Data Architect persona

### Data Flow

1. **Ingestion Phase**: 
   - `Orchestrator` discovers and runs `IngestionTool` subclasses
   - `CanonicalSsisParser` extracts business logic from .dtsx files
   - Creates canonical nodes (NodeType) and edges (EdgeType)

2. **Analysis Phase**:
   - `CrossPackageAnalyzer` identifies shared resources and dependencies
   - Adds cross-package edges to graph
   - Generates execution order and risk analysis

3. **Enrichment Phase** (Optional):
   - `EnrichmentPipeline` orchestrates AI-powered enhancement
   - `NodeEnricher` analyzes nodes using `ContextCollector` for business context
   - `OpenAIEnricher` generates business summaries via AI Data Architect prompts
   - Adds `llm_summary`, `llm_enriched_at`, and `llm_model` properties to nodes

4. **Indexing Phase**:
   - `IndexIntegration` creates searchable metadata index
   - Enhances graph with search capabilities including enriched summaries

### Node Types
- `PIPELINE`: SSIS packages
- `OPERATION`: Tasks within packages (SQL, Script, Data Flow)
  - SQL operations now include enhanced semantics (JOINs, aliases, column mappings)
- `DATA_ASSET`: Tables, files, datasets
- `CONNECTION`: Database connections
- `PARAMETER`: Package parameters
- `VARIABLE`: Package variables

### Edge Types
- `CONTAINS`: Parent-child relationships
- `READS_FROM` / `WRITES_TO`: Data flow
- `USES_CONNECTION` / `USES_PARAMETER`: Resource usage
- `DEPENDS_ON`: Cross-package dependencies
- `SHARES_RESOURCE`: Resource sharing

## Technology Stack

**Core Dependencies**:
- `lxml`: XML parsing for SSIS files
- `networkx`: Graph data structure and algorithms (default backend)
- `pymgclient` / `neo4j`: Memgraph database drivers (optional)
- `click`: CLI interface
- `pydantic`: Data validation and modeling
- `rank_bm25`: Search indexing
- `openai`: GPT-4o-mini integration for LLM enrichment (optional)
- `anthropic`: AI integration capabilities (optional)

**Database Backends**:
- **NetworkX**: In-memory graphs for small/medium projects
- **Memgraph**: Persistent graph database for large-scale enterprise analysis

**File Types Processed**:
- `.dtsx`: SSIS package files
- `.conmgr`: Connection manager files
- `.params`: Parameter files
- `.dtproj`: Project files

## Key Features

- **Offline Analysis**: Works without live database connections
- **Cross-Package Dependencies**: Identifies execution order and resource conflicts
- **Graph Output for Migration Tools**: Generates rich metadata graph that migration tools can consume
- **Search Capabilities**: Enhanced indexing for fast metadata search
- **Execution Order Analysis**: Provides dependency information for migration planning
- **SQL Semantics Extraction**: Deep parsing of SQL operations including JOINs, aliases, and column lineage
- **Enhanced Traceability**: Every element traced back to source file location
- **Migration-Ready Metadata**: Output includes all semantics needed by external migration tools
- **LLM Enrichment**: Optional AI-generated business summaries using GPT-4o-mini - technology-agnostic AI Data Architect approach

## Development Notes

### Working with the Codebase
- **Plugin-based architecture**: New ingestion tools extend `IngestionTool` base class in `metazcode/sdk/ingestion/`
- **Automatic tool discovery**: `Orchestrator` in `metazcode/cli/orchestrator.py` automatically discovers and runs all ingestion tools
- **Graph backend abstraction**: All graph operations go through `GraphClientInterface` (NetworkX or Memgraph backends)
- **File path requirements**: Must use absolute paths when calling API programmatically
- **Type mapping**: Multiple target platform support via `type_mapping.py` for data type conversions
- **JSON output**: Enhanced analysis results always saved as JSON for external tool integration
- **Database configuration**: Uses `DatabaseConfig` and `MetaZenseConfig` for environment-aware setup

### Adding New Ingestion Tools
To create a new ingestion tool for other platforms:

1. **Create new tool class**: Extend `IngestionTool` in `metazcode/sdk/ingestion/`
2. **Implement required methods**: 
   - `ingest()`: Generator yielding `(List[Node], List[Edge])` tuples
   - Use canonical node/edge types from `canonical_types.py`
3. **File discovery**: Use `discover_files(pattern)` method to find relevant files
4. **Auto-discovery**: Tool will be automatically found by `Orchestrator`

Example skeleton:
```python
from metazcode.sdk.ingestion.ingestion_tool import IngestionTool
from metazcode.sdk.models.canonical_types import NodeType, EdgeType
from metazcode.sdk.models.graph import Node, Edge

class MyPlatformTool(IngestionTool):
    def ingest(self):
        files = self.discover_files("*.myext")
        for file in files:
            # Parse file and create nodes/edges
            nodes = [Node(id="...", type=NodeType.PIPELINE, ...)]
            edges = [Edge(source="...", target="...", type=EdgeType.CONTAINS)]
            yield nodes, edges
```

### Key Integration Points
- `metazcode/cli/orchestrator.py`: Discovers and coordinates ingestion tools
- `metazcode/sdk/graph/graph_client_interface.py`: Backend abstraction layer
- `metazcode/sdk/models/canonical_types.py`: Core data model definitions
- `metazcode/sdk/integration/index_integration.py`: Search capability integration
- `metazcode/sdk/ingestion/ssis/sql_semantics.py`: SQL parsing and semantics extraction
- `metazcode/sdk/models/traceability.py`: Source context tracking

### Package Structure Understanding
- `cli/`: Command-line interface and orchestration
- `sdk/ingestion/`: SSIS file parsing and data extraction
  - `ssis/`: SSIS-specific parsers including SQL semantics extraction
- `sdk/graph/`: Graph backends (NetworkX in-memory, Memgraph persistent)
  - `analytics_ready_client.py`: Enhanced analytics-ready data structures
- `sdk/analysis/`: Cross-package dependency analysis algorithms
- `sdk/indexing/`: Search and metadata indexing
- `sdk/models/`: Canonical data types and configuration
  - `traceability.py`: Source context tracking for all elements

## Output Files

- `enhanced_graph_full_analysis.json`: Complete analysis with all metadata
- `enhanced_graph_with_dependencies.json`: Graph with cross-package dependencies only
- Custom output files when `--output` flag is specified
- Index files for search capabilities when `--index-output` is specified

## Development Troubleshooting

### Common Issues
- **"No .dtsx files found"**: Verify the path contains SSIS files with `ls <path>/*.dtsx`
- **"uv not found"**: Install uv package manager: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **Import/dependency errors**: Run `uv sync` to reinstall dependencies
- **Memgraph connection issues**: Ensure Docker container is running: `docker-compose up -d && docker ps | grep memgraph`
- **Permission errors**: Use absolute paths and ensure read access to SSIS project directories
- **Database connection timeouts**: Check if Memgraph is fully started with `docker logs mzcode-memgraph`
- **Empty analysis results**: Ensure SSIS project contains valid .dtsx files and check file permissions

### Debugging Commands
```bash
# Enable debug logging for detailed analysis output
METAZCODE_LOG_LEVEL=DEBUG uv run python -m metazcode full --path <ssis_project_path>

# Debug SQL semantics extraction
uv run python scripts/debug_sql_extraction.py --path <ssis_project_path>

# Validate traceability information
uv run python scripts/validate_traceability.py --path <ssis_project_path>

# Test SQL pattern recognition
uv run python scripts/test_sql_patterns.py

# Test specific SSIS files
find <path> -name "*.dtsx" -exec ls -la {} \;

# Validate environment variables
env | grep METAZCODE
env | grep MEMGRAPH

# Test database backends separately
# NetworkX (in-memory)
METAZCODE_DB_BACKEND=networkx uv run python -m metazcode ingest --path data/ssis/dataWH_ssis

# Memgraph (persistent)
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode ingest --path data/ssis/dataWH_ssis

# Check available sample data
ls -la data/ssis/*/

# Analyze SQL operations in a specific package
uv run python scripts/find_product_operations.py --package <package_name>

# Debug JOIN extraction issues
uv run python scripts/debug_join_issue.py --sql "SELECT * FROM A JOIN B ON A.id = B.id"
```

### Quick Validation
```bash
# Verify installation is working
./test.sh

# Test with sample data to validate functionality
uv run python -m metazcode full --path data/ssis/dataWH_ssis

# Validate specific components
uv run python -m metazcode ingest --path data/ssis/dataWH_ssis
uv run python -m metazcode analyze --path data/ssis/dataWH_ssis
```

## Sample Data Available

The repository includes extensive sample SSIS projects for testing:

- `data/ssis/dataWH_ssis/`: Basic data warehouse ETL (Q1.dtsx, Q2.dtsx, Q3.dtsx)
- `data/ssis/ssis_northwind/`: Northwind database ETL project  
- `data/ssis/ssis_advworks/`: Adventure Works ETL
- `data/ssis/ssis_DWH/`: Complete data warehouse project
- Additional sample projects for various ETL patterns

## Working with SQL Semantics

The enhanced SQL parser captures:
- **JOIN relationships**: All JOIN types with conditions and table aliases
- **Column transformations**: Aliases, expressions, and source tracking
- **Table references**: Including schema qualification and aliases
- **Query structure**: Complete metadata for migration tools to understand SQL logic

This semantic information is embedded in the graph output JSON, allowing external migration tools to:
- Understand data lineage and transformations
- Recreate equivalent logic in target platforms
- Maintain business logic integrity during migration

## LLM Enrichment Pipeline

The optional LLM enrichment pipeline adds AI-generated business summaries to ETL operations and pipelines using advanced language models. This technology-agnostic system works with any ETL platform and enhances graphs with human-readable business context.

### Quick Start
```bash
# Set your API key
export OPENAI_API_KEY=your-openai-api-key

# Run complete analysis with LLM enrichment
uv run python -m metazcode full --path <project_path> --enable-llm --output enriched_results.json
```

### Configuration
```bash
# Enable LLM enrichment via environment variables
export METAZCODE_ENABLE_LLM_ENRICHMENT=true
export OPENAI_API_KEY=your-openai-api-key

# Optional: Fine-tune LLM settings
export METAZCODE_LLM_PROVIDER=openai          # openai, anthropic, openrouter
export METAZCODE_LLM_MODEL=gpt-4o-mini        # Fast & cost-effective model
export METAZCODE_LLM_BATCH_SIZE=10            # Batch size for API efficiency
export METAZCODE_LLM_MAX_RETRIES=3            # Retry attempts for failed requests
export METAZCODE_LLM_TIMEOUT=30               # Request timeout in seconds

# Alternative: Use CLI flags
uv run python -m metazcode full --path <project> --enable-llm
```

### Usage Examples
```bash
# Complete ETL analysis with LLM enrichment (recommended)
uv run python -m metazcode full --path data/ssis/dataWH_ssis --enable-llm --output enhanced_results.json

# Standalone enrichment of existing graph
uv run python -m metazcode enrich --path data/ssis/dataWH_ssis --verbose

# Test with sample data
uv run python -m metazcode enrich --path data/ssis/dataWH_ssis

# Enable debug logging to see enrichment process
METAZCODE_LOG_LEVEL=DEBUG uv run python -m metazcode enrich --path data/ssis/dataWH_ssis
```

### AI Data Architect Approach

The LLM enrichment uses a structured **AI Data Architect** persona with comprehensive prompting:

#### Core Principles:
- **Source of Truth**: Summaries derived ONLY from provided metadata
- **Acknowledge Missing Info**: Explicitly states when data is unavailable
- **Technology-Agnostic**: Works with SSIS, Informatica, Talend, Airflow, etc.
- **Business-Focused**: Translates technical details into business purpose
- **Active Voice**: Professional, concise language (1-3 sentences)

#### Enrichment Process:
1. **Node Analysis**: Examines operation names, types, and transformation patterns
2. **Context Collection**: Gathers pipeline context, data sources/destinations
3. **Business Translation**: Converts technical metadata into business summaries
4. **Pattern Recognition**: Identifies ETL patterns (staging, dimension load, etc.)

### Example Enriched Output

**Before Enrichment:**
```json
{
  "id": "pipeline:Q2.dtsx:operation:Data Flow Task",
  "node_type": "operation", 
  "properties": {
    "name": "Data Flow Task",
    "operation_subtype": "DATA_FLOW"
  }
}
```

**After Enrichment:**
```json
{
  "id": "pipeline:Q2.dtsx:operation:Data Flow Task",
  "node_type": "operation",
  "properties": {
    "name": "Data Flow Task", 
    "operation_subtype": "DATA_FLOW",
    "llm_summary": "This operation transforms and consolidates employee data within the Q2.dtsx pipeline, facilitating change detection and historical tracking for HR analytics. It processes incoming employee records, compares them against existing data, and routes updates based on detected changes in city, email, or other attributes.",
    "llm_enriched_at": "2025-08-14T11:39:48.555373",
    "llm_model": "gpt-4o-mini"
  }
}
```

**Pipeline Example:**
```json
{
  "id": "pipeline:Q1.dtsx",
  "node_type": "pipeline",
  "properties": {
    "name": "Q1.dtsx",
    "llm_summary": "The Q1.dtsx pipeline transforms and consolidates data from various sources to support critical business insights across Sales, Human Resources, and Finance domains. By facilitating timely data integration, this pipeline enhances decision-making and operational efficiency, ensuring the organization stays responsive to evolving market needs.",
    "llm_enriched_at": "2025-08-14T11:39:48.360567",
    "llm_model": "gpt-4o-mini"
  }
}
```

### Performance & Cost Optimization

- **Batch Processing**: Processes multiple nodes simultaneously
- **Rate Limiting**: Configurable delays between API calls
- **Parallel Execution**: Multiple workers for improved throughput
- **Smart Retries**: Handles transient API failures gracefully
- **Cost-Effective Model**: GPT-4o-mini provides best price/performance ratio

```bash
# Typical performance metrics:
# - 7 nodes processed in ~15 seconds
# - 100% success rate with proper configuration
# - ~$0.01-0.05 per 100 operations (GPT-4o-mini pricing)
```

### Benefits for Migration Projects

1. **Human-Readable Documentation**: Every ETL component has business context
2. **AI Migration Agent Support**: Rich summaries help AI understand data flows
3. **Auto-Generated Documentation**: Business logic explanations for stakeholders
4. **Technology Transfer**: Eases understanding when migrating between platforms
5. **Knowledge Preservation**: Captures business intent that might be lost
6. **Migration Validation**: Helps verify that migrated logic serves same business purpose

### Troubleshooting LLM Enrichment

```bash
# Check API key configuration
env | grep OPENAI_API_KEY

# Test API connectivity
uv run python -c "
import openai
import os
client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
response = client.chat.completions.create(
    model='gpt-4o-mini',
    messages=[{'role': 'user', 'content': 'Hello, test connection'}],
    max_tokens=5
)
print('API connection successful!')
"

# Debug enrichment failures
METAZCODE_LOG_LEVEL=DEBUG uv run python -m metazcode enrich --path data/ssis/dataWH_ssis

# Check enrichment results
grep -A3 "llm_summary" enhanced_graph_full_analysis.json

# Validate configuration
uv run python -c "
from metazcode.sdk.models.config import MetaZenseConfig
config = MetaZenseConfig.from_environment()
print(f'LLM enabled: {getattr(config, \"enable_llm_enrichment\", False)}')
print(f'LLM provider: {getattr(config, \"llm_provider\", \"openai\")}')
"
```

### Integration with Migration Tools

The enriched graph output includes `llm_summary` properties that migration tools can leverage:

- **Business Context**: Understand what each operation accomplishes 
- **Intent Preservation**: Maintain business purpose during platform migration
- **Documentation Generation**: Auto-create migration documentation
- **Quality Assurance**: Verify migrated components serve same business needs
- **Stakeholder Communication**: Explain technical changes in business terms