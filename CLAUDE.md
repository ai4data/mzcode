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

# Step-by-step analysis workflow
uv run python -m metazcode ingest --path <ssis_project_path>
uv run python -m metazcode analyze --path <ssis_project_path>

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
```

### Code Quality
Currently no linting or formatting tools configured. Consider adding:
- `ruff` for linting and formatting
- `mypy` for type checking
- `pre-commit` for automated quality checks

### CLI Command Reference
- `full` / `complete`: Complete analysis pipeline (ingest + analyze + index)
- `ingest`: Extract business logic from SSIS packages
- `analyze`: Perform cross-package dependency analysis  
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
- `ingestion/`: SSIS package parsing and data extraction
- `graph/`: Graph construction and management (NetworkX-based)
- `analysis/`: Cross-package dependency analysis
- `models/`: Canonical data types and graph structures
- `indexing/`: Enhanced search capabilities and metadata indexing
- `integration/`: Coordination between components

**Key Classes**:
- `CanonicalSsisParser`: Parses SSIS .dtsx files using lxml
- `CrossPackageAnalyzer`: Analyzes dependencies between SSIS packages
- `GraphClientInterface`: Abstracts graph operations (NetworkX implementation)
- `Orchestrator`: Discovers and runs ingestion tools

### Data Flow

1. **Ingestion Phase**: 
   - `Orchestrator` discovers and runs `IngestionTool` subclasses
   - `CanonicalSsisParser` extracts business logic from .dtsx files
   - Creates canonical nodes (NodeType) and edges (EdgeType)

2. **Analysis Phase**:
   - `CrossPackageAnalyzer` identifies shared resources and dependencies
   - Adds cross-package edges to graph
   - Generates execution order and risk analysis

3. **Indexing Phase**:
   - `IndexIntegration` creates searchable metadata index
   - Enhances graph with search capabilities

### Node Types
- `PIPELINE`: SSIS packages
- `OPERATION`: Tasks within packages (SQL, Script, Data Flow)
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
- `anthropic`: AI integration capabilities

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
- **AI-Ready Output**: Generates rich metadata for AI migration planning
- **Search Capabilities**: Enhanced indexing for fast metadata search
- **Migration Planning**: Execution order analysis and risk assessment

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

### Package Structure Understanding
- `cli/`: Command-line interface and orchestration
- `sdk/ingestion/`: SSIS file parsing and data extraction
- `sdk/graph/`: Graph backends (NetworkX in-memory, Memgraph persistent)
- `sdk/analysis/`: Cross-package dependency analysis algorithms
- `sdk/indexing/`: Search and metadata indexing
- `sdk/models/`: Canonical data types and configuration

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