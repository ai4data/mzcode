# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development Setup
```bash
# Install dependencies (requires uv package manager)
uv sync

# For Memgraph database support
uv sync --extra memgraph

# Start Memgraph database (optional, for large-scale analysis)
docker-compose up -d

# Run installation test
uv run python test_installation.py

# Quick test script
./test.sh
```

### Main Commands
```bash
# Complete SSIS analysis (recommended for most users)
uv run python -m metazcode full --path <ssis_project_path>

# Step-by-step analysis
uv run python -m metazcode ingest --path <ssis_project_path>
uv run python -m metazcode analyze --path <ssis_project_path>

# Generate visualization
uv run python -m metazcode visualize --path <ssis_project_path>

# Export graph data
uv run python -m metazcode dump --path <ssis_project_path> --output results.json

# Run tests
uv run python -m pytest  # (if pytest is available)
```

### CLI Command Reference
- `full` / `complete`: Complete analysis pipeline (ingest + analyze + index)
- `ingest`: Extract business logic from SSIS packages
- `analyze`: Perform cross-package dependency analysis
- `visualize`: Generate graph visualization
- `dump`: Export graph data to JSON
- `ingest_n_index`: Run ingestion and indexing together

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

- The codebase uses a plugin-based architecture for ingestion tools
- Graph operations are abstracted through `GraphClientInterface`
- All file paths should be absolute when working with the API
- The system supports multiple target platforms for data type mapping
- Enhanced analysis results are saved as JSON for external consumption

## Output Files

- `enhanced_graph_full_analysis.json`: Complete graph with all analysis
- `enhanced_graph_with_dependencies.json`: Graph with cross-package dependencies
- Analysis result JSON files when `--output` is specified
- Index files for search capabilities when `--index-output` is specified