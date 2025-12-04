# PL/SQL Parser - Production Architecture

Production-quality PL/SQL parser with modular architecture matching SSIS and Informatica parsers.

## Architecture Overview

```
plsql/
├── __init__.py                 # Module exports
├── plsql_loader.py            # Main loader (extends IngestionTool)
├── orchestrator.py            # Parsing workflow coordinator
├── type_mapping.py            # Oracle type mapping system (ported)
├── sql_semantics.py           # SQLGlot-based SQL parser (ported)
│
├── models/                    # Data models
│   ├── __init__.py
│   └── parsing_context.py     # State management during parsing
│
├── builders/                  # Builder pattern for graph construction
│   ├── __init__.py
│   ├── graph_builder.py       # Node/edge creation with traceability
│   ├── column_lineage_builder.py  # Column-level lineage extraction
│   └── connection_builder.py  # Connection/parameter edge creation
│
├── parsers/                   # Component-specific parsers
│   ├── __init__.py
│   ├── base_parser.py         # Base parser with common functionality
│   ├── procedure_parser.py    # Stored procedure parser
│   ├── function_parser.py     # Function parser
│   └── sql_statement_parser.py # Anonymous block/SQL statement parser
│
└── post_processing/           # Graph enrichment pipeline
    ├── __init__.py
    ├── graph_validator.py     # Validation and cleanup
    └── node_enricher.py       # Metadata enrichment
```

## Key Features

### ✅ Architectural Sophistication (SSIS/Informatica Level)
- **Modular Design**: 6-level directory structure vs. flat single file
- **Builder Pattern**: Separate builders for graph, lineage, connections
- **Orchestrator Pattern**: Coordinates parsing workflow phases
- **Post-Processing Pipeline**: Validation and enrichment after parsing
- **Parsing Context**: Stateful context management with type safety

### ✅ Connection Handling (Critical Gap Fixed)
- **Connection Nodes**: Created from tnsnames.ora and connection scripts
- **USES_CONNECTION Edges**: Operations properly linked to connections
- **Connection Discovery**: Parses database links and CONNECT statements
- **Session Context**: Tracks connection usage per operation

### ✅ Advanced Features (From Original Implementation)
- **Type Mapping**: Best-in-class Oracle → target platform conversions
- **SQL Semantics**: SQLGlot-based robust SQL parsing
- **Column Lineage**: Comprehensive source-to-target column tracking
- **ETL Pattern Detection**: Identifies BULK_LOAD, MERGE_UPSERT, etc.

### ✅ Integration
- **IngestionTool Pattern**: Properly extends framework base class
- **Canonical Types**: Uses NodeType, EdgeType, SourceContext
- **Traceability**: Full source context on all nodes/edges
- **Auto-Discovery**: Framework automatically discovers PlsqlLoader

## Usage

### Basic Usage
```python
from metazcode.sdk.ingestion.plsql import PlsqlLoader

# Create loader
loader = PlsqlLoader(
    root_path="/path/to/plsql/project",
    enable_type_mapping=True,
    target_platforms=["sql_server", "postgresql"]
)

# Parse files
for nodes, edges in loader.ingest():
    # Process nodes and edges
    print(f"Generated {len(nodes)} nodes, {len(edges)} edges")
```

### Via CLI
```bash
# Parse PL/SQL project
uv run python -m metazcode full --path /path/to/plsql/project

# Parse with LLM enrichment
uv run python -m metazcode full --path /path/to/plsql/project --enable-llm
```

## Parsing Workflow

### Phase 1: Initialization
1. Discover Oracle configuration files (tnsnames.ora, etc.)
2. Parse connection and parameter contexts
3. Create connection/parameter nodes
4. Initialize builders and parsers

### Phase 2: Discovery
1. Scan for PL/SQL files (.sql, .pks, .pkb)
2. Detect procedures, functions, anonymous blocks
3. Build operation registry

### Phase 3: Parsing
1. Parse each construct with appropriate parser
2. Extract SQL statements and semantics
3. Build column lineage
4. Create nodes and edges with traceability

### Phase 4: Post-Processing
1. Validate graph (remove duplicates, invalid edges)
2. Enrich nodes (compute complexity, metadata)
3. Normalize properties

### Phase 5: Finalization
1. Create pipeline node for file
2. Create CONTAINS edges
3. Yield complete graph

## Output Format

### Node Types
- **PIPELINE**: PL/SQL file
- **OPERATION**: Procedure, function, anonymous block
- **DATA_ASSET**: Tables referenced or created
- **CONNECTION**: Oracle database connections
- **PARAMETER**: Oracle parameters and variables

### Edge Types
- **CONTAINS**: Pipeline → Operations
- **READS_FROM**: Operation → Tables (SELECT)
- **WRITES_TO**: Operation → Tables (INSERT/UPDATE/CREATE)
- **USES_CONNECTION**: Operation → Connections ✅ **NEW**
- **USES_PARAMETER**: Operation → Parameters ✅ **NEW**

### Metadata
- **Traceability**: source_file_path, line_number, xml_path
- **Type Mapping**: Oracle types → target platform types with confidence
- **Column Lineage**: Source-to-target column mappings
- **SQL Semantics**: JOINs, aliases, expressions
- **Complexity Scores**: Operation complexity metrics

## Comparison to Original Implementation

| Feature | Original | New Architecture |
|---------|----------|-----------------|
| **Architecture** | Flat (4 files) | Modular (18 files, 6 dirs) |
| **Parser Size** | 2212 lines (monolithic) | Distributed across components |
| **Orchestrator** | ❌ No | ✅ Yes |
| **Post-Processing** | ❌ Inline validation | ✅ Separate pipeline |
| **Builder Pattern** | ❌ No | ✅ 3 specialized builders |
| **Connection Edges** | ❌ Missing | ✅ Implemented |
| **Type Mapping** | ✅ Excellent | ✅ Preserved |
| **SQL Semantics** | ✅ SQLGlot-based | ✅ Preserved |
| **Column Lineage** | ✅ Good | ✅ Enhanced |
| **Maintainability** | ⚠️ Low | ✅ High |

## Files Ported From Original

- **type_mapping.py**: Oracle type mapping system (100% preserved)
- **sql_semantics.py**: SQLGlot-based SQL parser (100% preserved)

These modules represent the best features of the original implementation and were ported without modification.

## Integration with MetaZCode

### Auto-Discovery
The parser is automatically discovered by the MetaZCode orchestrator because:
1. `PlsqlLoader` extends `IngestionTool`
2. Module is in `metazcode/sdk/ingestion/plsql/`
3. Exports `PlsqlLoader` in `__init__.py`

### Cross-Package Analysis
After ingestion, the cross-package analyzer will:
- Detect tables shared across multiple PL/SQL files
- Identify execution dependencies
- Create SHARES_RESOURCE edges
- Generate execution order recommendations

### LLM Enrichment
Optional AI-powered business summaries work automatically:
```bash
export OPENAI_API_KEY=your-key
uv run python -m metazcode full --path /plsql/project --enable-llm
```

## Testing

### Unit Tests (TODO)
```bash
pytest metazcode/sdk/ingestion/plsql/tests/
```

### Integration Test
```bash
# Test with sample PL/SQL file
uv run python -m metazcode ingest --path /path/to/sample.sql

# Full analysis
uv run python -m metazcode full --path /path/to/plsql/project
```

## Development

### Adding New Parsers
To parse additional PL/SQL constructs:

1. Create parser in `parsers/`:
```python
from .base_parser import BasePlsqlParser

class TriggerParser(BasePlsqlParser):
    def parse_trigger(self, trigger_name, block, line_num):
        # Implementation
        pass
```

2. Register in `orchestrator.py`:
```python
trigger_parser = TriggerParser(context, graph_builder, ...)
```

3. Update discovery logic to find triggers

### Adding New Builders
To add specialized builders:

1. Create in `builders/`:
```python
class CustomBuilder:
    def build_custom_metadata(self, ...):
        pass
```

2. Initialize in orchestrator
3. Use in parsers

## Future Enhancements

- [ ] Package dependency analysis
- [ ] Trigger parser
- [ ] SQL*Loader control file parser
- [ ] APEX export parser
- [ ] Cross-file procedure call graph
- [ ] Oracle Forms (.fmb) parser
- [ ] Performance analysis metadata

## References

- SSIS Parser: `metazcode/sdk/ingestion/ssis/`
- Informatica Parser: `metazcode/sdk/ingestion/informatica/`
- Original Implementation: `mzcode_update/metazcode/sdk/ingestion/plsql/`
