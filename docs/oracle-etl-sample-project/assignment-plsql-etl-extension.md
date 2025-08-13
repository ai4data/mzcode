# Assignment: Extending MetaZCode for PL/SQL ETL Support

## Course Information
- **Assignment**: PL/SQL ETL Ingestion Module Development
- **Duration**: 3-4 weeks
- **Prerequisites**: Python OOP, SQL/PL/SQL knowledge, Basic understanding of ETL processes
- **Technology Stack**: Python 3.9+, lxml/sqlparse, NetworkX

## Background

MetaZCode is an extensible tool that currently supports SSIS (SQL Server Integration Services) package analysis. It parses ETL packages, extracts metadata and business logic, creates a graph representation, and outputs the results in JSON format or to a graph database (Memgraph).

Your task is to extend this tool to support Oracle PL/SQL ETL processes by creating a new ingestion module that follows the existing architecture patterns.

## Learning Objectives

By completing this assignment, you will:
1. Understand plugin-based software architecture
2. Work with existing codebase and extend functionality
3. Parse and analyze PL/SQL code programmatically
4. Create graph representations of ETL workflows
5. Practice clean code and SOLID principles

## Assignment Overview

Create a complete PL/SQL ingestion module that:
- Discovers PL/SQL files in a project directory
- Parses PL/SQL procedures, functions, and packages
- Extracts ETL logic and metadata
- Maps Oracle constructs to canonical graph nodes and edges
- Integrates seamlessly with the existing MetaZCode framework

## Detailed Requirements

### 1. File Structure

Create the following file structure under `metazcode/sdk/ingestion/`:

```
metazcode/sdk/ingestion/
└── plsql/
    ├── __init__.py
    ├── plsql_loader.py        # Main loader class
    ├── plsql_parser.py        # PL/SQL parsing logic
    ├── oracle_type_mapping.py # Oracle data type mappings
    └── sql_analyzer.py        # SQL statement analysis
```

### 2. Core Components to Implement

#### 2.1 PlSqlLoader Class (`plsql_loader.py`)

Create a class that extends `IngestionTool`:

```python
from ..ingestion_tool import IngestionTool
from typing import Generator, Tuple, List, Dict, Any
from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType
import logging

class PlSqlLoader(IngestionTool):
    """
    An ingestion tool for Oracle PL/SQL ETL projects.
    """
    
    def ingest(self) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Main ingestion method. Must:
        1. Discover all PL/SQL files (.sql, .pls, .plb, .pks, .pkb)
        2. Parse database connection files if any
        3. Extract procedures, functions, and packages
        4. Create nodes and edges for the graph
        """
        # Your implementation here
        pass
```

#### 2.2 PL/SQL Parser (`plsql_parser.py`)

Implement a parser that can handle:

```python
class PlSqlParser:
    """
    Parser for Oracle PL/SQL code.
    """
    
    def parse_package(self, file_path: str) -> Tuple[List[Node], List[Edge]]:
        """Parse a PL/SQL package specification or body"""
        pass
    
    def parse_procedure(self, content: str, parent_id: str) -> Tuple[List[Node], List[Edge]]:
        """Parse a stored procedure"""
        pass
    
    def parse_function(self, content: str, parent_id: str) -> Tuple[List[Node], List[Edge]]:
        """Parse a stored function"""
        pass
    
    def extract_sql_statements(self, plsql_content: str) -> List[Dict[str, Any]]:
        """Extract and categorize SQL statements from PL/SQL code"""
        pass
```

### 3. Specific Parsing Requirements

Your parser must extract and handle:

#### 3.1 PL/SQL Constructs
- **Packages**: Package specifications and bodies
- **Procedures**: Stored procedures with parameters
- **Functions**: Functions with return types
- **Cursors**: Explicit cursors and cursor FOR loops
- **Triggers**: Database triggers (bonus)

#### 3.2 ETL Operations
Identify and extract:
- **SELECT statements**: Source data queries
- **INSERT statements**: Target data loads
- **UPDATE statements**: Data transformations
- **MERGE statements**: Upsert operations
- **BULK COLLECT**: Bulk data operations
- **FORALL statements**: Bulk DML operations

#### 3.3 Dependencies
Track:
- Table/View references
- Procedure/Function calls
- Package dependencies
- Database links (if used)

### 4. Node and Edge Mapping

Map PL/SQL constructs to canonical types:

#### Nodes:
- PL/SQL Package → `NodeType.PIPELINE`
- Stored Procedure → `NodeType.OPERATION` with `operation_subtype="PROCEDURE"`
- Function → `NodeType.OPERATION` with `operation_subtype="FUNCTION"`
- Tables/Views → `NodeType.DATA_ASSET`
- Database Connection → `NodeType.CONNECTION`

#### Edges:
- Package contains Procedure → `EdgeType.CONTAINS`
- Procedure reads from Table → `EdgeType.READS_FROM`
- Procedure writes to Table → `EdgeType.WRITES_TO`
- Procedure calls another Procedure → `EdgeType.DEPENDS_ON`

### 5. Implementation Details

#### 5.1 File Discovery
```python
def discover_plsql_files(self) -> Dict[str, List[Path]]:
    """
    Discover PL/SQL files by type.
    Returns dict with keys: 'packages', 'procedures', 'functions', 'scripts'
    """
    return {
        'packages': self.discover_files("*.pks") + self.discover_files("*.pkb"),
        'procedures': self.discover_files("*.prc"),
        'functions': self.discover_files("*.fnc"),
        'scripts': self.discover_files("*.sql")
    }
```

#### 5.2 Metadata Extraction
Each node should include relevant properties:

```python
# For a procedure node
procedure_node = Node(
    node_id=f"procedure:{package_name}.{procedure_name}",
    node_type=NodeType.OPERATION,
    name=procedure_name,
    properties={
        "operation_subtype": "PROCEDURE",
        "technology": "PL/SQL",
        "package": package_name,
        "parameters": [...],  # List of parameters
        "sql_statements": [...],  # Extracted SQL
        "tables_read": [...],  # Source tables
        "tables_written": [...],  # Target tables
        "dependencies": [...],  # Called procedures/functions
        "file_path": file_path,
        "line_number": line_no
    }
)
```

### 6. Oracle Type Mapping

Create `oracle_type_mapping.py`:

```python
from typing import Dict
from ..ssis.type_mapping import CanonicalDataType

class OracleDataTypeMapper:
    """Maps Oracle data types to canonical types"""
    
    ORACLE_TO_CANONICAL: Dict[str, CanonicalDataType] = {
        "NUMBER": CanonicalDataType.DECIMAL,
        "VARCHAR2": CanonicalDataType.VARCHAR,
        "DATE": CanonicalDataType.DATETIME,
        "CLOB": CanonicalDataType.TEXT,
        # Add all Oracle types...
    }
    
    def map_oracle_type(self, oracle_type: str) -> CanonicalDataType:
        """Convert Oracle type to canonical type"""
        pass
```

### 7. Testing Requirements

Create test files in `tests/sdk/ingestion/plsql/`:

1. **Unit Tests**: Test individual parsing functions
2. **Integration Tests**: Test full ingestion pipeline
3. **Sample PL/SQL Files**: Create realistic test cases

Example test structure:
```
tests/sdk/ingestion/plsql/
├── test_plsql_loader.py
├── test_plsql_parser.py
├── test_oracle_type_mapping.py
└── fixtures/
    ├── sample_package.pks
    ├── sample_package.pkb
    └── etl_procedure.sql
```

### 8. Deliverables

1. **Source Code**: All Python modules listed above
2. **Tests**: Comprehensive test suite with >80% coverage
3. **Documentation**: 
   - README.md for the plsql module
   - Inline code documentation
   - Usage examples
4. **Sample Data**: At least 3 real-world PL/SQL ETL examples
5. **Performance Report**: Analysis of parsing performance on large files

### 9. Grading Criteria

| Component | Weight | Description |
|-----------|--------|-------------|
| Core Functionality | 40% | PlSqlLoader and parser implementation |
| Code Quality | 20% | Clean code, proper OOP, follows patterns |
| Testing | 20% | Test coverage and quality |
| Documentation | 10% | Clear documentation and examples |
| Integration | 10% | Seamless integration with MetaZCode |

### 10. Bonus Challenges (Extra Credit)

1. **Advanced SQL Analysis** (10 points)
   - Parse complex SQL with CTEs, subqueries, and analytic functions
   - Extract column-level lineage

2. **Performance Optimization** (10 points)
   - Implement caching for parsed results
   - Parallel processing for large projects

3. **Trigger Support** (5 points)
   - Parse database triggers
   - Identify trigger-based ETL patterns

4. **Visualization** (5 points)
   - Create custom visualization for PL/SQL workflows
   - Show procedure call hierarchies

## Getting Started

1. **Setup Development Environment**:
   ```bash
   git clone <metazcode-repo>
   cd metazcode
   uv sync --extra dev
   ```

2. **Study Existing Code**:
   - Review `metazcode/sdk/ingestion/ssis/` for patterns
   - Understand the `IngestionTool` interface
   - Study how nodes and edges are created

3. **Start with Simple Cases**:
   - Begin with parsing a single procedure
   - Gradually add complexity

4. **Use Available Tools**:
   - Consider `sqlparse` library for SQL parsing
   - Use regex for PL/SQL construct identification
   - Leverage existing type mapping patterns

## Resources

1. **PL/SQL Reference**: Oracle PL/SQL Language Reference
2. **Python Libraries**:
   - `sqlparse`: SQL parsing library
   - `ply`: Python Lex-Yacc for custom parsers
   - `antlr4-python3-runtime`: For ANTLR-based parsing

3. **Graph Concepts**: NetworkX documentation

4. **MetaZCode Architecture**: See `docs/metazcode-architecture-deep-dive.md`

## Submission Guidelines

1. Create a feature branch: `feature/plsql-etl-support`
2. Commit regularly with meaningful messages
3. Include a CHANGELOG.md with your changes
4. Submit a pull request with:
   - Description of implementation approach
   - Test results
   - Performance metrics
   - Known limitations

## Questions?

- Use the course forum for general questions
- Create GitHub issues for bugs/clarifications
- Office hours: Tuesdays 2-4 PM

---

**Note**: This assignment simulates real-world software development. You're extending an existing system, so code quality, integration, and following established patterns are as important as functionality.

Good luck!