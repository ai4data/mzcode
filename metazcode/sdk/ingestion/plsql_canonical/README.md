# PL/SQL Parser - Original Canonical Implementation

## Overview

This folder contains the **original monolithic PL/SQL parser** implementation that was created by a developer before the modular refactoring.

**Status**: ARCHIVED - Original implementation preserved for reference
**Current Version**: See `../plsql/` for the refactored modular version

---

## Files

### Core Implementation

- **plsql_parser.py** (2,212 lines) - Monolithic parser implementation
  - Single class handling all PL/SQL parsing logic
  - Mixed concerns: parsing, graph building, lineage extraction
  - No orchestrator pattern
  - Missing USES_CONNECTION edges

- **plsql_loader.py** (333 lines) - Loader for file discovery
  - Extends IngestionTool
  - Handles Oracle configuration parsing
  - Creates connection and parameter nodes (but never linked them)

### Supporting Modules (Excellent Quality - Preserved)

- **sql_semantics.py** (1,800 lines) - SQLGlot-based SQL parser
  - Robust SQL parsing with comprehensive JOIN extraction
  - Column alias and expression handling
  - Table reference tracking
  - **Rating: Excellent** - Preserved in refactored version

- **type_mapping.py** (660 lines) - Oracle type mapping system
  - Oracle → Canonical type mapping
  - Canonical → 7 target platforms (SQL Server, PostgreSQL, MySQL, etc.)
  - Conversion risk assessment and confidence scoring
  - **Rating: Best-in-class** - Preserved in refactored version

---

## Architecture Issues (Why Refactoring Was Needed)

### Problems in Original Implementation

1. **Monolithic God Class** (2,212 lines in single file)
   - Hard to maintain and test
   - Mixed responsibilities
   - No separation of concerns

2. **Missing USES_CONNECTION Edges** ⚠️ CRITICAL GAP
   - Created connection nodes but never linked them to operations
   - Incomplete graph representation
   - Missing critical relationship data

3. **No Orchestrator Pattern**
   - Parsing logic mixed throughout
   - No clear workflow phases
   - Difficult to extend

4. **No Builder Pattern**
   - Nodes/edges created inline throughout code
   - Inconsistent traceability
   - Hard to centralize metadata

5. **No Post-Processing Pipeline**
   - Basic validation inline
   - No systematic enrichment
   - Missing graph cleanup

6. **Dict-Based Context**
   - Used plain dictionaries for state
   - No type safety
   - Error-prone

7. **Limited Modularity**
   - 4 files total (flat structure)
   - Everything in one class
   - Hard to test components individually

---

## What Was Preserved

The refactored version preserved the **excellent** modules:

✅ **sql_semantics.py** - 100% preserved, no changes
✅ **type_mapping.py** - 100% preserved, no changes

These modules represent best-in-class implementations and were copied directly to the refactored version.

---

## What Was Fixed/Improved

The refactored version (`../plsql/`) addresses all issues:

1. ✅ **Modular Architecture** - 18 files across 6-level directory structure
2. ✅ **USES_CONNECTION Edges** - Properly implemented with ConnectionBuilder
3. ✅ **Orchestrator Pattern** - 5-phase workflow coordination
4. ✅ **Builder Pattern** - 3 specialized builders
5. ✅ **Post-Processing** - Validation and enrichment pipeline
6. ✅ **Typed Context** - PlsqlParsingContext dataclass
7. ✅ **Component Parsers** - Procedure, Function, SQL statement parsers
8. ✅ **Full Traceability** - Consistent source context on all nodes/edges

---

## Comparison

| Aspect | Original (This Folder) | Refactored (`../plsql/`) |
|--------|------------------------|--------------------------|
| **Parser Size** | 2,212 lines (monolithic) | Distributed (18 modules) |
| **Architecture** | Flat (4 files) | Modular (6-level) |
| **USES_CONNECTION** | ❌ Missing | ✅ Implemented |
| **Orchestrator** | ❌ No | ✅ Yes (5 phases) |
| **Builders** | ❌ No | ✅ Yes (3 builders) |
| **Post-Processing** | ❌ Inline | ✅ Pipeline |
| **Context** | Dict-based | Typed dataclass |
| **Testability** | ⚠️ Hard | ✅ Easy |
| **Maintainability** | ⚠️ Low | ✅ High |
| **Score** | 6.5/10 | 9.5/10 |

---

## Usage (For Reference Only)

**⚠️ NOT RECOMMENDED** - Use the refactored version instead

```python
# Original canonical version (archived)
from metazcode.sdk.ingestion.plsql_canonical import PlsqlLoader, CanonicalPlsqlParser

# ⚠️ Has limitations:
# - Missing USES_CONNECTION edges
# - Monolithic structure
# - No post-processing
```

**✅ RECOMMENDED** - Use refactored version:

```python
# Refactored modular version (current)
from metazcode.sdk.ingestion.plsql import PlsqlLoader, PlsqlOrchestrator

# ✅ Benefits:
# - Complete USES_CONNECTION edges
# - Modular architecture
# - Post-processing pipeline
# - Better maintainability
```

---

## Why Keep This Version?

This folder is preserved for:

1. **Historical Reference** - Understanding the evolution
2. **Comparison** - Demonstrating refactoring improvements
3. **Regression Testing** - Ensuring feature parity
4. **Documentation** - Learning from architectural decisions

---

## Migration Path

If you're using the original implementation:

```python
# OLD (canonical - archived)
from metazcode.sdk.ingestion.plsql_canonical import PlsqlLoader

# NEW (refactored - current)
from metazcode.sdk.ingestion.plsql import PlsqlLoader

# API is compatible, just change import path
loader = PlsqlLoader(root_path="/path/to/oracle/project")
for nodes, edges in loader.ingest():
    # Same API, better implementation
    pass
```

**Key Difference**: The refactored version includes USES_CONNECTION edges that were missing in the original.

---

## See Also

- **Refactored Implementation**: `../plsql/` (current production version)
- **Implementation Summary**: `../plsql/IMPLEMENTATION_SUMMARY.md`
- **Architecture Documentation**: `../plsql/README.md`
- **Integration Status**: `../plsql/INTEGRATION_STATUS.md`

---

## Conclusion

This canonical implementation served as the foundation for the refactored version. While it had excellent type mapping and SQL parsing capabilities, it lacked the architectural sophistication needed for production use.

The refactored version (`../plsql/`) combines the strengths of this implementation with modern architectural patterns from SSIS and Informatica parsers.

**Recommendation**: Use `metazcode.sdk.ingestion.plsql` (refactored) instead of this canonical version.
