"""
PL/SQL Parser Refactored - Modular Architecture

This module contains the refactored PL/SQL parser implementation, restructured from
a monolithic 2,212-line implementation into a clean, modular architecture following
SOLID principles and matching SSIS/Informatica patterns.

Key Improvements:
- Separated concerns: parsing, graph construction, lineage, connections
- 18+ focused modules averaging 150-280 lines each
- Orchestrator pattern with 5-phase workflow
- Builder pattern for graph, lineage, and connection construction
- Post-processing pipeline (validation, enrichment)
- Fixed critical gap: USES_CONNECTION edges

Architecture:
- parsers/: Component-specific parsers (procedure, function, SQL)
- models/: Domain models and parsing context
- builders/: Graph construction, lineage, connection handling
- post_processing/: Validation and enrichment
- orchestrator.py: High-level coordination
- type_mapping.py: Oracle type mapping system (preserved)
- sql_semantics.py: SQLGlot-based SQL parser (preserved)

Original Monolith: metazcode/sdk/ingestion/plsql_canonical/plsql_parser.py (2,212 lines)
"""

__version__ = "2.0.0-refactored"
__author__ = "MetazCode Team"

from .orchestrator import PlsqlOrchestrator

__all__ = ["PlsqlOrchestrator"]
