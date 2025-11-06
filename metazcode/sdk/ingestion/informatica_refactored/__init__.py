"""
Informatica Parser Refactored - Modular Architecture

This module contains the refactored Informatica parser implementation, restructured from
a monolithic 4,065-line God Class into a clean, modular architecture following
SOLID principles.

Key Improvements:
- Separated concerns: XML parsing, graph construction, resolution, analysis
- 20+ focused modules averaging 150-200 lines each
- Eliminated 30+ duplication points
- Reduced context proliferation (5 dicts → 1 context object)
- Improved testability and maintainability

Architecture:
- parsers/: XML parsing layer (transformation-specific parsers)
- models/: Domain models and context objects
- builders/: Graph construction layer
- resolvers/: Parameter and variable resolution
- analyzers/: SQL, type analysis
- extractors/: XML extraction utilities
- orchestrator.py: High-level coordination

Original Monolith: metazcode/sdk/ingestion/informatica/informatica_parser.py (4,065 lines)
"""

__version__ = "2.0.0-refactored"
__author__ = "MetazCode Team"

from .orchestrator import InformaticaParserRefactored

__all__ = ["InformaticaParserRefactored"]
