"""
SSIS Parser Refactored - Modular Architecture

This module contains the refactored SSIS parser implementation, restructured from
a monolithic 3,598-line God Class into a clean, modular architecture following
SOLID principles.

Key Improvements:
- Separated concerns: XML parsing, graph construction, resolution, analysis
- 20+ focused modules averaging 150-200 lines each
- Eliminated 46 duplication points
- Reduced parameter proliferation (8-12 params → 1 context object)
- Improved testability and maintainability

Architecture:
- parsers/: XML parsing layer (component-specific parsers)
- models/: Domain models and context objects
- builders/: Graph construction layer
- resolvers/: Parameter and expression resolution
- analyzers/: SQL, script, type, and schema analysis
- extractors/: XML extraction utilities
- orchestrator.py: High-level coordination

Original Monolith: metazcode/sdk/ingestion/ssis/ssis_parser.py (3,598 lines)
"""

__version__ = "2.0.0-refactored"
__author__ = "MetazCode Team"

from .orchestrator import SsisParserRefactored

__all__ = ["SsisParserRefactored"]
