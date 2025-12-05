"""
SAS Loader - File Discovery and Ingestion Orchestration

Discovers and orchestrates parsing of SAS files in a project directory.
Follows the same pattern as SsisLoader for consistency.
"""

import logging
from pathlib import Path
from typing import Generator, Tuple, List, Dict, Any, Optional

from ..ingestion_tool import IngestionTool
from .sas_parser import CanonicalSasParser
from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType, EdgeType

logger = logging.getLogger(__name__)


class SasLoader(IngestionTool):
    """
    Ingestion tool for SAS projects.

    Discovers and parses:
    - .sas files (SAS programs)
    - autoexec.sas (auto-execution scripts)
    - Macro libraries

    Creates:
    - PIPELINE nodes for each .sas file
    - OPERATION nodes for DATA steps and PROCs
    - MACRO nodes for macro definitions
    - TABLE nodes for datasets
    - CONNECTION nodes for LIBNAMEs
    - VARIABLE nodes for macro variables
    """

    def ingest(self) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Discover and parse all .sas files in the project directory.

        Yields:
            Tuples of (nodes, edges) for the knowledge graph
        """
        # Discover macro variable context from autoexec.sas if present
        macro_context = self._discover_macro_context()

        # Discover library context from common patterns
        library_context = self._discover_library_context()

        # Create parser with context
        parser = CanonicalSasParser(
            connections_context=library_context,
            parameters_context=macro_context,
        )

        # Discover all .sas files
        sas_files = self.discover_files("*.sas")

        if sas_files:
            logger.info(f"Found {len(sas_files)} SAS file(s)")
        else:
            logger.debug("No SAS files discovered in the project")
            return

        # Sort files to process in logical order
        # autoexec.sas first, then others alphabetically
        sas_files = sorted(sas_files, key=self._file_priority)

        # Parse each file
        for file_path in sas_files:
            try:
                logger.info(f"Parsing SAS file: {file_path}")
                yield from parser.parse(str(file_path))
            except Exception as e:
                logger.error(f"Failed to parse {file_path}: {e}", exc_info=True)
                continue

    def _file_priority(self, path: Path) -> Tuple[int, str]:
        """
        Return sort key for file processing order.

        Priority:
        1. autoexec.sas (defines global macros/libraries)
        2. Files starting with underscore (often utility files)
        3. Other files alphabetically
        """
        name = path.name.lower()

        if name == "autoexec.sas":
            return (0, name)
        elif name.startswith("_"):
            return (1, name)
        else:
            return (2, name)

    def _discover_macro_context(self) -> Dict[str, Dict[str, Any]]:
        """
        Discover macro variable context from autoexec.sas and common patterns.
        """
        context = {}

        # Look for autoexec.sas
        autoexec_files = list(self.root_path.rglob("autoexec.sas"))

        for autoexec in autoexec_files:
            try:
                with open(autoexec, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()

                # Extract %LET statements
                import re
                let_pattern = re.compile(r'%let\s+(\w+)\s*=\s*([^;]*);', re.IGNORECASE)

                for match in let_pattern.finditer(content):
                    var_name = match.group(1)
                    var_value = match.group(2).strip()
                    context[var_name.lower()] = {
                        "value": var_value,
                        "source": str(autoexec),
                    }

                logger.info(f"Found {len(context)} macro variables in {autoexec}")

            except Exception as e:
                logger.warning(f"Failed to parse {autoexec}: {e}")

        return context

    def _discover_library_context(self) -> Dict[str, Dict[str, Any]]:
        """
        Discover library (LIBNAME) context from common patterns.
        """
        context = {}

        # Look for common library definition files
        for pattern in ["*.sas", "autoexec.sas", "libnames.sas"]:
            for sas_file in self.root_path.rglob(pattern):
                try:
                    with open(sas_file, "r", encoding="utf-8", errors="replace") as f:
                        content = f.read()

                    # Extract LIBNAME statements
                    import re
                    libname_pattern = re.compile(
                        r'\blibname\s+(\w+)\s+(?:(\w+)\s+)?["\']([^"\']+)["\']',
                        re.IGNORECASE
                    )

                    for match in libname_pattern.finditer(content):
                        libref = match.group(1)
                        engine = match.group(2) or "BASE"
                        path = match.group(3)

                        context[libref.lower()] = {
                            "path": path,
                            "engine": engine,
                            "source": str(sas_file),
                        }

                except Exception as e:
                    logger.debug(f"Failed to extract libraries from {sas_file}: {e}")

        if context:
            logger.info(f"Discovered {len(context)} library reference(s)")

        return context
