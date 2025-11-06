"""
Informatica Parser Orchestrator - Refactored Architecture

This orchestrator coordinates all parsing subsystems, replacing the monolithic
4,065-line God Class with a clean, modular design.

Key improvements:
- Single Responsibility: Only coordinates, doesn't parse
- Dependency Injection: Receives specialized parsers
- ~200 lines vs 4,065 lines
- Clear separation of concerns
"""

import os
from typing import List, Tuple, Generator, Optional, Dict, Any
from lxml import etree
import logging

from ...models.graph import Node, Edge
from .models.parsing_context import InformaticaParsingContext
from .builders.graph_builder import InformaticaGraphBuilder

# Import the legacy parser temporarily for fallback
from ..informatica.informatica_parser import CanonicalInformaticaParser

logger = logging.getLogger(__name__)


class InformaticaParserRefactored:
    """
    Refactored Informatica parser with modular architecture.

    Phase 1 Implementation:
    - Demonstrates new architecture
    - Uses legacy parser as fallback
    - Provides migration path

    Phase 2 (Future):
    - Transformation parsers fully implemented
    - Legacy parser removed
    - All 12 responsibilities separated
    """

    def __init__(
        self,
        connections_context: Optional[Dict[str, Dict[str, Any]]] = None,
        parameters_context: Optional[Dict[str, Dict[str, Any]]] = None,
        variables_context: Optional[Dict[str, Dict[str, Any]]] = None,
        parameter_file_context: Optional[Dict[str, Dict[str, Any]]] = None,
        enable_schema_introspection: bool = True,
        enable_type_mapping: bool = True,
        target_platforms: Optional[List[str]] = None,
        use_legacy_fallback: bool = True,
    ):
        """
        Initialize the refactored Informatica parser.

        Args:
            connections_context: External connection definitions
            parameters_context: External parameter definitions
            variables_context: Workflow/mapping variables
            parameter_file_context: Parameter file (.par) definitions
            enable_schema_introspection: Enable database schema introspection
            enable_type_mapping: Enable type mapping for target platforms
            target_platforms: List of target platforms for type mapping
            use_legacy_fallback: Use legacy parser (Phase 1 migration strategy)
        """
        self.connections_context = connections_context or {}
        self.parameters_context = parameters_context or {}
        self.variables_context = variables_context or {}
        self.parameter_file_context = parameter_file_context or {}
        self.enable_schema_introspection = enable_schema_introspection
        self.enable_type_mapping = enable_type_mapping
        self.target_platforms = target_platforms or ["sql_server", "postgresql"]
        self.use_legacy_fallback = use_legacy_fallback

        # Legacy fallback for Phase 1
        if self.use_legacy_fallback:
            # Note: Legacy parser uses parameter_file_path instead of contexts
            parameter_file_path = None
            if parameter_file_context:
                # Extract path from context if available
                parameter_file_path = parameter_file_context.get("__file_path__")

            self.legacy_parser = CanonicalInformaticaParser(
                connections_context=connections_context,
                parameters_context=parameters_context,
                parameter_file_path=parameter_file_path,
                enable_schema_introspection=enable_schema_introspection,
                enable_type_mapping=enable_type_mapping,
                target_platforms=target_platforms,
            )
            logger.info("Refactored Informatica parser initialized with legacy fallback enabled")
        else:
            self.legacy_parser = None
            logger.info("Refactored Informatica parser initialized in pure mode (no fallback)")

    def parse(
        self, file_path: str
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Parse a single Informatica XML file (.xml) and yield discovered nodes and edges.

        Phase 1 Implementation:
        - Uses new architecture (ParsingContext, GraphBuilder)
        - Falls back to legacy parser for complex parsing
        - Demonstrates the refactored design

        Args:
            file_path: Path to the Informatica XML file

        Yields:
            Tuples of (nodes, edges) discovered
        """
        logger.info(f"Parsing Informatica file: {file_path}")

        # Phase 1: Use legacy parser with new architecture wrapper
        if self.use_legacy_fallback:
            logger.debug("Using legacy parser fallback (Phase 1)")
            yield from self._parse_with_legacy(file_path)
            return

        # Phase 2: Pure refactored implementation (future)
        try:
            yield from self._parse_with_refactored(file_path)
        except Exception as e:
            logger.error(f"Error parsing {file_path}: {e}", exc_info=True)
            return

    def _parse_with_legacy(
        self, file_path: str
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Parse using legacy parser (Phase 1 migration strategy).

        This demonstrates that the new architecture is compatible while
        we incrementally migrate components.
        """
        # Use legacy parser
        yield from self.legacy_parser.parse(file_path)

    def _parse_with_refactored(
        self, file_path: str
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Parse using fully refactored implementation (Phase 2 - future).

        This is the target architecture:
        - InformaticaParsingContext holds state
        - InformaticaGraphBuilder creates nodes/edges
        - Specialized parsers handle specific XML elements
        - Resolvers handle parameter/variable resolution
        - Analyzers handle SQL/type analysis
        """
        # Load and parse XML
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            if content.startswith("\ufeff"):  # Remove BOM
                content = content[1:]
            root = etree.fromstring(content.encode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to load XML from {file_path}: {e}")
            return

        # Determine file type (workflow or mapping)
        file_type = "workflow" if root.tag == "POWERMART" else "mapping"

        # Extract primary entity name
        if file_type == "workflow":
            workflow_elem = root.find(".//WORKFLOW")
            if workflow_elem is None:
                logger.error("No workflow found in file")
                return
            entity_name = workflow_elem.get("NAME", os.path.basename(file_path))
            entity_id = f"workflow:{entity_name}"
        else:
            mapping_elem = root.find(".//MAPPING")
            if mapping_elem is None:
                logger.error("No mapping found in file")
                return
            entity_name = mapping_elem.get("NAME", os.path.basename(file_path))
            entity_id = f"mapping:{entity_name}"

        # Initialize context and builder
        context = InformaticaParsingContext(
            file_path=file_path,
            file_type=file_type,
            workflow_name=entity_name if file_type == "workflow" else None,
            mapping_name=entity_name if file_type == "mapping" else None,
            workflow_id=entity_id if file_type == "workflow" else None,
            mapping_id=entity_id if file_type == "mapping" else None,
            connections_context=self.connections_context,
            parameters_context=self.parameters_context,
            variables_context=self.variables_context,
            parameter_file_context=self.parameter_file_context,
            enable_schema_introspection=self.enable_schema_introspection,
            enable_type_mapping=self.enable_type_mapping,
            target_platforms=self.target_platforms,
        )

        builder = InformaticaGraphBuilder(context)

        # Create primary entity node
        if file_type == "workflow":
            builder.create_workflow_node(entity_name)
        else:
            builder.create_mapping_node(entity_name)

        # TODO Phase 2: Implement specialized parsers
        # - WorkflowParser().parse(root, context, builder)
        # - MappingParser().parse(root, context, builder)
        # - TransformationParsers for each type...
        # - ParameterResolver().resolve(context)
        # - VariableResolver().resolve(context)
        # - ConnectorParser().parse(root, context, builder)

        logger.warning(
            "Pure refactored mode not fully implemented yet. "
            "Enable use_legacy_fallback=True for working parser."
        )

        # Return collected graph
        yield context.nodes, context.edges

        # Log statistics
        stats = builder.get_statistics()
        logger.info(
            f"Parsing complete: {stats['nodes_created']} nodes, "
            f"{stats['edges_created']} edges, "
            f"{stats['duplicates_detected']} duplicates detected"
        )

    def get_info(self) -> Dict[str, Any]:
        """Get information about the parser configuration."""
        return {
            "mode": "legacy_fallback" if self.use_legacy_fallback else "pure_refactored",
            "enable_schema_introspection": self.enable_schema_introspection,
            "enable_type_mapping": self.enable_type_mapping,
            "target_platforms": self.target_platforms,
            "version": "2.0.0-refactored",
        }


# Backward compatibility alias
class CanonicalInformaticaParserRefactored(InformaticaParserRefactored):
    """
    Backward compatibility alias for legacy code.

    Use InformaticaParserRefactored instead.
    """
    pass
