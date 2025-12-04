"""
PL/SQL Parser Orchestrator.

Coordinates the parsing workflow for PL/SQL files.
Similar to SSIS and Informatica orchestrators.
"""

import logging
from pathlib import Path
from typing import Generator, List, Tuple, Dict, Any, Optional

from ...models.graph import Node, Edge
from .models.parsing_context import PlsqlParsingContext
from .builders.graph_builder import PlsqlGraphBuilder
from .builders.column_lineage_builder import ColumnLineageBuilder
from .builders.connection_builder import ConnectionBuilder
from .parsers.procedure_parser import ProcedureParser
from .parsers.function_parser import FunctionParser
from .parsers.sql_statement_parser import SqlStatementParser
from .post_processing.graph_validator import GraphValidator
from .post_processing.node_enricher import NodeEnricher
from .sql_semantics import EnhancedPlsqlParser
from .type_mapping import PLSQLDataTypeMapper, TargetPlatform
from ...models.canonical_types import EdgeType

logger = logging.getLogger(__name__)


class PlsqlOrchestrator:
    """
    Orchestrates PL/SQL file parsing workflow.

    Phases:
    1. Initialization - Set up context and builders
    2. Discovery - Find procedures, functions, anonymous blocks
    3. Parsing - Parse each construct
    4. Post-processing - Validate and enrich graph
    5. Finalization - Create pipeline node and containment edges
    """

    def __init__(
        self,
        connections_context: Optional[Dict[str, Dict[str, Any]]] = None,
        parameters_context: Optional[Dict[str, Dict[str, Any]]] = None,
        enable_type_mapping: bool = True,
        target_platforms: Optional[List[str]] = None
    ):
        """
        Initialize orchestrator.

        Args:
            connections_context: Oracle connection context
            parameters_context: Oracle parameter context
            enable_type_mapping: Whether to enable type mapping
            target_platforms: List of target platforms for type conversion
        """
        self.connections_context = connections_context or {}
        self.parameters_context = parameters_context or {}
        self.enable_type_mapping = enable_type_mapping
        self.target_platforms = target_platforms or ["sql_server", "postgresql"]
        self.logger = logging.getLogger(__name__)

    def parse(
        self,
        file_path: str
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Parse a PL/SQL file and yield nodes and edges.

        Args:
            file_path: Path to PL/SQL file

        Yields:
            Tuples of (nodes, edges)
        """
        path = Path(file_path)

        # Read file
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            self.logger.error(f"Failed to read file {file_path}: {e}")
            return

        # Phase 1: Initialize context and builders
        context = self._initialize_context(path, text)
        builders = self._initialize_builders(context)

        # Phase 2: Discover constructs
        constructs = self._discover_constructs(text, context)

        if not constructs:
            self.logger.warning(f"No PL/SQL constructs found in {file_path}")
            return

        # Phase 3: Parse constructs
        all_nodes = []
        all_edges = []

        for construct_type, construct_name, block, line_num in constructs:
            try:
                nodes, edges = self._parse_construct(
                    construct_type,
                    construct_name,
                    block,
                    line_num,
                    builders,
                    context
                )
                all_nodes.extend(nodes)
                all_edges.extend(edges)
            except Exception as e:
                self.logger.error(
                    f"Failed to parse {construct_type} '{construct_name}' in {file_path}: {e}",
                    exc_info=True
                )

        # Phase 4: Post-processing
        all_nodes, all_edges = self._post_process(all_nodes, all_edges)

        # Phase 5: Create pipeline node and containment edges
        pipeline_node, containment_edges = self._finalize_graph(
            all_nodes,
            builders["graph_builder"],
            context
        )

        all_nodes.insert(0, pipeline_node)  # Add pipeline at the beginning
        all_edges.extend(containment_edges)

        # Yield the complete graph
        yield all_nodes, all_edges

    def _initialize_context(self, path: Path, text: str) -> PlsqlParsingContext:
        """Initialize parsing context."""
        return PlsqlParsingContext(
            file_path=path,
            file_content=text,
            connections_context=self.connections_context,
            parameters_context=self.parameters_context,
            enable_type_mapping=self.enable_type_mapping,
            target_platforms=self.target_platforms
        )

    def _initialize_builders(self, context: PlsqlParsingContext) -> Dict[str, Any]:
        """Initialize builder components."""
        # Create type mapper and SQL parser
        type_mapper = PLSQLDataTypeMapper() if context.enable_type_mapping else None
        sql_parser = EnhancedPlsqlParser()

        # Create builders
        graph_builder = PlsqlGraphBuilder(context, type_mapper)
        lineage_builder = ColumnLineageBuilder(sql_parser)
        connection_builder = ConnectionBuilder(context, graph_builder)

        # Create parsers
        procedure_parser = ProcedureParser(
            context, graph_builder, lineage_builder, connection_builder
        )
        function_parser = FunctionParser(
            context, graph_builder, lineage_builder, connection_builder
        )
        sql_parser_obj = SqlStatementParser(
            context, graph_builder, lineage_builder, connection_builder
        )

        return {
            "graph_builder": graph_builder,
            "lineage_builder": lineage_builder,
            "connection_builder": connection_builder,
            "procedure_parser": procedure_parser,
            "function_parser": function_parser,
            "sql_parser": sql_parser_obj
        }

    def _discover_constructs(
        self,
        text: str,
        context: PlsqlParsingContext
    ) -> List[Tuple[str, str, str, int]]:
        """
        Discover all PL/SQL constructs in the file.

        Returns:
            List of (type, name, block, line_number) tuples
        """
        constructs = []

        # Strip comments for analysis
        clean_text = self._strip_comments(text)

        # Discover procedures
        proc_parser = ProcedureParser(None, None, None, None)
        for proc_name, (start, end) in proc_parser.detect_procedures(clean_text):
            block = clean_text[start:end]
            line_num = text[:start].count('\n') + 1
            constructs.append(("procedure", proc_name, block, line_num))
            context.add_operation(proc_name, "procedure", start, end)

        # Discover functions
        func_parser = FunctionParser(None, None, None, None)
        for func_name, (start, end) in func_parser.detect_functions(clean_text):
            block = clean_text[start:end]
            line_num = text[:start].count('\n') + 1
            constructs.append(("function", func_name, block, line_num))
            context.add_operation(func_name, "function", start, end)

        # If no procedures/functions, check for anonymous block or standalone SQL
        if not constructs:
            sql_parser = SqlStatementParser(None, None, None, None)
            if sql_parser.has_anonymous_block(clean_text):
                constructs.append(("anonymous_block", "anonymous_block", clean_text, 1))
                context.add_operation("anonymous_block", "anonymous_block", 0, len(clean_text))

        return constructs

    def _parse_construct(
        self,
        construct_type: str,
        construct_name: str,
        block: str,
        line_num: int,
        builders: Dict[str, Any],
        context: PlsqlParsingContext
    ) -> Tuple[List[Node], List[Edge]]:
        """Parse a single construct."""
        context.set_current_operation(
            f"pipeline:{context.file_path.stem}:operation:{construct_name}",
            construct_name
        )

        if construct_type == "procedure":
            nodes, edges = builders["procedure_parser"].parse_procedure(
                construct_name, block, line_num
            )
        elif construct_type == "function":
            nodes, edges = builders["function_parser"].parse_function(
                construct_name, block, line_num
            )
        elif construct_type == "anonymous_block":
            nodes, edges = builders["sql_parser"].parse_anonymous_block(block)
        else:
            self.logger.warning(f"Unknown construct type: {construct_type}")
            nodes, edges = [], []

        context.clear_current_operation()
        return nodes, edges

    def _post_process(
        self,
        nodes: List[Node],
        edges: List[Edge]
    ) -> Tuple[List[Node], List[Edge]]:
        """Post-process graph with validation and enrichment."""
        # Validate
        validator = GraphValidator()
        nodes, edges = validator.validate_graph(nodes, edges)

        # Enrich
        enricher = NodeEnricher()
        nodes = enricher.enrich_nodes(nodes)

        return nodes, edges

    def _finalize_graph(
        self,
        operation_nodes: List[Node],
        graph_builder: PlsqlGraphBuilder,
        context: PlsqlParsingContext
    ) -> Tuple[Node, List[Edge]]:
        """Create pipeline node and containment edges."""
        # Create pipeline node
        pipeline_node = graph_builder.create_pipeline_node()

        # Create CONTAINS edges from pipeline to all operations
        containment_edges = []
        for node in operation_nodes:
            if node.node_type == "operation":
                edge = graph_builder.create_edge(
                    source_id=pipeline_node.node_id,
                    target_id=node.node_id,
                    relation=EdgeType.CONTAINS,
                    properties={"relationship_type": "pipeline_contains_operation"}
                )
                containment_edges.append(edge)

        return pipeline_node, containment_edges

    def _strip_comments(self, text: str) -> str:
        """Remove SQL comments from text."""
        import re
        # Remove /* */ comments
        text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
        # Remove -- comments
        text = re.sub(r"--.*?$", " ", text, flags=re.M)
        return text
