"""Parser for PL/SQL procedures."""

import logging
import re
from typing import List, Tuple

from ....models.graph import Node, Edge
from ....models.canonical_types import EdgeType
from .base_parser import BasePlsqlParser

logger = logging.getLogger(__name__)


class ProcedureParser(BasePlsqlParser):
    """Parses PL/SQL procedure definitions."""

    PROC_PATTERN = re.compile(
        r"\b(create|replace)\s+(or\s+replace\s+)?procedure\s+([a-zA-Z0-9_\$#]+)",
        re.IGNORECASE
    )

    def detect_procedures(self, text: str) -> List[Tuple[str, Tuple[int, int]]]:
        """
        Detect procedure definitions in text.

        Returns:
            List of (procedure_name, (start_pos, end_pos)) tuples
        """
        procedures = []
        for match in self.PROC_PATTERN.finditer(text):
            proc_name = match.group(3)
            start = match.start()
            # Find END; statement (simple heuristic)
            end_pattern = re.compile(r"\bend\s+" + re.escape(proc_name) + r"\s*;", re.IGNORECASE)
            end_match = end_pattern.search(text, start)
            end = end_match.end() if end_match else len(text)
            procedures.append((proc_name, (start, end)))
        return procedures

    def parse_procedure(
        self,
        proc_name: str,
        block: str,
        line_number: int = 0
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Parse a procedure and create nodes/edges.

        Returns:
            Tuple of (nodes, edges)
        """
        nodes = []
        edges = []

        # Extract SQL statements
        sql_statements = self.extract_sql_statements(block)

        # Extract column lineage
        column_lineage = self.lineage_builder.extract_column_lineage(sql_statements)
        cursor_lineage = self.lineage_builder.extract_cursor_lineage(block)

        # Build properties
        properties = {
            "procedure_type": "stored_procedure",
            "operation_subtype": self.categorize_operation_subtype(sql_statements),
            "sql_statements_count": len(sql_statements),
            "block_size": len(block),
        }

        if column_lineage:
            properties["column_lineage"] = column_lineage

        if cursor_lineage:
            properties["cursor_lineage"] = cursor_lineage

        # Create operation node
        operation_node = self.graph_builder.create_operation_node(
            operation_name=proc_name,
            operation_type="procedure",
            properties=properties,
            line_number=line_number
        )
        nodes.append(operation_node)

        # Create connection edges
        self.connection_builder.create_uses_connection_edge(
            operation_id=operation_node.node_id,
            block=block,
            edges=edges
        )

        # Process tables
        self._process_tables(operation_node.node_id, block, nodes, edges)

        return nodes, edges

    def _process_tables(
        self,
        operation_id: str,
        block: str,
        nodes: List[Node],
        edges: List[Edge]
    ) -> None:
        """Process table references in procedure."""
        # CREATE TABLE statements
        for table_name, create_stmt in self.extract_create_tables(block):
            if not self.context.is_table_created(table_name):
                table_node = self.graph_builder.create_table_node(
                    table_name=table_name,
                    create_statement=create_stmt
                )
                if table_node:
                    nodes.append(table_node)
                    self.context.mark_table_created(table_name)

                    # CREATE = WRITES_TO
                    edge = self.graph_builder.create_edge(
                        source_id=operation_id,
                        target_id=table_node.node_id,
                        relation=EdgeType.WRITES_TO,
                        properties={"operation_type": "CREATE_TABLE"}
                    )
                    edges.append(edge)

        # Read tables (SELECT)
        for table_name in self.extract_tables_from_select(block):
            if not self.context.is_node_created(f"data_asset:table:{table_name}"):
                table_node = self.graph_builder.create_table_node(table_name=table_name)
                if table_node:
                    nodes.append(table_node)
                    self.context.mark_table_referenced(table_name)

            edge = self.graph_builder.create_edge(
                source_id=operation_id,
                target_id=f"data_asset:table:{table_name}",
                relation=EdgeType.READS_FROM
            )
            edges.append(edge)

        # Write tables (INSERT/UPDATE/MERGE)
        reads, writes = self.extract_tables_from_dml(block)
        for table_name in writes:
            if not self.context.is_node_created(f"data_asset:table:{table_name}"):
                table_node = self.graph_builder.create_table_node(table_name=table_name)
                if table_node:
                    nodes.append(table_node)
                    self.context.mark_table_referenced(table_name)

            edge = self.graph_builder.create_edge(
                source_id=operation_id,
                target_id=f"data_asset:table:{table_name}",
                relation=EdgeType.WRITES_TO
            )
            edges.append(edge)
