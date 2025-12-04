"""Parser for standalone SQL statements and anonymous blocks."""

import re
from typing import List, Tuple

from ....models.graph import Node, Edge
from .base_parser import BasePlsqlParser


class SqlStatementParser(BasePlsqlParser):
    """Parses standalone SQL statements and anonymous PL/SQL blocks."""

    BEGIN_PATTERN = re.compile(r"\bbegin\b", re.IGNORECASE)

    def has_anonymous_block(self, text: str) -> bool:
        """Check if text contains an anonymous block."""
        return bool(self.BEGIN_PATTERN.search(text))

    def parse_anonymous_block(self, text: str) -> Tuple[List[Node], List[Edge]]:
        """
        Parse an anonymous PL/SQL block.

        Returns:
            Tuple of (nodes, edges)
        """
        nodes = []
        edges = []

        block_name = "anonymous_block"
        sql_statements = self.extract_sql_statements(text)

        # Extract lineage
        column_lineage = self.lineage_builder.extract_column_lineage(sql_statements)

        # Build properties
        properties = {
            "operation_type": "anonymous_block",
            "operation_subtype": self.categorize_operation_subtype(sql_statements),
            "sql_statements_count": len(sql_statements),
            "block_size": len(text),
        }

        if column_lineage:
            properties["column_lineage"] = column_lineage

        # Create operation node
        operation_node = self.graph_builder.create_operation_node(
            operation_name=block_name,
            operation_type="anonymous_block",
            properties=properties
        )
        nodes.append(operation_node)

        # Create connection edges
        self.connection_builder.create_uses_connection_edge(
            operation_id=operation_node.node_id,
            block=text,
            edges=edges
        )

        # Process tables
        self._process_tables(operation_node.node_id, text, nodes, edges)

        return nodes, edges

    def _process_tables(self, operation_id: str, block: str, nodes: List[Node], edges: List[Edge]):
        """Process table references (same as procedure parser)."""
        from ....models.canonical_types import EdgeType

        # CREATE TABLE
        for table_name, create_stmt in self.extract_create_tables(block):
            if not self.context.is_table_created(table_name):
                table_node = self.graph_builder.create_table_node(
                    table_name=table_name,
                    create_statement=create_stmt
                )
                if table_node:
                    nodes.append(table_node)
                    self.context.mark_table_created(table_name)

                    edge = self.graph_builder.create_edge(
                        source_id=operation_id,
                        target_id=table_node.node_id,
                        relation=EdgeType.WRITES_TO,
                        properties={"operation_type": "CREATE_TABLE"}
                    )
                    edges.append(edge)

        # SELECT
        for table_name in self.extract_tables_from_select(block):
            if not self.context.is_node_created(f"data_asset:table:{table_name}"):
                table_node = self.graph_builder.create_table_node(table_name=table_name)
                if table_node:
                    nodes.append(table_node)

            edge = self.graph_builder.create_edge(
                source_id=operation_id,
                target_id=f"data_asset:table:{table_name}",
                relation=EdgeType.READS_FROM
            )
            edges.append(edge)

        # DML
        reads, writes = self.extract_tables_from_dml(block)
        for table_name in writes:
            if not self.context.is_node_created(f"data_asset:table:{table_name}"):
                table_node = self.graph_builder.create_table_node(table_name=table_name)
                if table_node:
                    nodes.append(table_node)

            edge = self.graph_builder.create_edge(
                source_id=operation_id,
                target_id=f"data_asset:table:{table_name}",
                relation=EdgeType.WRITES_TO
            )
            edges.append(edge)
