"""
SAS AST Node Classes - Python Kolasu/StarLasu Equivalent

This module implements AST node classes following the Tomassetti/Strumenta approach:
- All nodes inherit from SASNode base class with navigation support
- Lazy evaluation for macro bodies (parsed only when accessed)
- Source position tracking for traceability

References:
- https://tomassetti.me/how-to-use-the-sas-parser/
- https://tomassetti.me/challenges-in-parsing-legacy-languages-sas-macros/
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple, TYPE_CHECKING
from functools import cached_property
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ProcType(str, Enum):
    """SAS Procedure types."""
    SQL = "SQL"
    SORT = "SORT"
    IMPORT = "IMPORT"
    EXPORT = "EXPORT"
    PRINT = "PRINT"
    MEANS = "MEANS"
    FREQ = "FREQ"
    CONTENTS = "CONTENTS"
    LOGISTIC = "LOGISTIC"
    REG = "REG"
    CORR = "CORR"
    UNIVARIATE = "UNIVARIATE"
    ANOVA = "ANOVA"
    TTEST = "TTEST"
    SGPLOT = "SGPLOT"
    SGPANEL = "SGPANEL"
    GPLOT = "GPLOT"
    TRANSPOSE = "TRANSPOSE"
    APPEND = "APPEND"
    DATASETS = "DATASETS"
    FORMAT = "FORMAT"
    SUMMARY = "SUMMARY"
    TABULATE = "TABULATE"
    REPORT = "REPORT"
    HPLOGISTIC = "HPLOGISTIC"
    FOREST = "FOREST"
    UNKNOWN = "UNKNOWN"


@dataclass
class SourcePosition:
    """Tracks source location for traceability."""
    line: int = 0
    column: int = 0
    end_line: int = 0
    end_column: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "line": self.line,
            "column": self.column,
            "end_line": self.end_line,
            "end_column": self.end_column,
        }


@dataclass
class SASNode:
    """
    Base AST node with navigation support.

    Provides:
    - Parent/child navigation
    - AST traversal (walk)
    - Source position tracking
    - Serialization support
    """
    parent: Optional['SASNode'] = field(default=None, repr=False)
    position: Optional[SourcePosition] = None

    def walk(self):
        """Traverse all descendants depth-first."""
        yield self
        for child in self.children:
            yield from child.walk()

    def walk_breadth_first(self):
        """Traverse all descendants breadth-first."""
        queue = [self]
        while queue:
            node = queue.pop(0)
            yield node
            queue.extend(node.children)

    @property
    def children(self) -> List['SASNode']:
        """Override in subclasses to return child nodes."""
        return []

    def find_all(self, node_type: type) -> List['SASNode']:
        """Find all descendant nodes of a specific type."""
        return [node for node in self.walk() if isinstance(node, node_type)]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize node to dictionary."""
        return {
            "type": self.__class__.__name__,
            "position": self.position.to_dict() if self.position else None,
        }


@dataclass
class SourceFile(SASNode):
    """
    Root AST node representing a .sas file.
    Contains all top-level statements.
    """
    name: str = ""
    file_path: str = ""
    statements: List['SASNode'] = field(default_factory=list)

    @property
    def children(self) -> List[SASNode]:
        return self.statements

    @property
    def data_steps(self) -> List['DataStep']:
        return [s for s in self.statements if isinstance(s, DataStep)]

    @property
    def proc_steps(self) -> List['ProcStep']:
        return [s for s in self.statements if isinstance(s, ProcStep)]

    @property
    def macros(self) -> List['MacroDefinition']:
        return [s for s in self.statements if isinstance(s, MacroDefinition)]

    @property
    def libnames(self) -> List['LibnameStatement']:
        return [s for s in self.statements if isinstance(s, LibnameStatement)]

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "name": self.name,
            "file_path": self.file_path,
            "statements": [s.to_dict() for s in self.statements],
            "statement_count": len(self.statements),
        })
        return base


@dataclass
class LibnameStatement(SASNode):
    """
    LIBNAME statement - defines a library reference.
    Example: libname mylib "/path/to/data";
    """
    libref: str = ""
    path: str = ""
    engine: str = "BASE"  # BASE, ORACLE, ODBC, etc.
    options: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "libref": self.libref,
            "path": self.path,
            "engine": self.engine,
            "options": self.options,
        })
        return base


@dataclass
class FilenameStatement(SASNode):
    """
    FILENAME statement - defines a file reference.
    Example: filename myfile "/path/to/file.csv";
    """
    fileref: str = ""
    path: str = ""
    options: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "fileref": self.fileref,
            "path": self.path,
            "options": self.options,
        })
        return base


@dataclass
class SetStatement(SASNode):
    """SET statement within DATA step."""
    datasets: List[str] = field(default_factory=list)
    options: Dict[str, str] = field(default_factory=dict)  # e.g., in=, end=

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "datasets": self.datasets,
            "options": self.options,
        })
        return base


@dataclass
class MergeStatement(SASNode):
    """MERGE statement within DATA step."""
    datasets: List[str] = field(default_factory=list)
    by_variables: List[str] = field(default_factory=list)
    options: Dict[str, str] = field(default_factory=dict)  # e.g., in=

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "datasets": self.datasets,
            "by_variables": self.by_variables,
            "options": self.options,
        })
        return base


@dataclass
class Assignment(SASNode):
    """Variable assignment statement."""
    variable: str = ""
    expression: str = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "variable": self.variable,
            "expression": self.expression,
        })
        return base


@dataclass
class IfStatement(SASNode):
    """IF/THEN/ELSE statement."""
    condition: str = ""
    then_statements: List['SASNode'] = field(default_factory=list)
    else_statements: List['SASNode'] = field(default_factory=list)

    @property
    def children(self) -> List[SASNode]:
        return self.then_statements + self.else_statements

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "condition": self.condition,
            "then_statements": [s.to_dict() for s in self.then_statements],
            "else_statements": [s.to_dict() for s in self.else_statements],
        })
        return base


@dataclass
class DoLoop(SASNode):
    """DO loop (DO, DO WHILE, DO UNTIL, iterative DO)."""
    loop_type: str = "DO"  # DO, WHILE, UNTIL, ITERATIVE
    condition: str = ""
    iterator_var: str = ""
    start_value: str = ""
    end_value: str = ""
    by_value: str = ""
    statements: List['SASNode'] = field(default_factory=list)

    @property
    def children(self) -> List[SASNode]:
        return self.statements

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "loop_type": self.loop_type,
            "condition": self.condition,
            "iterator_var": self.iterator_var,
            "start_value": self.start_value,
            "end_value": self.end_value,
            "by_value": self.by_value,
            "statements": [s.to_dict() for s in self.statements],
        })
        return base


@dataclass
class OutputStatement(SASNode):
    """OUTPUT statement in DATA step."""
    dataset: str = ""  # Optional specific dataset

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({"dataset": self.dataset})
        return base


@dataclass
class DataStep(SASNode):
    """
    DATA step - creates or modifies datasets.
    Example: data work.output; set lib.input; run;
    """
    output_datasets: List[str] = field(default_factory=list)
    options: Dict[str, str] = field(default_factory=dict)  # e.g., drop=, keep=, rename=
    statements: List['SASNode'] = field(default_factory=list)
    raw_text: str = ""  # Original source for reference

    @property
    def children(self) -> List[SASNode]:
        return self.statements

    @property
    def input_datasets(self) -> List[str]:
        """Extract input datasets from SET and MERGE statements."""
        datasets = []
        for stmt in self.statements:
            if isinstance(stmt, SetStatement):
                datasets.extend(stmt.datasets)
            elif isinstance(stmt, MergeStatement):
                datasets.extend(stmt.datasets)
        return datasets

    @property
    def set_statements(self) -> List[SetStatement]:
        return [s for s in self.statements if isinstance(s, SetStatement)]

    @property
    def merge_statements(self) -> List[MergeStatement]:
        return [s for s in self.statements if isinstance(s, MergeStatement)]

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "output_datasets": self.output_datasets,
            "input_datasets": self.input_datasets,
            "options": self.options,
            "statements": [s.to_dict() for s in self.statements],
        })
        return base


@dataclass
class ProcStep(SASNode):
    """
    PROC step - invokes a SAS procedure.
    Example: proc sort data=mydata; by var1; run;
    """
    proc_type: ProcType = ProcType.UNKNOWN
    input_datasets: List[str] = field(default_factory=list)
    output_datasets: List[str] = field(default_factory=list)
    options: Dict[str, str] = field(default_factory=dict)
    statements: List['SASNode'] = field(default_factory=list)
    raw_text: str = ""

    @property
    def children(self) -> List[SASNode]:
        return self.statements

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "proc_type": self.proc_type.value,
            "input_datasets": self.input_datasets,
            "output_datasets": self.output_datasets,
            "options": self.options,
            "statements": [s.to_dict() for s in self.statements],
        })
        return base


@dataclass
class ProcSQL(ProcStep):
    """
    PROC SQL - special handling for SQL procedure.
    Captures CREATE TABLE, SELECT, INSERT, UPDATE, DELETE statements.
    """
    sql_statements: List[str] = field(default_factory=list)
    created_tables: List[str] = field(default_factory=list)
    source_tables: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.proc_type = ProcType.SQL

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "sql_statements": self.sql_statements,
            "created_tables": self.created_tables,
            "source_tables": self.source_tables,
        })
        return base


@dataclass
class MacroVariable(SASNode):
    """
    Macro variable definition.
    Example: %let var = value;
    """
    name: str = ""
    value: str = ""
    is_global: bool = False

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "name": self.name,
            "value": self.value,
            "is_global": self.is_global,
        })
        return base


@dataclass
class MacroCall(SASNode):
    """
    Macro invocation.
    Example: %mymacro(param1, param2)
    """
    name: str = ""
    arguments: List[str] = field(default_factory=list)
    named_arguments: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "name": self.name,
            "arguments": self.arguments,
            "named_arguments": self.named_arguments,
        })
        return base


@dataclass
class UnparsedContent(SASNode):
    """
    Content that couldn't be fully parsed.
    Used for macro bodies and complex constructs.
    """
    content: str = ""
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "content": self.content[:200] + "..." if len(self.content) > 200 else self.content,
            "reason": self.reason,
        })
        return base


@dataclass
class MacroDefinition(SASNode):
    """
    Macro definition with lazy body parsing.

    Implements Tomassetti's two-pass approach:
    - Pass 1: Capture macro structure and body as raw tokens
    - Pass 2: Parse body only when accessed (lazy evaluation)

    Example:
        %macro mymacro(param1, param2);
            ... macro body ...
        %mend;
    """
    name: str = ""
    parameters: List[str] = field(default_factory=list)
    default_values: Dict[str, str] = field(default_factory=dict)
    _body_raw: str = ""  # Raw body text (Pass 1)
    _body_parsed: Optional[List[SASNode]] = field(default=None, repr=False)
    _parser_callback: Any = field(default=None, repr=False)  # Callback for lazy parsing

    @cached_property
    def body(self) -> List[SASNode]:
        """
        Lazy parsing of macro body - only executed when accessed.

        This implements the Tomassetti approach where macro bodies
        are parsed on-demand to improve performance.
        """
        if self._body_parsed is not None:
            return self._body_parsed

        if self._parser_callback is not None:
            try:
                self._body_parsed = self._parser_callback(self._body_raw)
                logger.debug(f"Lazy-parsed macro body for {self.name}: {len(self._body_parsed)} statements")
            except Exception as e:
                logger.warning(f"Failed to parse macro body for {self.name}: {e}")
                self._body_parsed = [UnparsedContent(content=self._body_raw, reason=str(e))]
        else:
            # No parser callback - return unparsed content
            self._body_parsed = [UnparsedContent(content=self._body_raw, reason="No parser available")]

        return self._body_parsed

    @property
    def children(self) -> List[SASNode]:
        # Only return parsed body if it has been accessed
        if self._body_parsed is not None:
            return self._body_parsed
        return []

    @property
    def has_parsed_body(self) -> bool:
        """Check if body has been parsed without triggering lazy evaluation."""
        return self._body_parsed is not None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "name": self.name,
            "parameters": self.parameters,
            "default_values": self.default_values,
            "body_parsed": self._body_parsed is not None,
            "body_raw_length": len(self._body_raw),
        })
        if self._body_parsed:
            base["body"] = [s.to_dict() for s in self._body_parsed]
        return base


@dataclass
class GlobalStatement(SASNode):
    """
    Global SAS statements like OPTIONS, TITLE, FOOTNOTE.
    """
    statement_type: str = ""  # OPTIONS, TITLE, FOOTNOTE, etc.
    content: str = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "statement_type": self.statement_type,
            "content": self.content,
        })
        return base


@dataclass
class CommentBlock(SASNode):
    """
    Comment block in SAS code.
    Useful for documentation extraction.
    """
    text: str = ""
    is_block_comment: bool = True  # /* */ vs * ;

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "text": self.text,
            "is_block_comment": self.is_block_comment,
        })
        return base
