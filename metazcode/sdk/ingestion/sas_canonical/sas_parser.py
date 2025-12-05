"""
SAS Canonical Parser - Tomassetti Two-Pass Approach

This parser implements the Strumenta/Tomassetti methodology for parsing SAS code:
- Pass 1: Structure extraction (DATA steps, PROCs, macros as raw content)
- Pass 2: Lazy parsing of macro bodies when accessed

The parser uses regex-based tokenization for Pass 1 (pragmatic approach recommended
by Tomassetti for legacy languages) and defers complex macro body parsing.

References:
- https://tomassetti.me/how-to-use-the-sas-parser/
- https://tomassetti.me/challenges-in-parsing-legacy-languages-sas-macros/

Key Features:
- Handles LIBNAME, FILENAME statements
- Parses DATA steps with SET, MERGE, assignments
- Parses all major PROC types (SQL, SORT, IMPORT, EXPORT, MEANS, etc.)
- Extracts macro definitions with lazy body parsing
- Extracts macro variable definitions (%LET)
- Tracks macro calls and variable references
- Maintains source position for traceability
"""

import re
import logging
from typing import Dict, List, Tuple, Generator, Any, Optional
from pathlib import Path

from ...models.canonical_types import NodeType, EdgeType
from ...models.graph import Node, Edge
from ...models.traceability import SourceContext

from .ast.nodes import (
    SASNode,
    SourceFile,
    LibnameStatement,
    FilenameStatement,
    DataStep,
    ProcStep,
    ProcSQL,
    ProcType,
    MacroDefinition,
    MacroCall,
    MacroVariable,
    SetStatement,
    MergeStatement,
    Assignment,
    IfStatement,
    DoLoop,
    OutputStatement,
    GlobalStatement,
    CommentBlock,
    UnparsedContent,
    SourcePosition,
)

logger = logging.getLogger(__name__)


class CanonicalSasParser:
    """
    Two-pass SAS parser following the Tomassetti/Strumenta approach.

    Pass 1: Extract program structure using regex patterns
        - Identify LIBNAME, DATA, PROC, MACRO blocks
        - Capture macro bodies as raw text (not parsed)
        - Build initial AST with lazy macro nodes

    Pass 2: Lazy parsing of macro bodies
        - Macro bodies are parsed only when accessed
        - Uses cached_property for on-demand evaluation
        - Handles parsing failures gracefully
    """

    # ==========================================================================
    # REGEX PATTERNS - Pass 1 Structure Extraction
    # ==========================================================================

    # Remove comments for parsing (but preserve for documentation)
    BLOCK_COMMENT_RE = re.compile(r'/\*.*?\*/', re.DOTALL)
    LINE_COMMENT_RE = re.compile(r'^\s*\*[^;]*;', re.MULTILINE)

    # LIBNAME statement
    LIBNAME_RE = re.compile(
        r'\blibname\s+(\w+)\s+(?:(\w+)\s+)?["\']([^"\']+)["\']([^;]*);',
        re.IGNORECASE | re.DOTALL
    )

    # FILENAME statement
    FILENAME_RE = re.compile(
        r'\bfilename\s+(\w+)\s+["\']([^"\']+)["\']([^;]*);',
        re.IGNORECASE | re.DOTALL
    )

    # DATA step - captures until RUN;
    DATA_STEP_RE = re.compile(
        r'\bdata\s+([^;]+);(.*?)\brun\s*;',
        re.IGNORECASE | re.DOTALL
    )

    # PROC step - captures until RUN; or QUIT;
    PROC_RE = re.compile(
        r'\bproc\s+(\w+)([^;]*);(.*?)(?:\brun\s*;|\bquit\s*;)',
        re.IGNORECASE | re.DOTALL
    )

    # Macro definition - captures %MACRO to %MEND
    MACRO_DEF_RE = re.compile(
        r'%macro\s+(\w+)\s*(?:\(([^)]*)\))?\s*;(.*?)%mend(?:\s+\w+)?\s*;',
        re.IGNORECASE | re.DOTALL
    )

    # Macro variable (%LET)
    MACRO_VAR_RE = re.compile(
        r'%let\s+(\w+)\s*=\s*([^;]*);',
        re.IGNORECASE
    )

    # Macro call
    MACRO_CALL_RE = re.compile(
        r'%(\w+)\s*(?:\(([^)]*)\))?',
        re.IGNORECASE
    )

    # Global statements
    GLOBAL_STMT_RE = re.compile(
        r'\b(options|title\d?|footnote\d?)\s+([^;]*);',
        re.IGNORECASE
    )

    # ==========================================================================
    # DATA STEP INTERNAL PATTERNS
    # ==========================================================================

    SET_RE = re.compile(
        r'\bset\s+([^;]+);',
        re.IGNORECASE | re.DOTALL
    )

    MERGE_RE = re.compile(
        r'\bmerge\s+([^;]+);',
        re.IGNORECASE | re.DOTALL
    )

    BY_RE = re.compile(
        r'\bby\s+([^;]+);',
        re.IGNORECASE
    )

    ASSIGNMENT_RE = re.compile(
        r'\b(\w+)\s*=\s*([^;]+);',
        re.IGNORECASE
    )

    IF_THEN_RE = re.compile(
        r'\bif\s+(.+?)\s+then\s+',
        re.IGNORECASE | re.DOTALL
    )

    OUTPUT_RE = re.compile(
        r'\boutput\s*(?:(\w+(?:\.\w+)?))?\s*;',
        re.IGNORECASE
    )

    DROP_RE = re.compile(r'\bdrop\s+([^;]+);', re.IGNORECASE)
    KEEP_RE = re.compile(r'\bkeep\s+([^;]+);', re.IGNORECASE)
    RENAME_RE = re.compile(r'\brename\s*=\s*\(([^)]+)\)', re.IGNORECASE)

    # ==========================================================================
    # PROC SQL PATTERNS
    # ==========================================================================

    SQL_CREATE_TABLE_RE = re.compile(
        r'\bcreate\s+table\s+([\w.]+)\s+as\s+',
        re.IGNORECASE
    )

    SQL_SELECT_FROM_RE = re.compile(
        r'\bfrom\s+([\w.]+)',
        re.IGNORECASE
    )

    SQL_SELECT_INTO_RE = re.compile(
        r'\bselect\s+.*?\binto\s*:\s*(\w+)',
        re.IGNORECASE | re.DOTALL
    )

    SQL_INSERT_INTO_RE = re.compile(
        r'\binsert\s+into\s+([\w.]+)',
        re.IGNORECASE
    )

    SQL_UPDATE_RE = re.compile(
        r'\bupdate\s+([\w.]+)',
        re.IGNORECASE
    )

    SQL_DELETE_FROM_RE = re.compile(
        r'\bdelete\s+from\s+([\w.]+)',
        re.IGNORECASE
    )

    # ==========================================================================
    # PROC OPTIONS PATTERNS
    # ==========================================================================

    PROC_DATA_OPT_RE = re.compile(r'\bdata\s*=\s*([\w.]+)', re.IGNORECASE)
    PROC_OUT_OPT_RE = re.compile(r'\bout\s*=\s*([\w.]+)', re.IGNORECASE)
    PROC_OUTFILE_RE = re.compile(r'\boutfile\s*=\s*["\']?([^"\';\s]+)', re.IGNORECASE)
    PROC_DATAFILE_RE = re.compile(r'\bdatafile\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)

    def __init__(
        self,
        connections_context: Optional[Dict[str, Dict[str, Any]]] = None,
        parameters_context: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        """
        Initialize SAS parser.

        Args:
            connections_context: Optional context for library references
            parameters_context: Optional context for macro variables
        """
        self.connections_context = connections_context or {}
        self.parameters_context = parameters_context or {}
        self._current_file = ""
        self._macro_var_context: Dict[str, str] = {}

    def parse(
        self, file_path: str
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Parse a SAS file and yield knowledge graph nodes/edges.

        This is the main entry point implementing the two-pass approach:
        - Pass 1: Build AST with lazy macro handling
        - Pass 2: Convert AST to knowledge graph

        Args:
            file_path: Path to the .sas file

        Yields:
            Tuples of (nodes, edges) for the knowledge graph
        """
        self._current_file = file_path

        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return

        # Remove BOM if present
        if content.startswith("\ufeff"):
            content = content[1:]

        # =======================================================================
        # PASS 1: Build AST with structure extraction
        # =======================================================================
        logger.info(f"Pass 1: Extracting structure from {Path(file_path).name}")
        ast = self._build_ast(content, file_path)

        # =======================================================================
        # PASS 2: Convert AST to Knowledge Graph
        # =======================================================================
        logger.info(f"Pass 2: Building knowledge graph from AST")
        nodes, edges = self._build_graph(ast, file_path)

        yield nodes, edges

    def _build_ast(self, content: str, file_path: str) -> SourceFile:
        """
        Pass 1: Build AST from SAS source code.

        Extracts:
        - LIBNAME/FILENAME statements
        - DATA steps with internal structure
        - PROC steps with options
        - Macro definitions (bodies stored as raw text)
        - Macro variables (%LET)
        - Global statements (OPTIONS, TITLE, etc.)
        """
        file_name = Path(file_path).name
        statements: List[SASNode] = []

        # Preserve original for position tracking
        original_content = content

        # Extract comments first (for documentation)
        comments = self._extract_comments(content)

        # Remove comments for structural parsing
        content_no_comments = self.BLOCK_COMMENT_RE.sub(' ', content)
        content_no_comments = self.LINE_COMMENT_RE.sub(' ', content_no_comments)

        # Extract macro definitions FIRST (they can contain other constructs)
        macros, content_no_macros = self._extract_macros(content_no_comments)
        statements.extend(macros)

        # Extract macro variables (%LET)
        macro_vars = self._extract_macro_variables(content_no_macros)
        statements.extend(macro_vars)

        # Update macro variable context
        for mv in macro_vars:
            self._macro_var_context[mv.name.lower()] = mv.value

        # Extract LIBNAME statements
        libnames = self._extract_libnames(content_no_macros)
        statements.extend(libnames)

        # Extract FILENAME statements
        filenames = self._extract_filenames(content_no_macros)
        statements.extend(filenames)

        # Extract DATA steps
        data_steps = self._extract_data_steps(content_no_macros)
        statements.extend(data_steps)

        # Extract PROC steps
        proc_steps = self._extract_proc_steps(content_no_macros)
        statements.extend(proc_steps)

        # Extract global statements
        global_stmts = self._extract_global_statements(content_no_macros)
        statements.extend(global_stmts)

        # Create source file AST node
        source_file = SourceFile(
            name=file_name,
            file_path=file_path,
            statements=statements,
        )

        logger.info(
            f"AST built: {len(data_steps)} DATA steps, {len(proc_steps)} PROCs, "
            f"{len(macros)} macros, {len(libnames)} LIBNAMEs"
        )

        return source_file

    def _extract_comments(self, content: str) -> List[CommentBlock]:
        """Extract comment blocks for documentation purposes."""
        comments = []

        # Block comments /* */
        for match in self.BLOCK_COMMENT_RE.finditer(content):
            comments.append(CommentBlock(
                text=match.group(0),
                is_block_comment=True,
            ))

        return comments

    def _extract_macros(self, content: str) -> Tuple[List[MacroDefinition], str]:
        """
        Extract macro definitions with lazy body parsing.

        The macro body is stored as raw text and only parsed when accessed.
        This implements the Tomassetti lazy evaluation pattern.
        """
        macros = []
        remaining_content = content

        for match in self.MACRO_DEF_RE.finditer(content):
            name = match.group(1)
            params_str = match.group(2) or ""
            body_raw = match.group(3)

            # Parse parameters
            parameters = []
            default_values = {}
            if params_str.strip():
                for param in params_str.split(','):
                    param = param.strip()
                    if '=' in param:
                        param_name, default = param.split('=', 1)
                        parameters.append(param_name.strip())
                        default_values[param_name.strip()] = default.strip()
                    else:
                        parameters.append(param)

            # Create macro with lazy body parsing
            macro = MacroDefinition(
                name=name,
                parameters=parameters,
                default_values=default_values,
                _body_raw=body_raw,
                _parser_callback=self._parse_macro_body,
            )
            macros.append(macro)

            # Remove macro from content to avoid double-parsing
            remaining_content = remaining_content.replace(match.group(0), ' ')

        return macros, remaining_content

    def _parse_macro_body(self, body_raw: str) -> List[SASNode]:
        """
        Pass 2 callback: Parse macro body content.

        This is called lazily when macro.body is accessed.
        Implements best-effort parsing as recommended by Tomassetti.
        """
        statements = []

        try:
            # Try to parse DATA steps in macro body
            data_steps = self._extract_data_steps(body_raw)
            statements.extend(data_steps)

            # Try to parse PROC steps in macro body
            proc_steps = self._extract_proc_steps(body_raw)
            statements.extend(proc_steps)

            # Try to parse macro calls in macro body
            macro_calls = self._extract_macro_calls(body_raw)
            statements.extend(macro_calls)

            # Extract macro variables in macro body
            macro_vars = self._extract_macro_variables(body_raw)
            statements.extend(macro_vars)

            if not statements:
                # If nothing was parsed, return as unparsed content
                statements.append(UnparsedContent(
                    content=body_raw,
                    reason="No parseable statements found"
                ))

        except Exception as e:
            logger.warning(f"Failed to parse macro body: {e}")
            statements.append(UnparsedContent(
                content=body_raw,
                reason=str(e)
            ))

        return statements

    def _extract_macro_calls(self, content: str) -> List[MacroCall]:
        """Extract macro invocations."""
        calls = []

        # Skip built-in macro functions
        builtin_macros = {
            'let', 'put', 'if', 'then', 'else', 'do', 'end', 'mend', 'macro',
            'eval', 'scan', 'substr', 'length', 'index', 'upcase', 'lowcase',
            'sysfunc', 'sysevalf', 'str', 'nrstr', 'quote', 'nrquote',
            'global', 'local', 'include', 'while', 'until'
        }

        for match in self.MACRO_CALL_RE.finditer(content):
            name = match.group(1)
            args_str = match.group(2) or ""

            if name.lower() in builtin_macros:
                continue

            arguments = []
            named_arguments = {}

            if args_str.strip():
                for arg in args_str.split(','):
                    arg = arg.strip()
                    if '=' in arg:
                        key, value = arg.split('=', 1)
                        named_arguments[key.strip()] = value.strip()
                    else:
                        arguments.append(arg)

            calls.append(MacroCall(
                name=name,
                arguments=arguments,
                named_arguments=named_arguments,
            ))

        return calls

    def _extract_macro_variables(self, content: str) -> List[MacroVariable]:
        """Extract %LET statements."""
        macro_vars = []

        for match in self.MACRO_VAR_RE.finditer(content):
            name = match.group(1)
            value = match.group(2).strip()

            macro_vars.append(MacroVariable(
                name=name,
                value=value,
            ))

        return macro_vars

    def _extract_libnames(self, content: str) -> List[LibnameStatement]:
        """Extract LIBNAME statements."""
        libnames = []

        for match in self.LIBNAME_RE.finditer(content):
            libref = match.group(1)
            engine = match.group(2) or "BASE"
            path = match.group(3)
            options_str = match.group(4) or ""

            libnames.append(LibnameStatement(
                libref=libref,
                path=path,
                engine=engine.upper() if engine else "BASE",
                options=self._parse_options(options_str),
            ))

        return libnames

    def _extract_filenames(self, content: str) -> List[FilenameStatement]:
        """Extract FILENAME statements."""
        filenames = []

        for match in self.FILENAME_RE.finditer(content):
            fileref = match.group(1)
            path = match.group(2)
            options_str = match.group(3) or ""

            filenames.append(FilenameStatement(
                fileref=fileref,
                path=path,
                options=self._parse_options(options_str),
            ))

        return filenames

    def _extract_data_steps(self, content: str) -> List[DataStep]:
        """Extract DATA steps with internal structure."""
        data_steps = []

        for match in self.DATA_STEP_RE.finditer(content):
            output_spec = match.group(1).strip()
            body = match.group(2)

            # Parse output datasets and options
            output_datasets, options = self._parse_data_output_spec(output_spec)

            # Parse internal statements
            statements = self._parse_data_step_body(body)

            data_steps.append(DataStep(
                output_datasets=output_datasets,
                options=options,
                statements=statements,
                raw_text=match.group(0),
            ))

        return data_steps

    def _parse_data_output_spec(self, spec: str) -> Tuple[List[str], Dict[str, str]]:
        """Parse DATA step output specification."""
        datasets = []
        options = {}

        # Handle options in parentheses: data out(drop=x);
        parts = re.split(r'\s+', spec)
        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Check for options in parentheses
            opt_match = re.match(r'(\w+(?:\.\w+)?)\s*\(([^)]+)\)', part)
            if opt_match:
                datasets.append(opt_match.group(1))
                # Parse options
                opt_str = opt_match.group(2)
                for opt in re.findall(r'(\w+)\s*=\s*([^,\s)]+)', opt_str):
                    options[opt[0]] = opt[1]
            elif re.match(r'^[\w.]+$', part):
                datasets.append(part)

        return datasets, options

    def _parse_data_step_body(self, body: str) -> List[SASNode]:
        """Parse internal statements of a DATA step."""
        statements = []

        # Extract SET statements
        for match in self.SET_RE.finditer(body):
            datasets_str = match.group(1)
            datasets, options = self._parse_dataset_list(datasets_str)
            statements.append(SetStatement(
                datasets=datasets,
                options=options,
            ))

        # Extract MERGE statements
        for match in self.MERGE_RE.finditer(body):
            datasets_str = match.group(1)
            datasets, options = self._parse_dataset_list(datasets_str)

            # Look for BY statement nearby
            by_match = self.BY_RE.search(body)
            by_vars = []
            if by_match:
                by_vars = [v.strip() for v in by_match.group(1).split()]

            statements.append(MergeStatement(
                datasets=datasets,
                by_variables=by_vars,
                options=options,
            ))

        # Extract OUTPUT statements
        for match in self.OUTPUT_RE.finditer(body):
            dataset = match.group(1) or ""
            statements.append(OutputStatement(dataset=dataset))

        return statements

    def _parse_dataset_list(self, spec: str) -> Tuple[List[str], Dict[str, str]]:
        """Parse a list of datasets with options."""
        datasets = []
        options = {}

        # Split by whitespace but handle parentheses
        current = ""
        paren_depth = 0
        for char in spec + " ":
            if char == '(':
                paren_depth += 1
                current += char
            elif char == ')':
                paren_depth -= 1
                current += char
            elif char.isspace() and paren_depth == 0:
                if current.strip():
                    # Parse this token
                    opt_match = re.match(r'(\w+(?:\.\w+)?)\s*\(([^)]+)\)', current)
                    if opt_match:
                        datasets.append(opt_match.group(1))
                        for opt in re.findall(r'(\w+)\s*=\s*(\w+)', opt_match.group(2)):
                            options[opt[0]] = opt[1]
                    elif re.match(r'^[\w.]+$', current):
                        datasets.append(current)
                current = ""
            else:
                current += char

        return datasets, options

    def _extract_proc_steps(self, content: str) -> List[ProcStep]:
        """Extract PROC steps."""
        proc_steps = []

        for match in self.PROC_RE.finditer(content):
            proc_name = match.group(1).upper()
            options_str = match.group(2)
            body = match.group(3)

            # Determine proc type
            try:
                proc_type = ProcType[proc_name]
            except KeyError:
                proc_type = ProcType.UNKNOWN
                logger.debug(f"Unknown PROC type: {proc_name}")

            # Parse input/output datasets from options
            input_datasets = []
            output_datasets = []
            options = self._parse_options(options_str)

            # DATA= option (input)
            data_match = self.PROC_DATA_OPT_RE.search(options_str)
            if data_match:
                input_datasets.append(data_match.group(1))

            # OUT= option (output)
            out_match = self.PROC_OUT_OPT_RE.search(options_str)
            if out_match:
                output_datasets.append(out_match.group(1))

            # Special handling for PROC SQL
            if proc_type == ProcType.SQL:
                proc_step = self._parse_proc_sql(body, options)
            # Special handling for PROC IMPORT
            elif proc_type == ProcType.IMPORT:
                proc_step = self._parse_proc_import(options_str, body)
            # Special handling for PROC EXPORT
            elif proc_type == ProcType.EXPORT:
                proc_step = self._parse_proc_export(options_str, body)
            else:
                proc_step = ProcStep(
                    proc_type=proc_type,
                    input_datasets=input_datasets,
                    output_datasets=output_datasets,
                    options=options,
                    raw_text=match.group(0),
                )

            proc_steps.append(proc_step)

        return proc_steps

    def _parse_proc_sql(self, body: str, options: Dict[str, str]) -> ProcSQL:
        """Parse PROC SQL specifically for data lineage."""
        created_tables = []
        source_tables = []
        sql_statements = []

        # Split by semicolon to get individual statements
        stmts = re.split(r';', body)
        for stmt in stmts:
            stmt = stmt.strip()
            if not stmt:
                continue

            sql_statements.append(stmt)

            # CREATE TABLE
            create_match = self.SQL_CREATE_TABLE_RE.search(stmt)
            if create_match:
                created_tables.append(create_match.group(1))

            # FROM clause (source tables)
            for from_match in self.SQL_SELECT_FROM_RE.finditer(stmt):
                table = from_match.group(1)
                if table.lower() not in ('dual', 'dictionary'):
                    source_tables.append(table)

            # INSERT INTO
            insert_match = self.SQL_INSERT_INTO_RE.search(stmt)
            if insert_match:
                created_tables.append(insert_match.group(1))

            # UPDATE
            update_match = self.SQL_UPDATE_RE.search(stmt)
            if update_match:
                created_tables.append(update_match.group(1))

        return ProcSQL(
            options=options,
            sql_statements=sql_statements,
            created_tables=list(set(created_tables)),
            source_tables=list(set(source_tables)),
            input_datasets=list(set(source_tables)),
            output_datasets=list(set(created_tables)),
        )

    def _parse_proc_import(self, options_str: str, body: str) -> ProcStep:
        """Parse PROC IMPORT for file imports."""
        options = self._parse_options(options_str)
        output_datasets = []

        # OUT= dataset
        out_match = self.PROC_OUT_OPT_RE.search(options_str)
        if out_match:
            output_datasets.append(out_match.group(1))

        # DATAFILE= source
        datafile_match = self.PROC_DATAFILE_RE.search(options_str)
        source_file = datafile_match.group(1) if datafile_match else ""

        options['source_file'] = source_file

        return ProcStep(
            proc_type=ProcType.IMPORT,
            input_datasets=[],  # External file, not a dataset
            output_datasets=output_datasets,
            options=options,
        )

    def _parse_proc_export(self, options_str: str, body: str) -> ProcStep:
        """Parse PROC EXPORT for file exports."""
        options = self._parse_options(options_str)
        input_datasets = []

        # DATA= dataset
        data_match = self.PROC_DATA_OPT_RE.search(options_str)
        if data_match:
            input_datasets.append(data_match.group(1))

        # OUTFILE= destination
        outfile_match = self.PROC_OUTFILE_RE.search(options_str)
        dest_file = outfile_match.group(1) if outfile_match else ""

        options['dest_file'] = dest_file

        return ProcStep(
            proc_type=ProcType.EXPORT,
            input_datasets=input_datasets,
            output_datasets=[],  # External file, not a dataset
            options=options,
        )

    def _extract_global_statements(self, content: str) -> List[GlobalStatement]:
        """Extract global statements like OPTIONS, TITLE, FOOTNOTE."""
        global_stmts = []

        for match in self.GLOBAL_STMT_RE.finditer(content):
            stmt_type = match.group(1).upper()
            stmt_content = match.group(2).strip()

            global_stmts.append(GlobalStatement(
                statement_type=stmt_type,
                content=stmt_content,
            ))

        return global_stmts

    def _parse_options(self, options_str: str) -> Dict[str, str]:
        """Parse option string into dictionary."""
        options = {}

        # Pattern: key=value or key="value" or just key
        for match in re.finditer(r'(\w+)\s*=\s*(?:["\']([^"\']+)["\']|([\w.]+))', options_str):
            key = match.group(1)
            value = match.group(2) or match.group(3) or ""
            options[key.lower()] = value

        return options

    # ==========================================================================
    # GRAPH BUILDING - Convert AST to Knowledge Graph
    # ==========================================================================

    def _build_graph(
        self, ast: SourceFile, file_path: str
    ) -> Tuple[List[Node], List[Edge]]:
        """
        Convert AST to knowledge graph nodes and edges.

        Creates:
        - PIPELINE node for the .sas file
        - OPERATION nodes for DATA steps and PROCs
        - MACRO nodes for macro definitions
        - TABLE nodes for datasets
        - CONNECTION nodes for LIBNAMEs
        - VARIABLE nodes for macro variables
        - Appropriate edges for relationships
        """
        nodes: List[Node] = []
        edges: List[Edge] = []

        # Create PIPELINE node
        pipeline_id = f"pipeline:{ast.name}"
        nodes.append(Node(
            node_id=pipeline_id,
            node_type=NodeType.PIPELINE,
            name=ast.name,
            properties={
                "technology": "SAS",
                "data_steps": len(ast.data_steps),
                "proc_steps": len(ast.proc_steps),
                "macros": len(ast.macros),
                **SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="sas",
                )
            }
        ))

        # Track created table nodes to avoid duplicates
        table_nodes: Dict[str, Node] = {}

        # Previous operation for PRECEDES edges
        prev_operation_id: Optional[str] = None

        # Process LIBNAME statements -> CONNECTION nodes
        for libname in ast.libnames:
            conn_id = f"connection:{libname.libref}"
            nodes.append(Node(
                node_id=conn_id,
                node_type=NodeType.CONNECTION,
                name=libname.libref,
                properties={
                    "path": libname.path,
                    "engine": libname.engine,
                    "options": libname.options,
                    "connection_type": "SAS_LIBRARY",
                }
            ))
            edges.append(Edge(
                source_id=pipeline_id,
                target_id=conn_id,
                relation=EdgeType.CONTAINS.value,
            ))

        # Process macro variables -> VARIABLE nodes
        for macro_var in [s for s in ast.statements if isinstance(s, MacroVariable)]:
            var_id = f"variable:{macro_var.name}"
            nodes.append(Node(
                node_id=var_id,
                node_type=NodeType.VARIABLE,
                name=macro_var.name,
                properties={
                    "value": macro_var.value,
                    "variable_type": "MACRO_VARIABLE",
                }
            ))
            edges.append(Edge(
                source_id=pipeline_id,
                target_id=var_id,
                relation=EdgeType.CONTAINS.value,
            ))

        # Process DATA steps -> OPERATION nodes
        for idx, data_step in enumerate(ast.data_steps):
            op_name = f"DATA {', '.join(data_step.output_datasets)}"
            op_id = f"{pipeline_id}:data_step:{idx}"

            nodes.append(Node(
                node_id=op_id,
                node_type=NodeType.OPERATION,
                name=op_name,
                properties={
                    "operation_subtype": "DATA_STEP",
                    "output_datasets": data_step.output_datasets,
                    "input_datasets": data_step.input_datasets,
                    "options": data_step.options,
                }
            ))

            # CONTAINS edge
            edges.append(Edge(
                source_id=pipeline_id,
                target_id=op_id,
                relation=EdgeType.CONTAINS.value,
            ))

            # PRECEDES edge
            if prev_operation_id:
                edges.append(Edge(
                    source_id=prev_operation_id,
                    target_id=op_id,
                    relation=EdgeType.PRECEDES.value,
                ))
            prev_operation_id = op_id

            # READS_FROM edges
            for dataset in data_step.input_datasets:
                table_id = self._get_or_create_table_node(
                    dataset, table_nodes, nodes
                )
                edges.append(Edge(
                    source_id=op_id,
                    target_id=table_id,
                    relation=EdgeType.READS_FROM.value,
                ))

            # WRITES_TO edges
            for dataset in data_step.output_datasets:
                table_id = self._get_or_create_table_node(
                    dataset, table_nodes, nodes
                )
                edges.append(Edge(
                    source_id=op_id,
                    target_id=table_id,
                    relation=EdgeType.WRITES_TO.value,
                ))

            # USES_CONNECTION edges (for library references)
            for dataset in data_step.input_datasets + data_step.output_datasets:
                if '.' in dataset:
                    libref = dataset.split('.')[0]
                    conn_id = f"connection:{libref}"
                    edges.append(Edge(
                        source_id=op_id,
                        target_id=conn_id,
                        relation=EdgeType.USES_CONNECTION.value,
                    ))

        # Process PROC steps -> OPERATION nodes
        for idx, proc_step in enumerate(ast.proc_steps):
            op_name = f"PROC {proc_step.proc_type.value}"
            if proc_step.input_datasets:
                op_name += f" ({proc_step.input_datasets[0]})"

            op_id = f"{pipeline_id}:proc:{proc_step.proc_type.value.lower()}:{idx}"

            properties = {
                "operation_subtype": f"PROC_{proc_step.proc_type.value}",
                "proc_type": proc_step.proc_type.value,
                "input_datasets": proc_step.input_datasets,
                "output_datasets": proc_step.output_datasets,
                "options": proc_step.options,
            }

            # Special handling for PROC SQL
            if isinstance(proc_step, ProcSQL):
                properties["sql_statements"] = proc_step.sql_statements
                properties["created_tables"] = proc_step.created_tables
                properties["source_tables"] = proc_step.source_tables

            nodes.append(Node(
                node_id=op_id,
                node_type=NodeType.OPERATION,
                name=op_name,
                properties=properties,
            ))

            # CONTAINS edge
            edges.append(Edge(
                source_id=pipeline_id,
                target_id=op_id,
                relation=EdgeType.CONTAINS.value,
            ))

            # PRECEDES edge
            if prev_operation_id:
                edges.append(Edge(
                    source_id=prev_operation_id,
                    target_id=op_id,
                    relation=EdgeType.PRECEDES.value,
                ))
            prev_operation_id = op_id

            # READS_FROM edges
            for dataset in proc_step.input_datasets:
                table_id = self._get_or_create_table_node(
                    dataset, table_nodes, nodes
                )
                edges.append(Edge(
                    source_id=op_id,
                    target_id=table_id,
                    relation=EdgeType.READS_FROM.value,
                ))

            # WRITES_TO edges
            for dataset in proc_step.output_datasets:
                table_id = self._get_or_create_table_node(
                    dataset, table_nodes, nodes
                )
                edges.append(Edge(
                    source_id=op_id,
                    target_id=table_id,
                    relation=EdgeType.WRITES_TO.value,
                ))

        # Process MACRO definitions -> MACRO nodes
        for macro in ast.macros:
            macro_id = f"{pipeline_id}:macro:{macro.name}"

            # Get datasets referenced in macro body (triggers lazy parsing)
            macro_inputs = []
            macro_outputs = []

            # Only parse body if we need to extract lineage
            # This triggers the lazy evaluation
            try:
                for stmt in macro.body:
                    if isinstance(stmt, DataStep):
                        macro_inputs.extend(stmt.input_datasets)
                        macro_outputs.extend(stmt.output_datasets)
                    elif isinstance(stmt, ProcStep):
                        macro_inputs.extend(stmt.input_datasets)
                        macro_outputs.extend(stmt.output_datasets)
            except Exception as e:
                logger.warning(f"Failed to analyze macro {macro.name}: {e}")

            nodes.append(Node(
                node_id=macro_id,
                node_type=NodeType.MACRO,
                name=macro.name,
                properties={
                    "parameters": macro.parameters,
                    "default_values": macro.default_values,
                    "body_parsed": macro.has_parsed_body,
                    "input_datasets": list(set(macro_inputs)),
                    "output_datasets": list(set(macro_outputs)),
                }
            ))

            # CONTAINS edge
            edges.append(Edge(
                source_id=pipeline_id,
                target_id=macro_id,
                relation=EdgeType.CONTAINS.value,
            ))

            # READS_FROM/WRITES_TO for macro datasets
            for dataset in set(macro_inputs):
                table_id = self._get_or_create_table_node(
                    dataset, table_nodes, nodes
                )
                edges.append(Edge(
                    source_id=macro_id,
                    target_id=table_id,
                    relation=EdgeType.READS_FROM.value,
                ))

            for dataset in set(macro_outputs):
                table_id = self._get_or_create_table_node(
                    dataset, table_nodes, nodes
                )
                edges.append(Edge(
                    source_id=macro_id,
                    target_id=table_id,
                    relation=EdgeType.WRITES_TO.value,
                ))

        # Process macro calls -> EXECUTES edges
        for stmt in ast.statements:
            if isinstance(stmt, MacroCall):
                macro_id = f"{pipeline_id}:macro:{stmt.name}"
                # Check if macro exists in this file
                if any(m.name == stmt.name for m in ast.macros):
                    edges.append(Edge(
                        source_id=pipeline_id,
                        target_id=macro_id,
                        relation=EdgeType.EXECUTES.value,
                    ))

        return nodes, edges

    def _get_or_create_table_node(
        self,
        dataset_name: str,
        table_nodes: Dict[str, Node],
        nodes: List[Node],
    ) -> str:
        """Get existing or create new TABLE node."""
        # Normalize dataset name
        dataset_name = dataset_name.strip().lower()
        table_id = f"table:{dataset_name}"

        if table_id not in table_nodes:
            # Parse library and table name
            if '.' in dataset_name:
                library, table_name = dataset_name.split('.', 1)
            else:
                library = "work"  # Default SAS library
                table_name = dataset_name

            table_node = Node(
                node_id=table_id,
                node_type=NodeType.TABLE,
                name=dataset_name,
                properties={
                    "library": library,
                    "table_name": table_name,
                    "qualified_name": dataset_name,
                }
            )
            table_nodes[table_id] = table_node
            nodes.append(table_node)

        return table_id
