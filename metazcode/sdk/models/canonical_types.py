from enum import Enum


class NodeType(str, Enum):
    """Canonical node types for technology-agnostic representation"""
    DIRECTORY = "directory"
    FILE = "file"
    PIPELINE = "pipeline"
    OPERATION = "operation"
    DATA_ASSET = "data_asset"
    CONNECTION = "connection"
    PARAMETER = "parameter"
    VARIABLE = "variable"
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    ENTITY = "entity"  # Fallback for unknown elements
    TRANSFORMATION = "transformation"

    # Macro type for code generation constructs
    # Added to support SAS macros and similar metaprogramming constructs in other
    # technologies (e.g., dbt macros, Jinja templates in Airflow, T-SQL dynamic SQL).
    # Macros are distinct from OPERATION because they:
    #   1. Generate code rather than execute data transformations directly
    #   2. Can produce different outputs based on parameters/runtime context
    #   3. Represent reusable code templates that expand into multiple operations
    #   4. Have their own dependency graph (macro calls macro)
    # Properties should include: macro_name, parameters, expansion_pattern, is_deterministic
    MACRO = "macro"

    # Phase 2: AI Enrichment Types
    OPERATION_SUMMARY = "operation_summary"
    PIPELINE_SUMMARY = "pipeline_summary"


class EdgeType(str, Enum):
    """Canonical relationship types for technology-agnostic representation"""

    CONTAINS = "contains"
    READS_FROM = "reads_from"
    WRITES_TO = "writes_to"
    USES_CONNECTION = "uses_connection"
    USES_PARAMETER = "uses_parameter"
    USES_VARIABLE = "uses_variable"
    # Phase 2: AI Enrichment Types
    SUMMARIZES = "summarizes"
    EXECUTES = "executes"
    DERIVED_FROM = "derived_from"
    TRANSFORMS = "transforms"
    PART_OF = "part_of"
    REFERENCES = "references"
    CONFIGURES = "configures"
    PRECEDES = "precedes"
    # Cross-Package Dependency Types
    DEPENDS_ON = "depends_on"
    SHARES_RESOURCE = "shares_resource"
    # SQL Semantic Types for Migration
    JOINS_WITH = "joins_with"
    ALIASES_AS = "aliases_as"
    # Event Handler Types for SSIS
    HANDLES_EVENT = "handles_event"