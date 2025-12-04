"""
SSIS Data Type Mapping Engine

This module provides comprehensive data type mapping capabilities for SSIS metadata extraction.
It handles conversion between SSIS native types and target platform types, providing
canonical type definitions and conversion rules.
"""

from typing import Dict, List, Optional, Any, Set
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CanonicalDataType(Enum):
    """Canonical data type definitions for cross-platform compatibility."""
    
    # Numeric types
    INTEGER = "INTEGER"
    BIGINT = "BIGINT" 
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    FLOAT = "FLOAT"
    REAL = "REAL"
    MONEY = "MONEY"
    
    # String types
    STRING = "STRING"
    VARCHAR = "VARCHAR"
    NVARCHAR = "NVARCHAR"
    CHAR = "CHAR"
    NCHAR = "NCHAR"
    TEXT = "TEXT"
    NTEXT = "NTEXT"
    
    # Date/Time types
    DATETIME = "DATETIME"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    
    # Binary types
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    IMAGE = "IMAGE"
    
    # Boolean type
    BOOLEAN = "BOOLEAN"
    
    # Special types
    GUID = "GUID"
    JSON = "JSON"
    XML = "XML"
    UNKNOWN = "UNKNOWN"


class ConversionRisk(Enum):
    """Risk levels for data type conversions."""
    NONE = "none"          # No conversion needed
    LOW = "low"            # Safe conversion with no data loss
    MEDIUM = "medium"      # Conversion with potential precision loss
    HIGH = "high"          # Conversion with potential data loss
    UNSAFE = "unsafe"      # Conversion likely to cause errors


class TargetPlatform(Enum):
    """Supported target platforms for type mapping."""
    SQL_SERVER = "sql_server"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    AZURE_SYNAPSE = "azure_synapse"
    DATABRICKS = "databricks"
    FABRIC = "fabric"


class SSISDataTypeMapper:
    """
    Maps SSIS data types to canonical types and target platform types.
    """
    
    def __init__(self):
        self._ssis_to_canonical = self._build_ssis_canonical_mapping()
        self._canonical_to_platforms = self._build_platform_mappings()
        self._conversion_rules = self._build_conversion_rules()
    
    def _build_ssis_canonical_mapping(self) -> Dict[str, CanonicalDataType]:
        """Build mapping from SSIS data types to canonical types."""
        return {
            # Numeric types - both DT_ prefixed and short forms
            "DT_I1": CanonicalDataType.TINYINT,
            "i1": CanonicalDataType.TINYINT,
            "DT_I2": CanonicalDataType.SMALLINT,
            "i2": CanonicalDataType.SMALLINT,
            "DT_I4": CanonicalDataType.INTEGER,
            "i4": CanonicalDataType.INTEGER,
            "DT_I8": CanonicalDataType.BIGINT,
            "i8": CanonicalDataType.BIGINT,
            "DT_UI1": CanonicalDataType.TINYINT,
            "ui1": CanonicalDataType.TINYINT,
            "DT_UI2": CanonicalDataType.SMALLINT,
            "ui2": CanonicalDataType.SMALLINT,
            "DT_UI4": CanonicalDataType.INTEGER,
            "ui4": CanonicalDataType.INTEGER,
            "DT_UI8": CanonicalDataType.BIGINT,
            "ui8": CanonicalDataType.BIGINT,
            "DT_R4": CanonicalDataType.REAL,
            "r4": CanonicalDataType.REAL,
            "DT_R8": CanonicalDataType.FLOAT,
            "r8": CanonicalDataType.FLOAT,
            "DT_DECIMAL": CanonicalDataType.DECIMAL,
            "decimal": CanonicalDataType.DECIMAL,
            "DT_NUMERIC": CanonicalDataType.NUMERIC,
            "numeric": CanonicalDataType.NUMERIC,
            "DT_CY": CanonicalDataType.MONEY,
            "cy": CanonicalDataType.MONEY,
            
            # String types - both DT_ prefixed and short forms
            "DT_STR": CanonicalDataType.VARCHAR,
            "str": CanonicalDataType.VARCHAR,
            "DT_WSTR": CanonicalDataType.NVARCHAR,
            "wstr": CanonicalDataType.NVARCHAR,
            "DT_TEXT": CanonicalDataType.TEXT,
            "text": CanonicalDataType.TEXT,
            "DT_NTEXT": CanonicalDataType.NTEXT,
            "ntext": CanonicalDataType.NTEXT,
            
            # Date/Time types - both DT_ prefixed and short forms
            "DT_DBTIMESTAMP": CanonicalDataType.DATETIME,
            "dbtimestamp": CanonicalDataType.DATETIME,
            "DT_DBTIMESTAMP2": CanonicalDataType.DATETIME,
            "dbtimestamp2": CanonicalDataType.DATETIME,
            "DT_DBDATE": CanonicalDataType.DATE,
            "dbdate": CanonicalDataType.DATE,
            "DT_DBTIME": CanonicalDataType.TIME,
            "dbtime": CanonicalDataType.TIME,
            "DT_DBTIME2": CanonicalDataType.TIME,
            "dbtime2": CanonicalDataType.TIME,
            "DT_DBTIMESTAMPOFFSET": CanonicalDataType.TIMESTAMP,
            "dbtimestampoffset": CanonicalDataType.TIMESTAMP,
            
            # Binary types - both DT_ prefixed and short forms
            "DT_BYTES": CanonicalDataType.VARBINARY,
            "bytes": CanonicalDataType.VARBINARY,
            "DT_IMAGE": CanonicalDataType.IMAGE,
            "image": CanonicalDataType.IMAGE,
            
            # Boolean type - both DT_ prefixed and short forms
            "DT_BOOL": CanonicalDataType.BOOLEAN,
            "bool": CanonicalDataType.BOOLEAN,
            
            # Special types - both DT_ prefixed and short forms
            "DT_GUID": CanonicalDataType.GUID,
            "guid": CanonicalDataType.GUID,
        }
    
    def _build_platform_mappings(self) -> Dict[CanonicalDataType, Dict[TargetPlatform, str]]:
        """Build mappings from canonical types to target platform types."""
        return {
            # Integer types
            CanonicalDataType.INTEGER: {
                TargetPlatform.SQL_SERVER: "int",
                TargetPlatform.POSTGRESQL: "integer",
                TargetPlatform.MYSQL: "int",
                TargetPlatform.ORACLE: "number(10)",
                TargetPlatform.SNOWFLAKE: "INTEGER",
                TargetPlatform.BIGQUERY: "int64",
                TargetPlatform.AZURE_SYNAPSE: "int",
                TargetPlatform.DATABRICKS: "INT",
                TargetPlatform.FABRIC: "INT"
            },
            CanonicalDataType.BIGINT: {
                TargetPlatform.SQL_SERVER: "bigint",
                TargetPlatform.POSTGRESQL: "bigint",
                TargetPlatform.MYSQL: "bigint",
                TargetPlatform.ORACLE: "number(19)",
                TargetPlatform.SNOWFLAKE: "BIGINT",
                TargetPlatform.BIGQUERY: "int64",
                TargetPlatform.AZURE_SYNAPSE: "bigint",
                TargetPlatform.DATABRICKS: "BIGINT",
                TargetPlatform.FABRIC: "BIGINT"
            },
            CanonicalDataType.SMALLINT: {
                TargetPlatform.SQL_SERVER: "smallint",
                TargetPlatform.POSTGRESQL: "smallint",
                TargetPlatform.MYSQL: "smallint",
                TargetPlatform.ORACLE: "number(5)",
                TargetPlatform.SNOWFLAKE: "SMALLINT",
                TargetPlatform.BIGQUERY: "int64",
                TargetPlatform.AZURE_SYNAPSE: "smallint",
                TargetPlatform.DATABRICKS: "SMALLINT",
                TargetPlatform.FABRIC: "SMALLINT"
            },
            CanonicalDataType.TINYINT: {
                TargetPlatform.SQL_SERVER: "tinyint",
                TargetPlatform.POSTGRESQL: "smallint",
                TargetPlatform.MYSQL: "tinyint",
                TargetPlatform.ORACLE: "number(3)",
                TargetPlatform.SNOWFLAKE: "TINYINT",
                TargetPlatform.BIGQUERY: "int64",
                TargetPlatform.AZURE_SYNAPSE: "tinyint",
                TargetPlatform.DATABRICKS: "TINYINT",
                TargetPlatform.FABRIC: "TINYINT"
            },
            # String types
            CanonicalDataType.VARCHAR: {
                TargetPlatform.SQL_SERVER: "varchar({length})",
                TargetPlatform.POSTGRESQL: "varchar({length})",
                TargetPlatform.MYSQL: "varchar({length})",
                TargetPlatform.ORACLE: "varchar2({length})",
                TargetPlatform.SNOWFLAKE: "VARCHAR({length})",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "varchar({length})",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            CanonicalDataType.NVARCHAR: {
                TargetPlatform.SQL_SERVER: "nvarchar({length})",
                TargetPlatform.POSTGRESQL: "varchar({length})",
                TargetPlatform.MYSQL: "varchar({length})",
                TargetPlatform.ORACLE: "nvarchar2({length})",
                TargetPlatform.SNOWFLAKE: "VARCHAR({length})",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "nvarchar({length})",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            CanonicalDataType.CHAR: {
                TargetPlatform.SQL_SERVER: "char({length})",
                TargetPlatform.POSTGRESQL: "char({length})",
                TargetPlatform.MYSQL: "char({length})",
                TargetPlatform.ORACLE: "char({length})",
                TargetPlatform.SNOWFLAKE: "CHAR({length})",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "char({length})",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            CanonicalDataType.NCHAR: {
                TargetPlatform.SQL_SERVER: "nchar({length})",
                TargetPlatform.POSTGRESQL: "char({length})",
                TargetPlatform.MYSQL: "char({length})",
                TargetPlatform.ORACLE: "nchar({length})",
                TargetPlatform.SNOWFLAKE: "CHAR({length})",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "nchar({length})",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            CanonicalDataType.TEXT: {
                TargetPlatform.SQL_SERVER: "varchar(max)",
                TargetPlatform.POSTGRESQL: "text",
                TargetPlatform.MYSQL: "longtext",
                TargetPlatform.ORACLE: "clob",
                TargetPlatform.SNOWFLAKE: "VARCHAR(16777216)",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "varchar(max)",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            CanonicalDataType.NTEXT: {
                TargetPlatform.SQL_SERVER: "nvarchar(max)",
                TargetPlatform.POSTGRESQL: "text",
                TargetPlatform.MYSQL: "longtext",
                TargetPlatform.ORACLE: "nclob",
                TargetPlatform.SNOWFLAKE: "VARCHAR(16777216)",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "nvarchar(max)",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            # Numeric types
            CanonicalDataType.DECIMAL: {
                TargetPlatform.SQL_SERVER: "decimal({precision},{scale})",
                TargetPlatform.POSTGRESQL: "decimal({precision},{scale})",
                TargetPlatform.MYSQL: "decimal({precision},{scale})",
                TargetPlatform.ORACLE: "number({precision},{scale})",
                TargetPlatform.SNOWFLAKE: "DECIMAL({precision},{scale})",
                TargetPlatform.BIGQUERY: "numeric({precision},{scale})",
                TargetPlatform.AZURE_SYNAPSE: "decimal({precision},{scale})",
                TargetPlatform.DATABRICKS: "DECIMAL({precision},{scale})",
                TargetPlatform.FABRIC: "DECIMAL({precision},{scale})"
            },
            CanonicalDataType.NUMERIC: {
                TargetPlatform.SQL_SERVER: "numeric({precision},{scale})",
                TargetPlatform.POSTGRESQL: "numeric({precision},{scale})",
                TargetPlatform.MYSQL: "decimal({precision},{scale})",
                TargetPlatform.ORACLE: "number({precision},{scale})",
                TargetPlatform.SNOWFLAKE: "DECIMAL({precision},{scale})",
                TargetPlatform.BIGQUERY: "numeric({precision},{scale})",
                TargetPlatform.AZURE_SYNAPSE: "numeric({precision},{scale})",
                TargetPlatform.DATABRICKS: "DECIMAL({precision},{scale})",
                TargetPlatform.FABRIC: "DECIMAL({precision},{scale})"
            },
            CanonicalDataType.MONEY: {
                TargetPlatform.SQL_SERVER: "money",
                TargetPlatform.POSTGRESQL: "money",
                TargetPlatform.MYSQL: "decimal(19,4)",
                TargetPlatform.ORACLE: "number(19,4)",
                TargetPlatform.SNOWFLAKE: "DECIMAL(19,4)",
                TargetPlatform.BIGQUERY: "numeric(19,4)",
                TargetPlatform.AZURE_SYNAPSE: "money",
                TargetPlatform.DATABRICKS: "DECIMAL(19,4)",
                TargetPlatform.FABRIC: "DECIMAL(19,4)"
            },
            CanonicalDataType.FLOAT: {
                TargetPlatform.SQL_SERVER: "float",
                TargetPlatform.POSTGRESQL: "double precision",
                TargetPlatform.MYSQL: "double",
                TargetPlatform.ORACLE: "binary_double",
                TargetPlatform.SNOWFLAKE: "FLOAT",
                TargetPlatform.BIGQUERY: "float64",
                TargetPlatform.AZURE_SYNAPSE: "float",
                TargetPlatform.DATABRICKS: "DOUBLE",
                TargetPlatform.FABRIC: "DOUBLE"
            },
            CanonicalDataType.REAL: {
                TargetPlatform.SQL_SERVER: "real",
                TargetPlatform.POSTGRESQL: "real",
                TargetPlatform.MYSQL: "float",
                TargetPlatform.ORACLE: "binary_float",
                TargetPlatform.SNOWFLAKE: "REAL",
                TargetPlatform.BIGQUERY: "float64",
                TargetPlatform.AZURE_SYNAPSE: "real",
                TargetPlatform.DATABRICKS: "FLOAT",
                TargetPlatform.FABRIC: "FLOAT"
            },
            # Date/Time types
            CanonicalDataType.DATETIME: {
                TargetPlatform.SQL_SERVER: "datetime2",
                TargetPlatform.POSTGRESQL: "timestamp",
                TargetPlatform.MYSQL: "datetime",
                TargetPlatform.ORACLE: "timestamp",
                TargetPlatform.SNOWFLAKE: "TIMESTAMP_NTZ",
                TargetPlatform.BIGQUERY: "datetime",
                TargetPlatform.AZURE_SYNAPSE: "datetime2",
                TargetPlatform.DATABRICKS: "TIMESTAMP",
                TargetPlatform.FABRIC: "TIMESTAMP"
            },
            CanonicalDataType.DATE: {
                TargetPlatform.SQL_SERVER: "date",
                TargetPlatform.POSTGRESQL: "date",
                TargetPlatform.MYSQL: "date",
                TargetPlatform.ORACLE: "date",
                TargetPlatform.SNOWFLAKE: "DATE",
                TargetPlatform.BIGQUERY: "date",
                TargetPlatform.AZURE_SYNAPSE: "date",
                TargetPlatform.DATABRICKS: "DATE",
                TargetPlatform.FABRIC: "DATE"
            },
            CanonicalDataType.TIME: {
                TargetPlatform.SQL_SERVER: "time",
                TargetPlatform.POSTGRESQL: "time",
                TargetPlatform.MYSQL: "time",
                TargetPlatform.ORACLE: "timestamp",
                TargetPlatform.SNOWFLAKE: "TIME",
                TargetPlatform.BIGQUERY: "time",
                TargetPlatform.AZURE_SYNAPSE: "time",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            CanonicalDataType.TIMESTAMP: {
                TargetPlatform.SQL_SERVER: "datetimeoffset",
                TargetPlatform.POSTGRESQL: "timestamptz",
                TargetPlatform.MYSQL: "timestamp",
                TargetPlatform.ORACLE: "timestamp with time zone",
                TargetPlatform.SNOWFLAKE: "TIMESTAMP_TZ",
                TargetPlatform.BIGQUERY: "timestamp",
                TargetPlatform.AZURE_SYNAPSE: "datetimeoffset",
                TargetPlatform.DATABRICKS: "TIMESTAMP",
                TargetPlatform.FABRIC: "TIMESTAMP"
            },
            # Boolean type
            CanonicalDataType.BOOLEAN: {
                TargetPlatform.SQL_SERVER: "bit",
                TargetPlatform.POSTGRESQL: "boolean",
                TargetPlatform.MYSQL: "boolean",
                TargetPlatform.ORACLE: "number(1)",
                TargetPlatform.SNOWFLAKE: "BOOLEAN",
                TargetPlatform.BIGQUERY: "bool",
                TargetPlatform.AZURE_SYNAPSE: "bit",
                TargetPlatform.DATABRICKS: "BOOLEAN",
                TargetPlatform.FABRIC: "BOOLEAN"
            },
            # Binary types
            CanonicalDataType.BINARY: {
                TargetPlatform.SQL_SERVER: "binary({length})",
                TargetPlatform.POSTGRESQL: "bytea",
                TargetPlatform.MYSQL: "binary({length})",
                TargetPlatform.ORACLE: "raw({length})",
                TargetPlatform.SNOWFLAKE: "BINARY",
                TargetPlatform.BIGQUERY: "bytes",
                TargetPlatform.AZURE_SYNAPSE: "binary({length})",
                TargetPlatform.DATABRICKS: "BINARY",
                TargetPlatform.FABRIC: "BINARY"
            },
            CanonicalDataType.VARBINARY: {
                TargetPlatform.SQL_SERVER: "varbinary({length})",
                TargetPlatform.POSTGRESQL: "bytea",
                TargetPlatform.MYSQL: "varbinary({length})",
                TargetPlatform.ORACLE: "raw({length})",
                TargetPlatform.SNOWFLAKE: "BINARY",
                TargetPlatform.BIGQUERY: "bytes",
                TargetPlatform.AZURE_SYNAPSE: "varbinary({length})",
                TargetPlatform.DATABRICKS: "BINARY",
                TargetPlatform.FABRIC: "BINARY"
            },
            CanonicalDataType.IMAGE: {
                TargetPlatform.SQL_SERVER: "varbinary(max)",
                TargetPlatform.POSTGRESQL: "bytea",
                TargetPlatform.MYSQL: "longblob",
                TargetPlatform.ORACLE: "blob",
                TargetPlatform.SNOWFLAKE: "BINARY",
                TargetPlatform.BIGQUERY: "bytes",
                TargetPlatform.AZURE_SYNAPSE: "varbinary(max)",
                TargetPlatform.DATABRICKS: "BINARY",
                TargetPlatform.FABRIC: "BINARY"
            },
            # Special types
            CanonicalDataType.GUID: {
                TargetPlatform.SQL_SERVER: "uniqueidentifier",
                TargetPlatform.POSTGRESQL: "uuid",
                TargetPlatform.MYSQL: "char(36)",
                TargetPlatform.ORACLE: "char(36)",
                TargetPlatform.SNOWFLAKE: "VARCHAR(36)",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "uniqueidentifier",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            CanonicalDataType.STRING: {
                TargetPlatform.SQL_SERVER: "varchar(max)",
                TargetPlatform.POSTGRESQL: "text",
                TargetPlatform.MYSQL: "longtext",
                TargetPlatform.ORACLE: "clob",
                TargetPlatform.SNOWFLAKE: "VARCHAR(16777216)",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "varchar(max)",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            CanonicalDataType.XML: {
                TargetPlatform.SQL_SERVER: "xml",
                TargetPlatform.POSTGRESQL: "xml",
                TargetPlatform.MYSQL: "longtext",
                TargetPlatform.ORACLE: "xmltype",
                TargetPlatform.SNOWFLAKE: "VARIANT",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "xml",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            },
            CanonicalDataType.JSON: {
                TargetPlatform.SQL_SERVER: "nvarchar(max)",
                TargetPlatform.POSTGRESQL: "jsonb",
                TargetPlatform.MYSQL: "json",
                TargetPlatform.ORACLE: "json",
                TargetPlatform.SNOWFLAKE: "VARIANT",
                TargetPlatform.BIGQUERY: "json",
                TargetPlatform.AZURE_SYNAPSE: "nvarchar(max)",
                TargetPlatform.DATABRICKS: "STRING",
                TargetPlatform.FABRIC: "STRING"
            }
        }
    
    def _build_conversion_rules(self) -> Dict[tuple, ConversionRisk]:
        """Build conversion risk rules between canonical types."""
        rules = {}
        
        # Safe conversions (no data loss)
        safe_conversions = [
            (CanonicalDataType.TINYINT, CanonicalDataType.SMALLINT),
            (CanonicalDataType.SMALLINT, CanonicalDataType.INTEGER),
            (CanonicalDataType.INTEGER, CanonicalDataType.BIGINT),
            (CanonicalDataType.REAL, CanonicalDataType.FLOAT),
            (CanonicalDataType.CHAR, CanonicalDataType.VARCHAR),
            (CanonicalDataType.NCHAR, CanonicalDataType.NVARCHAR),
            (CanonicalDataType.DATE, CanonicalDataType.DATETIME),
            (CanonicalDataType.TIME, CanonicalDataType.DATETIME)
        ]
        
        for source, target in safe_conversions:
            rules[(source, target)] = ConversionRisk.LOW
            
        # Medium risk conversions (potential precision loss)
        medium_conversions = [
            (CanonicalDataType.BIGINT, CanonicalDataType.INTEGER),
            (CanonicalDataType.FLOAT, CanonicalDataType.REAL),
            (CanonicalDataType.DECIMAL, CanonicalDataType.INTEGER),
            (CanonicalDataType.DATETIME, CanonicalDataType.DATE),
            (CanonicalDataType.NVARCHAR, CanonicalDataType.VARCHAR)
        ]
        
        for source, target in medium_conversions:
            rules[(source, target)] = ConversionRisk.MEDIUM
            
        # High risk conversions (potential data loss)
        high_conversions = [
            (CanonicalDataType.VARCHAR, CanonicalDataType.INTEGER),
            (CanonicalDataType.NVARCHAR, CanonicalDataType.INTEGER),
            (CanonicalDataType.DATETIME, CanonicalDataType.TIME)
        ]
        
        for source, target in high_conversions:
            rules[(source, target)] = ConversionRisk.HIGH
            
        return rules
    
    def get_canonical_type(self, ssis_type: str) -> CanonicalDataType:
        """Get canonical type for SSIS data type."""
        return self._ssis_to_canonical.get(ssis_type, CanonicalDataType.UNKNOWN)
    
    def get_platform_type(self, canonical_type: CanonicalDataType, 
                         platform: TargetPlatform, 
                         length: Optional[int] = None,
                         precision: Optional[int] = None,
                         scale: Optional[int] = None) -> str:
        """Get platform-specific type for canonical type."""
        platform_mapping = self._canonical_to_platforms.get(canonical_type, {})
        type_template = platform_mapping.get(platform, "unknown")
        
        # Replace placeholders with actual values
        if "{length}" in type_template and length:
            type_template = type_template.replace("{length}", str(length))
        if "{precision}" in type_template and precision:
            type_template = type_template.replace("{precision}", str(precision))
        if "{scale}" in type_template and scale:
            type_template = type_template.replace("{scale}", str(scale))
            
        return type_template
    
    def get_conversion_risk(self, source_type: CanonicalDataType, 
                          target_type: CanonicalDataType) -> ConversionRisk:
        """Get conversion risk between two canonical types."""
        if source_type == target_type:
            return ConversionRisk.NONE
        return self._conversion_rules.get((source_type, target_type), ConversionRisk.UNSAFE)
    
    def enrich_column_properties(self, ssis_type: str, 
                               length: Optional[str] = None,
                               precision: Optional[str] = None,
                               scale: Optional[str] = None,
                               nullable: Optional[bool] = None,
                               target_platforms: Optional[List[TargetPlatform]] = None) -> Dict[str, Any]:
        """
        Enrich column properties with comprehensive type mapping information.
        
        Args:
            ssis_type: SSIS native data type (e.g., "DT_I4")
            length: Column length if applicable
            precision: Numeric precision if applicable
            scale: Numeric scale if applicable
            nullable: Whether column allows nulls
            target_platforms: List of target platforms to map to
            
        Returns:
            Dictionary with enriched type mapping properties
        """
        canonical_type = self.get_canonical_type(ssis_type)
        
        # Convert string parameters to integers
        length_int = int(length) if length and length.isdigit() else None
        precision_int = int(precision) if precision and precision.isdigit() else None
        scale_int = int(scale) if scale and scale.isdigit() else None
        
        # Default target platforms if not specified
        if target_platforms is None:
            target_platforms = [
                TargetPlatform.SQL_SERVER,
                TargetPlatform.POSTGRESQL,
                TargetPlatform.MYSQL,
                TargetPlatform.ORACLE
            ]
        
        # Build target type mappings
        target_types = {}
        conversion_confidence = 1.0
        potential_issues = []
        
        for platform in target_platforms:
            platform_type = self.get_platform_type(
                canonical_type, platform, length_int, precision_int, scale_int
            )
            target_types[platform.value] = platform_type
            
            # Check for potential conversion issues
            if platform_type == "unknown":
                potential_issues.append(f"No mapping defined for {platform.value}")
                conversion_confidence = min(conversion_confidence, 0.5)
        
        # Additional validation
        if canonical_type == CanonicalDataType.UNKNOWN:
            potential_issues.append(f"Unknown SSIS type: {ssis_type}")
            conversion_confidence = 0.3
        
        if length_int and length_int > 8000:
            potential_issues.append("Large column length may require special handling")
            conversion_confidence = min(conversion_confidence, 0.8)
        
        return {
            "ssis_native_type": ssis_type,
            "canonical_type": canonical_type.value,
            "target_types": target_types,
            "type_precision": precision_int,
            "type_scale": scale_int,
            "type_length": length_int,
            "nullable": nullable,
            "conversion_confidence": conversion_confidence,
            "potential_issues": potential_issues,
            "type_category": self._get_type_category(canonical_type),
            "supports_indexing": self._supports_indexing(canonical_type),
            "supports_sorting": self._supports_sorting(canonical_type)
        }
    
    def _get_type_category(self, canonical_type: CanonicalDataType) -> str:
        """Categorize canonical type."""
        numeric_types = {CanonicalDataType.INTEGER, CanonicalDataType.BIGINT, 
                        CanonicalDataType.SMALLINT, CanonicalDataType.TINYINT,
                        CanonicalDataType.DECIMAL, CanonicalDataType.NUMERIC,
                        CanonicalDataType.FLOAT, CanonicalDataType.REAL, CanonicalDataType.MONEY}
        
        string_types = {CanonicalDataType.STRING, CanonicalDataType.VARCHAR,
                       CanonicalDataType.NVARCHAR, CanonicalDataType.CHAR,
                       CanonicalDataType.NCHAR, CanonicalDataType.TEXT, CanonicalDataType.NTEXT}
        
        datetime_types = {CanonicalDataType.DATETIME, CanonicalDataType.DATE,
                         CanonicalDataType.TIME, CanonicalDataType.TIMESTAMP}
        
        binary_types = {CanonicalDataType.BINARY, CanonicalDataType.VARBINARY, CanonicalDataType.IMAGE}
        
        if canonical_type in numeric_types:
            return "numeric"
        elif canonical_type in string_types:
            return "string"
        elif canonical_type in datetime_types:
            return "datetime"
        elif canonical_type in binary_types:
            return "binary"
        elif canonical_type == CanonicalDataType.BOOLEAN:
            return "boolean"
        else:
            return "special"
    
    def _supports_indexing(self, canonical_type: CanonicalDataType) -> bool:
        """Check if type supports database indexing."""
        non_indexable = {CanonicalDataType.TEXT, CanonicalDataType.NTEXT, 
                        CanonicalDataType.IMAGE, CanonicalDataType.JSON, CanonicalDataType.XML}
        return canonical_type not in non_indexable
    
    def _supports_sorting(self, canonical_type: CanonicalDataType) -> bool:
        """Check if type supports sorting operations."""
        non_sortable = {CanonicalDataType.IMAGE, CanonicalDataType.JSON, CanonicalDataType.XML}
        return canonical_type not in non_sortable
    
    def analyze_type_conversions(self, transformations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze type conversions in a list of transformations.
        
        Args:
            transformations: List of transformation definitions with source/target types
            
        Returns:
            Analysis of type conversion patterns and risks
        """
        analysis = {
            "total_conversions": len(transformations),
            "conversion_risks": {"none": 0, "low": 0, "medium": 0, "high": 0, "unsafe": 0},
            "risk_summary": [],
            "common_patterns": {},
            "recommendations": []
        }
        
        for transform in transformations:
            source_type = transform.get("source_canonical_type")
            target_type = transform.get("target_canonical_type")
            
            if source_type and target_type:
                try:
                    source_enum = CanonicalDataType(source_type)
                    target_enum = CanonicalDataType(target_type)
                    risk = self.get_conversion_risk(source_enum, target_enum)
                    
                    analysis["conversion_risks"][risk.value] += 1
                    
                    if risk in [ConversionRisk.HIGH, ConversionRisk.UNSAFE]:
                        analysis["risk_summary"].append({
                            "source": source_type,
                            "target": target_type,
                            "risk": risk.value,
                            "transformation": transform.get("column_name", "unknown")
                        })
                        
                    # Track conversion patterns
                    pattern_key = f"{source_type}->{target_type}"
                    analysis["common_patterns"][pattern_key] = analysis["common_patterns"].get(pattern_key, 0) + 1
                    
                except ValueError:
                    analysis["conversion_risks"]["unsafe"] += 1
        
        # Generate recommendations
        if analysis["conversion_risks"]["high"] > 0:
            analysis["recommendations"].append("Review high-risk type conversions for potential data loss")
        if analysis["conversion_risks"]["unsafe"] > 0:
            analysis["recommendations"].append("Validate unsafe type conversions before deployment")
        if analysis["total_conversions"] > 50:
            analysis["recommendations"].append("Consider implementing automated type validation testing")
            
        return analysis
    
    def assess_conversion_risk(self, ssis_type: str, 
                              length: Optional[int] = None,
                              precision: Optional[int] = None,
                              scale: Optional[int] = None,
                              target_platforms: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Assess migration risk for SSIS data type conversions.
        
        REQUIREMENT 2.3: Implement assess_conversion_risk Method
        This method evaluates potential data type conversion risks during migration.
        
        Args:
            ssis_type: SSIS native data type (e.g., "DT_I4", "DT_WSTR")
            length: Column length if applicable
            precision: Numeric precision if applicable
            scale: Numeric scale if applicable
            target_platforms: List of target platform names
            
        Returns:
            Dictionary containing:
                - risk_level: "low", "medium", or "high"
                - requires_manual_review: True or False
                - risk_factors: List of strings explaining the risks
        """
        canonical_type = self.get_canonical_type(ssis_type)
        risk_factors = []
        risk_level = "low"
        requires_manual_review = False
        
        # Default target platforms if not specified
        if target_platforms is None:
            target_platforms = ["sql_server", "postgresql"]
        
        # Convert string platform names to enums
        platform_enums = []
        for platform_name in target_platforms:
            try:
                platform_enum = TargetPlatform(platform_name)
                platform_enums.append(platform_enum)
            except ValueError:
                risk_factors.append(f"Unknown target platform: {platform_name}")
                risk_level = "high"
                requires_manual_review = True
        
        # Check if SSIS type is recognized
        if canonical_type == CanonicalDataType.UNKNOWN:
            risk_factors.append(f"Unrecognized SSIS data type: {ssis_type}")
            risk_level = "high"
            requires_manual_review = True
            return {
                "risk_level": risk_level,
                "requires_manual_review": requires_manual_review,
                "risk_factors": risk_factors
            }
        
        # Analyze conversion risks per target platform
        for platform_enum in platform_enums:
            platform_type = self.get_platform_type(canonical_type, platform_enum, length, precision, scale)
            
            # Check for unsupported types
            if platform_type == "unknown":
                risk_factors.append(f"No type mapping available for {platform_enum.value}")
                risk_level = "high"
                requires_manual_review = True
                continue
            
            # Check for potential data truncation
            if canonical_type in [CanonicalDataType.VARCHAR, CanonicalDataType.NVARCHAR]:
                if length and length > 4000:
                    risk_factors.append(f"Large string length ({length}) may cause truncation on {platform_enum.value}")
                    if risk_level == "low":
                        risk_level = "medium"
                elif not length:
                    risk_factors.append(f"String type without specified length may default to small size on {platform_enum.value}")
                    if risk_level == "low":
                        risk_level = "medium"
            
            # Check for precision loss in numeric conversions
            if canonical_type in [CanonicalDataType.DECIMAL, CanonicalDataType.NUMERIC]:
                if precision and precision > 28:
                    risk_factors.append(f"High precision ({precision}) may be reduced on {platform_enum.value}")
                    risk_level = "medium"
                elif not precision:
                    risk_factors.append(f"Numeric type without precision may default to lower precision on {platform_enum.value}")
                    if risk_level == "low":
                        risk_level = "medium"
            
            # Check for float precision issues
            if canonical_type in [CanonicalDataType.REAL, CanonicalDataType.FLOAT]:
                if platform_enum in [TargetPlatform.MYSQL, TargetPlatform.ORACLE]:
                    risk_factors.append(f"Float precision may vary on {platform_enum.value}")
                    if risk_level == "low":
                        risk_level = "medium"
            
            # Check for GUID/UUID compatibility
            if canonical_type == CanonicalDataType.GUID:
                if platform_enum in [TargetPlatform.MYSQL, TargetPlatform.ORACLE]:
                    risk_factors.append(f"GUID stored as string on {platform_enum.value}, may affect performance")
                    if risk_level == "low":
                        risk_level = "medium"
            
            # Check for datetime precision differences
            if canonical_type in [CanonicalDataType.DATETIME, CanonicalDataType.TIMESTAMP]:
                if platform_enum == TargetPlatform.MYSQL:
                    risk_factors.append(f"DateTime precision may differ on {platform_enum.value}")
                    if risk_level == "low":
                        risk_level = "medium"
            
            # Check for money type conversions
            if canonical_type == CanonicalDataType.MONEY:
                if platform_enum != TargetPlatform.SQL_SERVER:
                    risk_factors.append(f"Money type converted to decimal on {platform_enum.value}, may affect calculations")
                    risk_level = "medium"
            
            # Check for text/blob types
            if canonical_type in [CanonicalDataType.TEXT, CanonicalDataType.NTEXT, CanonicalDataType.IMAGE]:
                risk_factors.append(f"Large object type ({canonical_type.value}) may have different storage characteristics on {platform_enum.value}")
                risk_level = "medium"
                requires_manual_review = True
        
        # Evaluate implicit conversion risks
        type_category = self._get_type_category(canonical_type)
        
        # Check for risky implicit conversions that might be happening in transformations
        if type_category == "string" and ssis_type.upper() in ["DT_STR", "STR"]:
            # ANSI string to Unicode platforms
            for platform_enum in platform_enums:
                if platform_enum in [TargetPlatform.POSTGRESQL, TargetPlatform.MYSQL]:
                    risk_factors.append(f"ANSI string may need Unicode conversion for {platform_enum.value}")
                    if risk_level == "low":
                        risk_level = "medium"
        
        # Special case: Check for conversion to string types (high risk)
        if type_category == "numeric" and any("varchar" in self.get_platform_type(canonical_type, p) for p in platform_enums):
            risk_factors.append("Numeric to string conversion detected - potential data format issues")
            risk_level = "high"
            requires_manual_review = True
        
        # Determine final manual review requirement
        if risk_level in ["high"] or len(risk_factors) > 3:
            requires_manual_review = True
        
        return {
            "risk_level": risk_level,
            "requires_manual_review": requires_manual_review,
            "risk_factors": risk_factors
        }