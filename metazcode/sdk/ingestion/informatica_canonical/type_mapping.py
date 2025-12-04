"""
Informatica Data Type Mapping Engine

This module provides comprehensive data type mapping capabilities for Informatica metadata extraction.
It handles conversion between Informatica native types and target platform types, providing
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
    UUID = "UUID"
    JSON = "JSON"
    XML = "XML"
    
    # Unknown/unsupported type
    UNKNOWN = "UNKNOWN"


class TargetPlatform(Enum):
    """Supported target platforms for type mapping."""
    
    SQL_SERVER = "sql_server"
    POSTGRESQL = "postgresql"
    ORACLE = "oracle"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    DATABRICKS = "databricks"


class InformaticaDataTypeMapper:
    """
    Maps Informatica data types to canonical types and target platform types.
    
    This mapper understands Informatica's native data type system and provides
    conversion capabilities for various target platforms.
    """
    
    def __init__(self):
        """Initialize the Informatica data type mapper."""
        self._initialize_type_mappings()
    
    def _initialize_type_mappings(self):
        """Initialize the type mapping dictionaries."""
        
        # Informatica to Canonical type mapping
        self.informatica_to_canonical = {
            # String types
            'string': CanonicalDataType.VARCHAR,
            'varchar': CanonicalDataType.VARCHAR,
            'varchar2': CanonicalDataType.VARCHAR,
            'char': CanonicalDataType.CHAR,
            'nstring': CanonicalDataType.NVARCHAR,
            'nvarchar': CanonicalDataType.NVARCHAR,
            'nchar': CanonicalDataType.NCHAR,
            'text': CanonicalDataType.TEXT,
            'ntext': CanonicalDataType.NTEXT,
            
            # Numeric types
            'decimal': CanonicalDataType.DECIMAL,
            'numeric': CanonicalDataType.NUMERIC,
            'number': CanonicalDataType.DECIMAL,
            'number(p,s)': CanonicalDataType.DECIMAL,
            'integer': CanonicalDataType.INTEGER,
            'int': CanonicalDataType.INTEGER,
            'bigint': CanonicalDataType.BIGINT,
            'smallint': CanonicalDataType.SMALLINT,
            'tinyint': CanonicalDataType.TINYINT,
            'float': CanonicalDataType.FLOAT,
            'double': CanonicalDataType.FLOAT,
            'real': CanonicalDataType.REAL,
            
            # Date/Time types
            'date/time': CanonicalDataType.DATETIME,
            'datetime': CanonicalDataType.DATETIME,
            'date': CanonicalDataType.DATE,
            'time': CanonicalDataType.TIME,
            'timestamp': CanonicalDataType.TIMESTAMP,
            
            # Binary types
            'binary': CanonicalDataType.BINARY,
            'varbinary': CanonicalDataType.VARBINARY,
        }
        
        # Target platform mappings
        self.canonical_to_targets = {
            TargetPlatform.SQL_SERVER: {
                CanonicalDataType.VARCHAR: "VARCHAR",
                CanonicalDataType.NVARCHAR: "NVARCHAR",
                CanonicalDataType.CHAR: "CHAR",
                CanonicalDataType.NCHAR: "NCHAR",
                CanonicalDataType.TEXT: "TEXT",
                CanonicalDataType.NTEXT: "NTEXT",
                CanonicalDataType.INTEGER: "INT",
                CanonicalDataType.BIGINT: "BIGINT",
                CanonicalDataType.SMALLINT: "SMALLINT",
                CanonicalDataType.TINYINT: "TINYINT",
                CanonicalDataType.DECIMAL: "DECIMAL",
                CanonicalDataType.NUMERIC: "NUMERIC",
                CanonicalDataType.FLOAT: "FLOAT",
                CanonicalDataType.REAL: "REAL",
                CanonicalDataType.MONEY: "MONEY",
                CanonicalDataType.DATETIME: "DATETIME2",
                CanonicalDataType.DATE: "DATE",
                CanonicalDataType.TIME: "TIME",
                CanonicalDataType.TIMESTAMP: "DATETIME2",
                CanonicalDataType.BINARY: "BINARY",
                CanonicalDataType.VARBINARY: "VARBINARY",
                CanonicalDataType.BOOLEAN: "BIT",
                CanonicalDataType.UUID: "UNIQUEIDENTIFIER",
            },
            
            TargetPlatform.POSTGRESQL: {
                CanonicalDataType.VARCHAR: "VARCHAR",
                CanonicalDataType.NVARCHAR: "VARCHAR",
                CanonicalDataType.CHAR: "CHAR",
                CanonicalDataType.NCHAR: "CHAR",
                CanonicalDataType.TEXT: "TEXT",
                CanonicalDataType.NTEXT: "TEXT",
                CanonicalDataType.INTEGER: "INTEGER",
                CanonicalDataType.BIGINT: "BIGINT",
                CanonicalDataType.SMALLINT: "SMALLINT",
                CanonicalDataType.TINYINT: "SMALLINT",
                CanonicalDataType.DECIMAL: "DECIMAL",
                CanonicalDataType.NUMERIC: "NUMERIC",
                CanonicalDataType.FLOAT: "DOUBLE PRECISION",
                CanonicalDataType.REAL: "REAL",
                CanonicalDataType.MONEY: "MONEY",
                CanonicalDataType.DATETIME: "TIMESTAMP",
                CanonicalDataType.DATE: "DATE",
                CanonicalDataType.TIME: "TIME",
                CanonicalDataType.TIMESTAMP: "TIMESTAMP",
                CanonicalDataType.BINARY: "BYTEA",
                CanonicalDataType.VARBINARY: "BYTEA",
                CanonicalDataType.BOOLEAN: "BOOLEAN",
                CanonicalDataType.UUID: "UUID",
                CanonicalDataType.JSON: "JSONB",
            },
            
            TargetPlatform.ORACLE: {
                CanonicalDataType.VARCHAR: "VARCHAR2",
                CanonicalDataType.NVARCHAR: "NVARCHAR2",
                CanonicalDataType.CHAR: "CHAR",
                CanonicalDataType.NCHAR: "NCHAR",
                CanonicalDataType.TEXT: "CLOB",
                CanonicalDataType.NTEXT: "NCLOB",
                CanonicalDataType.INTEGER: "NUMBER(38,0)",
                CanonicalDataType.BIGINT: "NUMBER(19,0)",
                CanonicalDataType.SMALLINT: "NUMBER(5,0)",
                CanonicalDataType.TINYINT: "NUMBER(3,0)",
                CanonicalDataType.DECIMAL: "NUMBER",
                CanonicalDataType.NUMERIC: "NUMBER",
                CanonicalDataType.FLOAT: "BINARY_DOUBLE",
                CanonicalDataType.REAL: "BINARY_FLOAT",
                CanonicalDataType.DATETIME: "TIMESTAMP",
                CanonicalDataType.DATE: "DATE",
                CanonicalDataType.TIME: "TIMESTAMP",
                CanonicalDataType.TIMESTAMP: "TIMESTAMP",
                CanonicalDataType.BINARY: "RAW",
                CanonicalDataType.VARBINARY: "BLOB",
                CanonicalDataType.BOOLEAN: "NUMBER(1,0)",
            }
        }
    
    def map_informatica_type(
        self,
        informatica_type: str,
        precision: Optional[int] = None,
        scale: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Map an Informatica data type to canonical representation.
        
        Args:
            informatica_type: The Informatica native data type
            precision: Optional precision for numeric/string types
            scale: Optional scale for numeric types
            
        Returns:
            Dictionary with canonical type information
        """
        # Normalize the type name
        normalized_type = informatica_type.lower().strip()
        
        # Handle special Oracle number patterns
        if normalized_type.startswith('number') and '(' in normalized_type:
            normalized_type = 'number(p,s)'
        
        # Map to canonical type
        canonical_type = self.informatica_to_canonical.get(
            normalized_type, 
            CanonicalDataType.UNKNOWN
        )
        
        return {
            'source_type': informatica_type,
            'canonical_type': canonical_type.value,
            'precision': precision,
            'scale': scale,
            'is_nullable': True,  # Default assumption
            'metadata': {
                'normalized_source_type': normalized_type,
                'type_category': self._categorize_type(canonical_type)
            }
        }
    
    def get_target_type(
        self,
        canonical_type: CanonicalDataType,
        target_platform: TargetPlatform,
        precision: Optional[int] = None,
        scale: Optional[int] = None
    ) -> str:
        """
        Get target platform type for a canonical type.
        
        Args:
            canonical_type: The canonical data type
            target_platform: Target platform
            precision: Optional precision
            scale: Optional scale
            
        Returns:
            Target platform type string
        """
        platform_mappings = self.canonical_to_targets.get(target_platform, {})
        base_type = platform_mappings.get(canonical_type, "VARCHAR")
        
        # Add precision/scale if applicable
        if precision is not None and canonical_type in [
            CanonicalDataType.VARCHAR, CanonicalDataType.NVARCHAR,
            CanonicalDataType.CHAR, CanonicalDataType.NCHAR,
            CanonicalDataType.DECIMAL, CanonicalDataType.NUMERIC
        ]:
            if scale is not None and canonical_type in [CanonicalDataType.DECIMAL, CanonicalDataType.NUMERIC]:
                return f"{base_type}({precision},{scale})"
            else:
                return f"{base_type}({precision})"
        
        return base_type
    
    def _categorize_type(self, canonical_type: CanonicalDataType) -> str:
        """Categorize a canonical type into broad categories."""
        string_types = {CanonicalDataType.STRING, CanonicalDataType.VARCHAR, 
                       CanonicalDataType.NVARCHAR, CanonicalDataType.CHAR,
                       CanonicalDataType.NCHAR, CanonicalDataType.TEXT, CanonicalDataType.NTEXT}
        
        numeric_types = {CanonicalDataType.INTEGER, CanonicalDataType.BIGINT,
                        CanonicalDataType.SMALLINT, CanonicalDataType.TINYINT,
                        CanonicalDataType.DECIMAL, CanonicalDataType.NUMERIC,
                        CanonicalDataType.FLOAT, CanonicalDataType.REAL, CanonicalDataType.MONEY}
        
        datetime_types = {CanonicalDataType.DATETIME, CanonicalDataType.DATE,
                         CanonicalDataType.TIME, CanonicalDataType.TIMESTAMP}
        
        binary_types = {CanonicalDataType.BINARY, CanonicalDataType.VARBINARY, CanonicalDataType.IMAGE}
        
        if canonical_type in string_types:
            return "string"
        elif canonical_type in numeric_types:
            return "numeric"
        elif canonical_type in datetime_types:
            return "datetime"
        elif canonical_type in binary_types:
            return "binary"
        elif canonical_type == CanonicalDataType.BOOLEAN:
            return "boolean"
        else:
            return "other"
    
    def get_supported_platforms(self) -> List[TargetPlatform]:
        """Get list of supported target platforms."""
        return list(self.canonical_to_targets.keys())
    
    def validate_type_conversion(
        self,
        source_type: str,
        target_platform: TargetPlatform
    ) -> Dict[str, Any]:
        """
        Validate if a type conversion is supported.
        
        Args:
            source_type: Source Informatica type
            target_platform: Target platform
            
        Returns:
            Validation result dictionary
        """
        type_info = self.map_informatica_type(source_type)
        canonical_type = CanonicalDataType(type_info['canonical_type'])
        
        platform_mappings = self.canonical_to_targets.get(target_platform, {})
        is_supported = canonical_type in platform_mappings
        
        result = {
            'is_supported': is_supported,
            'source_type': source_type,
            'canonical_type': canonical_type.value,
            'target_platform': target_platform.value,
            'warnings': [],
            'recommendations': []
        }
        
        if not is_supported:
            result['warnings'].append(f"Type conversion not directly supported for {source_type}")
            result['recommendations'].append("Consider using VARCHAR as fallback type")
        
        return result

    def enrich_column_properties(
        self,
        informatica_type: str,
        length: Optional[str] = None,
        precision: Optional[str] = None,
        scale: Optional[str] = None,
        nullable: Optional[bool] = None,
        target_platforms: Optional[List[TargetPlatform]] = None
    ) -> Dict[str, Any]:
        """
        Enrich column properties with comprehensive type mapping information.
        
        Args:
            informatica_type: Informatica native data type (e.g., "string", "decimal")
            length: Column length if applicable
            precision: Numeric precision if applicable
            scale: Numeric scale if applicable
            nullable: Whether column allows nulls
            target_platforms: List of target platforms to map to
            
        Returns:
            Dictionary with enriched type mapping properties
        """
        canonical_type = self.informatica_to_canonical.get(
            informatica_type.lower(), 
            CanonicalDataType.UNKNOWN
        )
        
        # Convert string parameters to integers
        length_int = int(length) if length and str(length).isdigit() else None
        precision_int = int(precision) if precision and str(precision).isdigit() else None
        scale_int = int(scale) if scale and str(scale).isdigit() else None
        
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
            platform_mappings = self.canonical_to_targets.get(platform, {})
            platform_type = platform_mappings.get(canonical_type, "unknown")
            
            # Add length/precision/scale for appropriate types
            if canonical_type in [CanonicalDataType.VARCHAR, CanonicalDataType.NVARCHAR, CanonicalDataType.CHAR, CanonicalDataType.NCHAR]:
                if length_int:
                    platform_type = f"{platform_type}({length_int})"
            elif canonical_type in [CanonicalDataType.DECIMAL, CanonicalDataType.NUMERIC]:
                if precision_int and scale_int:
                    platform_type = f"{platform_type}({precision_int},{scale_int})"
                elif precision_int:
                    platform_type = f"{platform_type}({precision_int})"
            
            target_types[platform.value] = platform_type
            
            # Check for potential conversion issues
            if platform_type == "unknown":
                potential_issues.append(f"No mapping defined for {platform.value}")
                conversion_confidence *= 0.8
        
        # Determine type category and capabilities
        type_category = self._categorize_type(canonical_type)
        
        supports_indexing = canonical_type not in [
            CanonicalDataType.TEXT, CanonicalDataType.NTEXT, 
            CanonicalDataType.IMAGE, CanonicalDataType.JSON
        ]
        
        supports_sorting = canonical_type not in [
            CanonicalDataType.IMAGE, CanonicalDataType.JSON, CanonicalDataType.XML
        ]
        
        return {
            "informatica_native_type": informatica_type,
            "canonical_type": canonical_type.value,
            "target_types": target_types,
            "type_precision": precision_int,
            "type_scale": scale_int,
            "type_length": length_int,
            "nullable": nullable if nullable is not None else True,
            "conversion_confidence": conversion_confidence,
            "potential_issues": potential_issues,
            "type_category": type_category,
            "supports_indexing": supports_indexing,
            "supports_sorting": supports_sorting
        }