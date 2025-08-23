"""
Informatica ingestion package.
"""

from .informatica_loader import InformaticaLoader
from .informatica_parser import CanonicalInformaticaParser
from .type_mapping import InformaticaDataTypeMapper

__all__ = [
    "InformaticaLoader",
    "CanonicalInformaticaParser", 
    "InformaticaDataTypeMapper"
]