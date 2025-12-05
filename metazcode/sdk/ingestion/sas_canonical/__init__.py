# SAS Canonical Parser Package
#
# Implements the Tomassetti two-pass approach for SAS parsing:
# - Pass 1: Structure extraction (DATA steps, PROCs, macros as token lists)
# - Pass 2: Lazy parsing of macro bodies when accessed
#
# References:
# - https://tomassetti.me/how-to-use-the-sas-parser/
# - https://tomassetti.me/challenges-in-parsing-legacy-languages-sas-macros/

from .sas_parser import CanonicalSasParser
from .sas_loader import SasLoader

__all__ = ["CanonicalSasParser", "SasLoader"]
