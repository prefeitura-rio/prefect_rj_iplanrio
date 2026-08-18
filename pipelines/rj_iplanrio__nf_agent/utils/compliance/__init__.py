"""
Compliance Validator Package

This package provides utilities and classes for validating extracted NFs against expected NFs.

Public API:
- ComplianceValidator: Main validator class
- compute_classification(): Classification function
- validate_against_expected(): Convenience validation function
- Normalization functions: normalize_cnpj, normalize_number, normalize_value
- Matching functions: fuzzy_match_numero, values_match, find_extraction_match
- Date functions: parse_date_flexible, check_date_against_company_start
- Constants: VALUE_TOLERANCE
"""

# Import from validator module
# Import from utils module
from .utils import (
    # Constants
    VALUE_TOLERANCE,
    check_date_against_company_start,
    extract_core_numero,
    find_extraction_match,
    find_near_match,
    # Matching functions
    fuzzy_match_numero,
    levenshtein_distance,
    # Normalization functions
    normalize_cnpj,
    normalize_number,
    normalize_value,
    # Date functions
    parse_date_flexible,
    values_match,
)
from .validator import ComplianceValidator, compute_classification, validate_against_expected

# Define public API
__all__ = [
    # Main classes and functions
    'ComplianceValidator',
    'compute_classification',
    'validate_against_expected',

    # Constants
    'VALUE_TOLERANCE',

    # Normalization
    'normalize_cnpj',
    'normalize_number',
    'normalize_value',
    'extract_core_numero',

    # Date functions
    'parse_date_flexible',
    'check_date_against_company_start',

    # Matching
    'fuzzy_match_numero',
    'levenshtein_distance',
    'find_near_match',
    'find_extraction_match',
    'values_match'
]
