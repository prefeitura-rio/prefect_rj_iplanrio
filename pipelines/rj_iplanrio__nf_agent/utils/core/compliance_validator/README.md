# Compliance Validator Package

This package provides utilities and classes for validating extracted NFs (invoices) against expected NFs from the database.

## Package Structure

```
core/compliance_validator/
├── __init__.py       # Public API exports
├── utils.py          # Utility functions (normalization, matching, dates)
├── validator.py      # Main validation logic (ComplianceValidator class)
└── README.md         # This file
```

## Refactoring Summary

The compliance validation module was refactored from a single 1309-line file into a well-organized package:

### Before
- **Single file**: `core/compliance_validator.py` (1309 lines)
- All functions mixed together (utils + validator logic)
- Difficult to navigate and maintain

### After
- **Package structure**: `core/compliance_validator/`
- **utils.py** (19KB): Pure utility functions
  - Normalization: `normalize_cnpj`, `normalize_number`, `normalize_value`, `extract_core_numero`
  - Date handling: `parse_date_flexible`, `check_date_against_company_start`
  - Matching algorithms: `fuzzy_match_numero`, `levenshtein_distance`, `find_near_match`, `find_extraction_match`, `values_match`
  - Constants: `VALUE_TOLERANCE`

- **validator.py** (32KB): Main validation logic
  - `compute_classification()`: Determines NF status (OK/Suspect/Not Analyzable)
  - `ComplianceValidator`: Main validator class with batch processing
  - `validate_against_expected()`: Convenience function

- **__init__.py**: Public API that exports everything needed

## Public API

### Main Classes
- `ComplianceValidator`: Main validator class for batch validation
- `compute_classification()`: Classification function (OK/Suspect/Not Analyzable)
- `validate_against_expected()`: Quick validation convenience function

### Normalization Functions
- `normalize_cnpj(cnpj)`: Normalize CNPJ to 14 digits
- `normalize_number(num)`: Normalize NF number
- `normalize_value(val)`: Normalize Brazilian currency format

### Date Functions
- `parse_date_flexible(date_str)`: Parse DD/MM/YYYY, MM/YYYY, YYYY-MM-DD
- `check_date_against_company_start(data_emissao, inicio_atividade_data)`: Validate NF date

### Matching Functions
- `fuzzy_match_numero(num1, num2)`: Fuzzy number matching
- `levenshtein_distance(s1, s2)`: String distance calculation
- `find_near_match(extracted_nf, expected_nfs)`: Near-match detection
- `find_extraction_match(expected_cnpj, expected_numero, extracted_nfs)`: Find matching NF
- `values_match(val1, val2, tolerance)`: Compare monetary values

### Constants
- `VALUE_TOLERANCE`: Default tolerance for value comparison (R$ 0.01)

## Usage

All imports remain the same as before:

```python
# Import main classes
from core.compliance_validator import ComplianceValidator, compute_classification

# Import utilities
from core.compliance_validator import normalize_cnpj, normalize_number, normalize_value

# Import date functions
from core.compliance_validator import parse_date_flexible, check_date_against_company_start

# Import matching functions
from core.compliance_validator import fuzzy_match_numero, values_match

# Import constants
from core.compliance_validator import VALUE_TOLERANCE
```

## Benefits of Refactoring

1. **Better Organization**: Utility functions are separated from validation logic
2. **Easier Navigation**: Find specific functions quickly (utils vs validator)
3. **Clearer Responsibilities**: Each file has a single, clear purpose
4. **Improved Maintainability**: Changes to utilities don't affect validator logic
5. **Better Testing**: Can test utilities independently from validation logic
6. **No Breaking Changes**: All existing imports continue to work

## Backward Compatibility

✅ **100% backward compatible**
- All existing imports work without modification
- All function signatures remain the same
- All behavior is preserved
- The old file is backed up as `compliance_validator.py.old`

## Migration Notes

No migration needed! The refactoring is transparent to all existing code.

If you want to import directly from the submodules:

```python
# Import from utils submodule
from core.compliance_validator.utils import normalize_cnpj, levenshtein_distance

# Import from validator submodule
from core.compliance_validator.validator import ComplianceValidator
```

## Testing

All imports and functionality have been tested:
- ✅ `core/__init__.py` exports
- ✅ `run_poc.bigquery_loader` integration
- ✅ `test_date_validation.py` imports
- ✅ `run_poc/processor.py` imports
- ✅ `evaluation/utils/generate_results.py` imports

Run tests with:
```bash
python -m pytest test_date_validation.py
```
