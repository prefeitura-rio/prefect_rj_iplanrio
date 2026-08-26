"""
Compliance Validation Utility Functions

This module contains utility functions for normalizing and matching NF data:
- CNPJ and number normalization
- Value normalization
- Date parsing
- Fuzzy number matching and 3-field match scoring

Every function here is a real dependency of ``processing/metadata.py``'s
``build_json_output`` (the per-page JSON output, the pipeline's only official
output format) or of ``nfst_fatura_merger.py``. Functions that were
only reachable via the now-removed ``ComplianceValidator`` rule-engine
machinery (``values_match``, ``find_extraction_match``, ``find_near_match``,
``check_date_against_company_start``, ``VALUE_TOLERANCE``) were deleted along
with it — see ``matching/README.md``.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime

# =============================================================================
# NORMALIZATION FUNCTIONS
# =============================================================================


def normalize_cnpj(cnpj: str) -> str:
    """
    Normalize CNPJ by removing all non-digit characters and padding to 14 digits.

    :param cnpj: CNPJ value in any format.
    :returns: CNPJ with only digits, zero-padded to 14 characters, or empty string.
    """
    if cnpj is None or (isinstance(cnpj, float) and cnpj != cnpj) or cnpj == "-" or cnpj == "":
        return ""
    digits = re.sub(r"\D", "", str(cnpj))
    return digits.zfill(14)


def normalize_number(num: str) -> str:
    """
    Normalize NF number by removing non-alphanumeric characters and leading zeros.

    :param num: NF number value in any format.
    :returns: NF number with only alphanumeric characters, leading zeros removed,
        or empty string.
    """
    if num is None or (isinstance(num, float) and num != num) or num == "-" or num == "":
        return ""
    cleaned = re.sub(r"[^\w]", "", str(num)).upper()
    try:
        if cleaned.isdigit():
            return str(int(cleaned))
        return cleaned
    except Exception:
        return cleaned


def extract_core_numero(num: str) -> str:
    """
    Extract core number by identifying and removing common prefixes/suffixes.

    IMPORTANT: Extract patterns BEFORE removing separators to avoid ambiguity.

    Patterns removed:
    - Year prefix: 2024/4076, 2023-4076, 20XX_4076
    - Year suffix: 4076/2024, 4076-2024, 4076_2024
    - NF prefix: NF30456, NF/30456, NF-30456
    - NF suffix: 30456\\NF, 30456/NF, 30456-NF
    - Year + zero padding: 202400000124, 20230001234, 200000078
    - Combined: NF/2024/4076, 2024/4076-NF

    :param num: NF number value in any format.
    :returns: Core number without year prefix/suffix or NF markers,
        or empty string.
    """
    if num is None or (isinstance(num, float) and num != num) or num == "-" or num == "":
        return ""

    original = str(num).strip()

    # Helper function to check if a string looks like a year
    def looks_like_year(s):
        """Check if string looks like a year (19XX, 20XX, or 2-digit year)."""
        if not s.isdigit():
            return False
        year_int = int(s)
        # 2-digit years (00-99) or 4-digit years (1900-2099)
        return (0 <= year_int <= 99) or (1900 <= year_int <= 2099)

    # Pattern 1: Remove year prefix with separator (year + separator + rest)
    # Examples: 2024/4076, 20/4076, 2023-4076
    # Be smart: only remove if the first part looks like a year
    match = re.match(r"^(\d{2,4})[/\-_\\]+(.+)$", original, re.IGNORECASE)
    if match:
        potential_year = match.group(1)
        rest = match.group(2)
        # Only remove if it looks like a year AND rest is not empty
        if looks_like_year(potential_year) and rest:
            original = rest

    # Pattern 2: Remove year suffix with separator (rest + separator + year)
    # Examples: 4076/2024, 4076-24
    # Be smart: only remove if the last part looks like a year
    match = re.search(r"^(.+)[/\-_\\]+(\d{2,4})$", original, re.IGNORECASE)
    if match:
        rest = match.group(1)
        potential_year = match.group(2)
        # Only remove if it looks like a year AND rest is not empty
        if looks_like_year(potential_year) and rest:
            original = rest

    # Pattern 3: Remove NF prefix with optional separator
    # Examples: NF/30456, NF30456, NF-30456
    match = re.match(r"^NF[/\-_\\]*(.+)$", original, re.IGNORECASE)
    if match:
        original = match.group(1)
        # After removing NF, re-check for year patterns
        # This handles cases like NF/2024/4076 → 2024/4076 → 4076
        match_year = re.match(r"^(\d{2,4})[/\-_\\]+(.+)$", original, re.IGNORECASE)
        if match_year:
            potential_year = match_year.group(1)
            rest = match_year.group(2)
            if looks_like_year(potential_year) and rest:
                original = rest

    # Pattern 4: Remove NF suffix with optional separator
    # Examples: 30456\NF, 30456/NF, 30456NF
    match = re.search(r"^(.+?)[/\-_\\]*NF$", original, re.IGNORECASE)
    if match:
        original = match.group(1)
        # After removing NF, re-check for year patterns
        # This handles cases like 2024/4076-NF → 2024/4076 → 4076
        match_year = re.match(r"^(\d{2,4})[/\-_\\]+(.+)$", original, re.IGNORECASE)
        if match_year:
            potential_year = match_year.group(1)
            rest = match_year.group(2)
            if looks_like_year(potential_year) and rest:
                original = rest

    # Pattern 5: Year prefix with zero padding (no separator)
    # Examples: 202400000124 -> 124, 20230001234 -> 1234, 200000078 -> 78
    # Pattern: 20XX + at least 3 zeros + actual number
    # This is a fallback for cases where year and number are concatenated with zero padding
    match = re.match(r"^(19\d{2}|20\d{2})0{3,}(\d+)$", original)
    if match:
        potential_year = match.group(1)
        number = match.group(2)
        # Only extract if it looks like a year AND the number part is not empty
        if looks_like_year(potential_year) and number:
            original = number

    # Now normalize: remove remaining special chars and leading zeros
    cleaned = re.sub(r"[^\w]", "", original).upper()

    # Remove leading zeros from numeric-only strings
    if cleaned.isdigit():
        return str(int(cleaned))

    # Fallback: If still has letters mixed with numbers (like "A4756"),
    # extract the longest sequence of consecutive digits
    # Examples: A-4756 → 4756, ABC123XYZ → 123
    if not cleaned.isdigit() and any(c.isdigit() for c in cleaned):
        digit_sequences = re.findall(r"\d+", cleaned)
        if digit_sequences:
            # Return the longest sequence (remove leading zeros)
            longest = max(digit_sequences, key=len)
            return str(int(longest))

    return cleaned


def normalize_value(val: object) -> float:
    """
    Normalize monetary value to float.

    Handles Brazilian currency format where dots are thousand separators
    and commas are decimal separators. Only treats . or , as decimal separator
    if followed by 1-2 digits at the end.

    :param val: Monetary value as string, int, or float.
    :returns: Normalized float value, or 0.0 if unparseable.
    """
    if val is None or (isinstance(val, float) and val != val) or val == "-" or val == "":
        return 0.0

    if isinstance(val, (int, float)):
        return float(val)

    val_str = str(val).replace("R$", "").replace(" ", "").strip()

    decimal_pattern = r"[.,](\d{1,2})$"
    match = re.search(decimal_pattern, val_str)

    if match:
        decimal_pos = match.start()
        integer_part = val_str[:decimal_pos]
        decimal_part = match.group(1)
        integer_part = integer_part.replace(".", "").replace(",", "")
        normalized = f"{integer_part}.{decimal_part}"
    else:
        normalized = val_str.replace(".", "").replace(",", "")

    try:
        return float(normalized)
    except Exception:
        return 0.0


# =============================================================================
# DATE FUNCTIONS
# =============================================================================


def parse_date_flexible(date_str: str) -> date | None:
    """
    Parse date from flexible formats.

    Formats supported:
    - DD/MM/YYYY → date(2024, 1, 15)
    - DD/MM/YY → date(2024, 1, 15)
    - MM/YYYY → date(2024, 1, 1)  # Use day=1 for month-only
    - YYYY-MM-DD → date(2024, 1, 15)  # BigQuery DATE format

    :param date_str: Date string in Brazilian or ISO format.
    :returns: datetime.date object, or None if parsing fails.
    """
    if not date_str or date_str is None or (isinstance(date_str, float) and date_str != date_str):
        return None

    date_str = str(date_str).strip()

    # Try DD/MM/YYYY and DD/MM/YY (Brazilian format)
    for fmt in ["%d/%m/%Y", "%d/%m/%y"]:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            pass

    # Try MM/YYYY (competency dates)
    try:
        dt = datetime.strptime(date_str, "%m/%Y")
        return dt.date()
    except ValueError:
        pass

    # Try YYYY-MM-DD (ISO format - BigQuery DATE type)
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.date()
    except ValueError:
        pass

    return None


# =============================================================================
# MATCHING FUNCTIONS
# =============================================================================


def fuzzy_match_numero(num1: str, num2: str) -> bool:
    """
    Fuzzy match two numero_nf values.

    Tries:
    1. Exact match of original strings (raw comparison)
    2. Match of core extracted numbers (pattern-aware)
    3. Check if one is substring of the other (only if cores are similar)

    :param num1: First NF number.
    :param num2: Second NF number.
    :returns: True if numbers fuzzy match.
    """
    # Convert to strings
    str1 = "" if (num1 is None or (isinstance(num1, float) and num1 != num1)) else str(num1)
    str2 = "" if (num2 is None or (isinstance(num2, float) and num2 != num2)) else str(num2)

    # Exact match of original strings (before any normalization)
    if str1 == str2:
        return True

    # Extract core numbers (pattern-aware extraction)
    core1 = extract_core_numero(num1)
    core2 = extract_core_numero(num2)

    # If cores are empty, no match
    if not core1 or not core2:
        return False

    # Try core match
    if core1 == core2:
        return True

    # Try substring match (one contains the other)
    # Only if both cores are similar in length (within 2 chars)
    # This prevents false positives like "4765" matching "20244765"
    len_diff = abs(len(core1) - len(core2))
    if len_diff <= 2:
        if core1 in core2 or core2 in core1:
            # Only match if the shorter one is at least 4 chars (avoid false positives)
            min_len = min(len(core1), len(core2))
            if min_len >= 4:
                return True

    return False


@dataclass(frozen=True)
class DocumentFields:
    """CNPJ, número and data_emissão for one side of a 3-field match comparison."""

    cnpj: str
    numero: str
    data: str | None


def match_score_3_fields(expected: DocumentFields, extracted: DocumentFields) -> int:
    """
    Calculate match score for 3-field matching (CNPJ + número + data_emissão).

    This function implements a flexible matching strategy where any 2 of 3 fields
    matching is sufficient to consider the documents as matching. This helps handle
    cases where one field might have minor discrepancies.

    Matching rules:
    - CNPJ: Normalized exact match (removes formatting, pads to 14 digits)
    - Número: Fuzzy match (handles leading zeros, separators, year prefixes/suffixes)
    - Data: Exact date match (dia/mês/ano must match exactly)

    :param expected: CNPJ/número/data from the declaration. ``data`` uses
        format DD/MM/YYYY or YYYY-MM-DD.
    :param extracted: CNPJ/número/data extracted by the model. ``data`` uses
        format DD/MM/YYYY or YYYY-MM-DD.
    :returns: Number of fields that match (0-3):
        - 3: Perfect match (all fields)
        - 2: Partial match (sufficient for positive match)
        - 1: Weak match (insufficient)
        - 0: No match
    """
    score = 0

    # CNPJ match (normalized exact match)
    cnpj_exp_norm = normalize_cnpj(expected.cnpj)
    cnpj_ext_norm = normalize_cnpj(extracted.cnpj)
    if cnpj_exp_norm and cnpj_ext_norm and cnpj_exp_norm == cnpj_ext_norm:
        score += 1

    # Número match (fuzzy match - handles leading zeros, separators, patterns)
    if fuzzy_match_numero(expected.numero, extracted.numero):
        score += 1

    # Data match (exact date comparison)
    if expected.data and extracted.data:
        # Parse both dates
        expected_date_parsed = parse_date_flexible(expected.data)
        extracted_date_parsed = parse_date_flexible(extracted.data)

        # If both dates are valid and match exactly
        if expected_date_parsed and extracted_date_parsed:
            if expected_date_parsed == extracted_date_parsed:
                score += 1

    return score
