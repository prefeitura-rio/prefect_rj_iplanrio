"""NF coalescing and decimal sanity checks for ``NFExtractor``."""

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def split_pages_into_batches(pages: list[int], batch_size: int = 5) -> list[list[int]]:
    """
    Split page numbers into batches of specified size.

    :param pages: List of 1-indexed page numbers.
    :param batch_size: Maximum pages per batch (default: 5).
    :returns: List of page batches.

    Example: ``[1, 2, 3, ..., 25]`` → ``[[1-10], [11-20], [21-25]]``
    """
    if len(pages) <= batch_size:
        return [pages]

    batches = []
    for i in range(0, len(pages), batch_size):
        batch = pages[i : i + batch_size]
        batches.append(batch)

    return batches


def coalesce_nfs_by_numero(all_nfs: list[dict]) -> list[dict]:
    """
    Coalesce NFs with the same numero_nf across batches.

    Handles NFs split across multiple pages/batches by merging fields:

    - Prefer non-null values.
    - For ``valor_total``/``valor_total_servico``: prefer the largest value.
    - For ``pagina``: use the earliest page number.
    - Append merge warnings to ``observacao``.

    :param all_nfs: List of NF dictionaries from all batches.
    :returns: List of coalesced NFs.
    """
    from collections import defaultdict

    if not all_nfs:
        return []

    # Group by numero_nf
    nf_groups = defaultdict(list)

    for nf in all_nfs:
        numero = nf.get("numero_nf")
        # Use numero as key (or unique ID if null)
        key = str(numero) if numero else f"_unnamed_{id(nf)}"
        nf_groups[key].append(nf)

    # Coalesce each group
    coalesced = []
    for _numero_key, group in nf_groups.items():
        if len(group) == 1:
            # Single NF, no coalescing needed
            coalesced.append(group[0])
        else:
            # Multiple NFs with same numero_nf - MERGE
            merged = {}
            conflicts = []

            for nf in group:
                for field, value in nf.items():
                    # Skip null/empty values
                    if value is None or value in ("", "-"):
                        continue

                    # Field not in merged yet - add it
                    if field not in merged:
                        merged[field] = value

                    # Field exists but is null - replace
                    elif merged[field] is None or merged[field] == "" or merged[field] == "-":
                        merged[field] = value

                    # SPECIAL: For valor_total field, prefer MAIOR valor
                    # TODO: Review this section! Change to "Not Analyzable" with comments
                    elif field == "valor_total":
                        if isinstance(value, (int, float)) and isinstance(merged[field], (int, float)):
                            if value > merged[field]:
                                old_val = merged[field]
                                merged[field] = value
                                conflicts.append(f"{field}: {old_val} → {value}")

                    # SPECIAL: For pagina, use earliest (menor número)
                    elif field == "pagina":
                        if isinstance(value, int) and isinstance(merged[field], int):
                            merged[field] = min(merged[field], value)

                    # For other fields, if different, log conflict but keep first value
                    elif merged[field] != value:
                        conflicts.append(f"{field}: '{merged[field]}' vs '{value}'")

            # Add conflict info to observacao if any
            if conflicts:
                existing_obs = merged.get("observacao", "")
                conflict_note = f"[MERGE: {'; '.join(conflicts)}]"

                if existing_obs:
                    merged["observacao"] = f"{existing_obs} {conflict_note}"
                else:
                    merged["observacao"] = conflict_note

            coalesced.append(merged)

    return coalesced


def count_decimals(value: float) -> int:
    """
    Count number of decimal places in a float.

    :param value: Float value to check.
    :returns: Number of decimal places.
    """
    if value == 0:
        return 0

    # Convert to string with high precision and strip trailing zeros
    value_str = f"{value:.10f}".rstrip("0")

    # If no decimal point, return 0
    if "." not in value_str:
        return 0

    # Count digits after decimal point
    return len(value_str.split(".")[1])


def has_suspicious_decimals(notas_fiscais: list[dict]) -> bool:
    """
    Check if any extracted valor has more than 2 decimal places.
    Brazilian currency only uses 2 decimals, so more indicates an error.

    :param notas_fiscais: List of extracted NF dictionaries.
    :returns: True if suspicious decimals detected.
    """
    for nf in notas_fiscais:
        # Check valor_total
        valor_total = nf.get("valor_total", 0.0)
        if valor_total and count_decimals(valor_total) > 2:
            return True

    return False
