"""Column schema derived from the DLO metadata returned by `SELECT * LIMIT 0`.

The integration user has no permission to call the Data Stream metadata endpoint.
The workaround is a zero-row query: the response includes column names and types
(`Varchar`, `Numeric`, `TimestampTZ`, …) at essentially zero cost.

This approach makes the schema self-updating: Account has 105 fields in QA and
106 in production, and the extra column does not break anything.
"""

import re
from dataclasses import dataclass
from typing import Literal

from prefect.logging import get_logger

logger = get_logger("toolkit")

ColumnType = Literal["string", "float", "timestamp", "boolean", "date"]

CONTROL_COLUMNS = ("_loaded_at", "data_criacao", "data_atualizacao")

DC_TYPES: dict[str, ColumnType] = {
    "Varchar": "string",
    "Text": "string",
    "Phone": "string",
    "Email": "string",
    "Url": "string",
    "Numeric": "float",
    "Number": "float",
    "Percent": "float",
    "Currency": "float",
    "Int": "float",
    "BigInt": "float",
    "SmallInt": "float",
    "Long": "float",
    "Float": "float",
    "Double": "float",
    "Decimal": "float",
    "TimestampTZ": "timestamp",
    "Timestamp": "timestamp",
    "DateTime": "timestamp",
    "Bool": "boolean",
    "Boolean": "boolean",
    "Date": "date",
}


def to_snake_case(name: str) -> str:
    """Convert a CamelCase or PascalCase name to snake_case.

    ```python
    to_snake_case("SystemModstamp")  # "system_modstamp"
    to_snake_case("CPFc")            # "cp_fc"
    ```
    """
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower()


def normalize_column_name(dlo_name: str, rename: dict[str, str] | None = None) -> str:
    """Return the column name for a DLO column.

    Strips the `ssot__` prefix and the `__c` / `__dlm` / `__dll` suffixes,
    applies snake_case, and then strips the residual `_c` left by Data Cloud
    for custom fields (e.g. `CPF_c__c` → `cpf_c` → `cpf`).  Only strips `_c`
    when the original name ended in `__c` to avoid mangling standard fields.

    After normalisation the `rename` mapping is applied for edge cases.

    ```python
    normalize_column_name("Data_Fechamento__c")  # "data_fechamento"
    normalize_column_name("SystemModstamp__dll")  # "system_modstamp"
    ```
    """
    is_custom = dlo_name.endswith("__c") or "__c__" in dlo_name
    cleaned = re.sub(r"^ssot__", "", dlo_name)
    cleaned = re.sub(r"(__c|__dlm|__dll)$", "", cleaned)
    normalized = to_snake_case(cleaned)
    if is_custom:
        normalized = re.sub(r"_c$", "", normalized)
    return (rename or {}).get(normalized, normalized)


@dataclass(frozen=True)
class TableSchema:
    """Schema for one table derived from the DLO metadata."""

    columns: tuple[tuple[str, ColumnType], ...]
    timestamp_columns: tuple[str, ...]
    date_columns: tuple[str, ...]
    all_columns: tuple[str, ...]


def build_schema(
    dlo_columns: list[dict],
    rename: dict[str, str] | None = None,
    boolean_overrides: list[str] | None = None,
) -> TableSchema:
    """Build a `TableSchema` from the column list returned by `SELECT * LIMIT 0`.

    Raise `ValueError` if an unmapped type is encountered or if a DLO column
    would collide with one of the control columns.
    """
    boolean_overrides = boolean_overrides or []
    columns: list[tuple[str, ColumnType]] = []
    timestamps: list[str] = []
    dates: list[str] = []
    seen: set[str] = set()

    for col in dlo_columns:
        name = normalize_column_name(col["name"], rename)
        if name in seen:
            continue
        seen.add(name)

        raw_type = col.get("type", "Varchar")
        if name in boolean_overrides:
            col_type: ColumnType = "boolean"
        else:
            try:
                col_type = DC_TYPES[raw_type]
            except KeyError as exc:
                raise ValueError(
                    f"Unmapped Data Cloud type '{raw_type}' on column '{col['name']}'."
                ) from exc

        if col_type == "timestamp":
            if raw_type == "Timestamp":
                logger.warning(
                    "Schema column without timezone: table=%s column=%s — values treated as UTC.",
                    name,
                    col["name"],
                )
            timestamps.append(name)
        if col_type == "date":
            dates.append(name)
        if name in CONTROL_COLUMNS:
            raise ValueError(f"DLO column '{name}' collides with a control column.")

        columns.append((name, col_type))

    columns.extend([
        ("_loaded_at", "timestamp"),
        ("data_criacao", "date"),
        ("data_atualizacao", "date"),
    ])

    return TableSchema(
        columns=tuple(columns),
        timestamp_columns=tuple(timestamps),
        date_columns=tuple(dates),
        all_columns=tuple(name for name, _ in columns),
    )
