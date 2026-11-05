"""Load and validate `tabelas.yaml`.

Validation runs at import time: unknown field, wrong type, missing DLO for an
origin, duplicate name or invalid identifier raise `ValueError` before any run
starts.
"""

from collections import Counter
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Literal

import yaml

PIPELINE_DIR = Path(__file__).parent.parent
ORIGINS = ("qa", "staging", "prod")
IDENTIFIER_RE = __import__("re").compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Table:
    """One entry from `tabelas.yaml`."""

    nome: str
    grupo: str
    dlo: dict[str, str]
    primary_key: str = "id"
    order_by: str = "Id__c"
    coluna_modificacao: str = "SystemModstamp__c"
    renomear: dict[str, str] = field(default_factory=dict)
    booleanas: list[str] = field(default_factory=list)
    cluster: list[str] = field(default_factory=list)

    @property
    def staging(self) -> str:
        """Name of the staging table in BigQuery."""
        return f"{self.nome}_staging"

    def dlo_for(self, origin: str) -> str:
        """Return the DLO name for `origin`.

        Raise `ValueError` if `origin` is not configured.
        """
        try:
            return self.dlo[origin]
        except KeyError as exc:
            raise ValueError(f"Table '{self.nome}' has no DLO for origin '{origin}'.") from exc


def expected_type(annotation: object) -> type:
    """Resolve the runtime type from a dataclass field annotation."""
    name = annotation if isinstance(annotation, str) else getattr(annotation, "__origin__", annotation)
    if not isinstance(name, str):
        return name  # type: ignore[return-value]
    name = name.replace(" ", "")
    if name.startswith("dict["):
        return dict
    if name.startswith("list["):
        return list
    return {"str": str, "bool": bool, "int": int, "dict": dict, "list": list}[name]


def validate_identifier(value: str, context: str) -> None:
    """Raise `ValueError` if `value` is not a safe SQL identifier."""
    if not IDENTIFIER_RE.match(value):
        raise ValueError(f"{context}: '{value}' is not a valid identifier (letters, digits, underscores).")


def load_tables(path: Path) -> list[Table]:
    """Load and validate `tabelas.yaml`, returning a list of `Table` objects.

    Raise `ValueError` on any structural or semantic error.
    """
    items = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(items, list) or not items:
        raise ValueError(f"{path.name}: expected a non-empty list of tables.")

    all_fields = {f.name: f for f in fields(Table)}
    tables: list[Table] = []

    for idx, item in enumerate(items, start=1):
        where = f"{path.name}, item {idx}"
        if not isinstance(item, dict):
            raise ValueError(f"{where}: expected an object with nome/grupo/dlo.")
        where = f"{where} ({item.get('nome', '?')})"

        unknown = set(item) - set(all_fields)
        if unknown:
            raise ValueError(f"{where}: unknown field(s) {sorted(unknown)}. Valid: {sorted(all_fields)}")

        for fname, fval in item.items():
            expected = expected_type(all_fields[fname].type)
            if not isinstance(fval, expected):
                raise ValueError(f"{where}: '{fname}' should be {all_fields[fname].type}, got {fval!r}")

        try:
            table = Table(**item)
        except TypeError as exc:
            raise ValueError(f"{where}: {exc}") from exc

        missing_origins = set(ORIGINS) - set(table.dlo)
        if missing_origins:
            raise ValueError(f"{where}: dlo missing origin(s) {sorted(missing_origins)}.")
        extra_origins = set(table.dlo) - set(ORIGINS)
        if extra_origins:
            raise ValueError(f"{where}: dlo has invalid origin(s) {sorted(extra_origins)}. Use {list(ORIGINS)}.")

        # Validate identifiers that go into SQL to prevent injection.
        validate_identifier(table.order_by, f"{where}.order_by")
        validate_identifier(table.coluna_modificacao, f"{where}.coluna_modificacao")

        # primary_key must be a valid snake_case name.
        if not __import__("re").match(r"^[a-z][a-z0-9_]*$", table.primary_key):
            raise ValueError(
                f"{where}: primary_key '{table.primary_key}' is not valid snake_case."
            )

        tables.append(table)

    names = [t.nome for t in tables]
    duplicates = {n for n, count in Counter(names).items() if count > 1}
    if duplicates:
        raise ValueError(f"{path.name}: duplicate name(s) {sorted(duplicates)}.")
    return tables


TABLES: list[Table] = load_tables(PIPELINE_DIR / "tabelas.yaml")

NomeTabela = Literal[tuple(t.nome for t in TABLES)]  # type: ignore[valid-type]
Origem = Literal[ORIGINS]  # type: ignore[valid-type]
