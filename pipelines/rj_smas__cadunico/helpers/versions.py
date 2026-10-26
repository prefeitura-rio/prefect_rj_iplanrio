"""Follow the columns of a type of record from one layout version to the next."""

from collections.abc import Sequence
from itertools import pairwise

from pipelines.rj_smas__cadunico.constants import TABLE_NAMES
from pipelines.rj_smas__cadunico.types import Record


def order_key(row: Record) -> tuple[str, str]:
    """Return the key that orders the rows: the type of record, then the layout version."""
    return row["reg"] or "", row["versao_layout_particao"] or ""


def sort_rows(rows: Sequence[Record], *, descending: bool = False) -> list[Record]:
    """Return the rows by type of record and layout version.

    Rows that tie keep their order.
    """
    return sorted(rows, key=order_key, reverse=descending)


def compare_versions(rows: Sequence[Record]) -> tuple[list[Record], list[str]]:
    """Compare each version of a type of record with the one before it, in the order of the rows.

    The rows of a version get `versao_layout_anterior`, the version before it, and
    `coluna_esta_versao_anterior`, `True` if the column is in that version. The first
    version is its own previous one. The second value lists a warning for each version
    that has columns that the one before it lacks.
    """
    compared: list[Record] = []
    warnings: list[str] = []
    registers = list(dict.fromkeys(row["reg"] for row in rows))

    for register in registers:
        table = [row for row in rows if row["reg"] == register]
        versions = list(dict.fromkeys(row["versao_layout_particao"] for row in table))

        for current, following in pairwise([versions[0], *versions]):
            known = {row["column"] for row in table if row["versao_layout_particao"] == current}
            added = [row for row in table if row["versao_layout_particao"] == following]
            compared += [
                {**row, "versao_layout_anterior": current, "coluna_esta_versao_anterior": str(row["column"] in known)}
                for row in added
            ]
            missing = [str(row["column"]) for row in added if row["column"] not in known]

            if missing:
                number = (register or "").zfill(2)
                name = TABLE_NAMES.get(number, f"reg_{register}")
                warnings.append(
                    f"Tabela {number} ({name}) v{following}: Colunas ausentes em v{current}: [{', '.join(missing)}]"
                )

    return sort_rows(compared), warnings


def spread_missing(compared: Sequence[Record], *, ascending: bool) -> list[Record]:
    """Return the rows of the columns that a version lacks, once for it and once for each version on the far side.

    These rows tell a version to select the column as `NULL`, so the versions can join
    in one query. The far side is the older versions when `ascending`, else the newer
    ones.
    """
    versions = sorted({row["versao_layout_particao"] or "" for row in compared})
    spread: list[Record] = []

    for row in compared:
        if row["coluna_esta_versao_anterior"] != "False":
            continue

        spread.append(row)
        previous = row["versao_layout_anterior"] or ""
        spread += [
            {**row, "versao_layout_anterior": version}
            for version in versions
            if (version < previous if ascending else version > previous)
        ]

    return spread


def control_versions(rows: Sequence[Record]) -> tuple[list[Record], list[str]]:
    """Return the layout rows with the control of their columns between versions, and the warnings.

    A first pass goes from the oldest version to the newest, and a second one the other
    way. The result has every row once as it is, and the rows of the columns that a
    version lacks, from both passes.
    """
    up, warnings = compare_versions(sort_rows(rows))
    down, _ = compare_versions(sort_rows(rows, descending=True))
    kept = [{**row, "coluna_esta_versao_anterior": "True"} for row in up]
    extra = spread_missing(up, ascending=True) + spread_missing(down, ascending=False)

    return sort_rows(kept + extra), warnings
