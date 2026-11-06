"""Format the summaries that the tasks log."""

from collections.abc import Iterable, Sequence
from datetime import timedelta

from pipelines.rj_smas__cadunico.constants import TABLE_NAMES
from pipelines.rj_smas__cadunico.types import Record

MEBIBYTE = 1 << 20
GIBIBYTE = 1 << 30


def format_duration(seconds: float) -> str:
    """Return a duration as `H:MM:SS`."""
    return str(timedelta(seconds=int(seconds)))


def format_size(size: int) -> str:
    """Return a number of bytes in megabytes, or in gigabytes from one up."""
    return f"{size / GIBIBYTE:.2f} GB" if size >= GIBIBYTE else f"{size / MEBIBYTE:.1f} MB"


def format_count(count: int) -> str:
    """Return a number with a dot between the thousands, as `1.234.567`."""
    return f"{count:,}".replace(",", ".")


def short_name(name: str) -> str:
    """Return the name of a bucket file without its folders."""
    return name.rpartition("/")[-1]


def compare_keys(raw: Iterable[str], staged: Iterable[str]) -> str:
    """Return a line for each key, saying where it is and what happens to it.

    ```python
    print(compare_keys(["a", "b"], ["a", "c"]))
    #    a  raw=✅ staging=✅ → já carregado
    #    b  raw=✅ staging=❌ → será carregado
    #    c  raw=❌ staging=✅ → só no staging
    ```
    """
    raw_keys, staged_keys = set(raw), set(staged)
    lines: list[str] = []

    for key in sorted(raw_keys | staged_keys):
        in_raw, in_staging = key in raw_keys, key in staged_keys
        action = "já carregado" if in_raw and in_staging else "será carregado" if in_raw else "só no staging"
        lines.append(f"   {key}  raw={'✅' if in_raw else '❌'} staging={'✅' if in_staging else '❌'} → {action}")

    return "\n".join(lines)


def number_lines(items: Sequence[str]) -> str:
    """Return the items as a numbered list, one for each line."""
    return "\n".join(f"   {index}. {item}" for index, item in enumerate(items, 1))


def summarize_models(rows: Sequence[Record]) -> str:
    """Return a line for each type of record, by number, with its layout versions.

    ```python
    rows = [
        {"reg": "1", "versao_layout_particao": "0601"},
        {"reg": "1", "versao_layout_particao": "0629"},
    ]
    print(summarize_models(rows))
    #    📄 identificacao_controle (01): 2 versões, 0601 → 0629
    ```
    """
    lines: list[str] = []

    for register in sorted({row["reg"] or "" for row in rows}, key=lambda register: register.zfill(2)):
        versions = sorted({row["versao_layout_particao"] or "" for row in rows if row["reg"] == register})
        number = register.zfill(2)
        name = TABLE_NAMES.get(number, f"reg_{register}")
        count = f"{len(versions)} versão" if len(versions) == 1 else f"{len(versions)} versões"
        lines.append(f"   📄 {name} ({number}): {count}, {versions[0]} → {versions[-1]}")

    return "\n".join(lines)
