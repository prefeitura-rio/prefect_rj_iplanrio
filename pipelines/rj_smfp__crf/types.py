"""Types of rj_smfp__crf."""

from dataclasses import dataclass
from typing import Literal

type CrfTable = Literal["periodos_simples", "periodos_mei", "eventos_simples", "eventos_mei"]


@dataclass(frozen=True)
class FwfLayout:
    """How to read a table of fixed width fields.

    `colspecs` are the `(start, end)` positions of the columns and `names` their names,
    in the same order. `file_pattern` is the glob of the file inside the ZIP.
    """

    colspecs: tuple[tuple[int, int], ...]
    names: tuple[str, ...]
    file_pattern: str
    encoding: str = "utf-8"
