"""The Oracle source: the configuration that the `OracleEngine` connects with."""

from typing import ClassVar

from pydantic import PositiveInt
from pydantic.dataclasses import dataclass

from ....constants import ORACLE_BATCH_ROWS
from ...constants import SOURCE_CONFIG
from ..base import Registered
from ..protocols import DBConn


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class OracleSource(Registered):
    """An Oracle database. `batch_size` is the rows of one fetch."""

    name: ClassVar[str] = "oracle"
    probe_kind: ClassVar[str] = "oracle-rowscn-1"

    conn: DBConn
    batch_size: PositiveInt = ORACLE_BATCH_ROWS
