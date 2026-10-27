"""MySQL, read with `aiomysql`: the configuration of the `MySQLEngine`."""

from typing import ClassVar

from pydantic import PositiveInt
from pydantic.dataclasses import dataclass

from ....constants import MYSQL_BATCH_ROWS
from ...constants import SOURCE_CONFIG
from ..base import Registered
from ..protocols import DBConn


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class MySQLSource(Registered):
    """A MySQL database. `charset` is the character set of the connection.

    It is `utf8mb4` by default, and a MySQL 5.0 or 5.1 needs `utf8` or `latin1`, or
    `legacy` for a column that holds Latin-1 bytes under a `utf8` declaration.
    `batch_size` is the rows of one fetch.
    """

    name: ClassVar[str] = "mysql"
    probe_kind: ClassVar[str] = "mysql-range-1"
    range_column_needed: ClassVar[bool] = True

    conn: DBConn
    charset: str = "utf8mb4"
    batch_size: PositiveInt = MYSQL_BATCH_ROWS
