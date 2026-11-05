from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from types import TracebackType

import pytest

from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.config import OracleConfig
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.tasks_db import OracleTaskStore, TaskRow, row_from_record

CONFIG = OracleConfig(user="u", password="segredo", host="h", port="1521", service_name="s")
COLUMNS = (
    "TAREFA",
    "TIPO",
    "STATUS",
    "DATA_INICIO",
    "DATA_TERMINO",
    "EXECUCAO_PROGRESSO",
    "EXECUCAO_RESUMO",
    "SEGUNDOS_EM_EXECUCAO",
)
STARTED = datetime(2026, 10, 29, 17, 0, 5)
FINISHED = datetime(2026, 10, 29, 18, 2, 0)


@dataclass
class Column:
    name: str


@dataclass
class FakeCursor:
    rows: list[tuple[object, ...]]
    executed: list[tuple[str, dict[str, object] | None]] = field(default_factory=list)
    description: list[Column] | None = None

    def execute(self, sql: str, binds: dict[str, object] | None = None) -> None:
        self.executed.append((sql, binds))
        if sql.lstrip().upper().startswith("SELECT"):
            self.description = [Column(name) for name in COLUMNS]

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_exc: type[BaseException] | BaseException | TracebackType | None) -> None:
        return None


@dataclass
class FakeConnection:
    cursor_obj: FakeCursor
    rolled_back: bool = False

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def rollback(self) -> None:
        self.rolled_back = True

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *_exc: type[BaseException] | BaseException | TracebackType | None) -> None:
        return None


def store_with(rows: list[tuple[object, ...]]) -> tuple[OracleTaskStore, FakeConnection]:
    connection = FakeConnection(FakeCursor(rows))
    return OracleTaskStore(CONFIG, open_connection=lambda _config: connection), connection  # type: ignore[arg-type,return-value]


def test_get_reads_the_task_in_a_read_only_transaction() -> None:
    store, connection = store_with([(134191, 46, 2, STARTED, FINISHED, Decimal(100), "Concluído", Decimal(3715))])

    row = store.get(134191)

    assert row == TaskRow(134191, 46, 2, STARTED, FINISHED, 100, "Concluído", 3715)
    executed = connection.cursor_obj.executed
    assert executed[0][0] == "SET TRANSACTION READ ONLY"
    assert "WHERE tarefa = :tarefa" in executed[1][0]
    assert executed[1][1] == {"tarefa": 134191}
    assert connection.rolled_back


def test_get_returns_none_when_the_task_is_not_visible() -> None:
    store, _ = store_with([])

    assert store.get(1) is None


def test_running_asks_for_extraction_and_post_processing_in_execution() -> None:
    store, connection = store_with([(2, 48, 1, STARTED, None, None, None, 60)])

    rows = store.running()

    assert [row.kind for row in rows] == [48]
    sql, binds = connection.cursor_obj.executed[1]
    assert "status = 1" in sql
    assert binds == {"tipo_extracao": 46, "tipo_pos_processamento": 48}


def test_row_from_record_treats_blank_summary_as_missing_and_rejects_rows_without_status() -> None:
    record = dict(zip((c.lower() for c in COLUMNS), (1, 46, 1, None, None, None, "  ", None), strict=True))

    assert row_from_record(record).summary is None
    record["status"] = None
    with pytest.raises(TypeError, match="status"):
        row_from_record(record)


def test_oracle_config_repr_hides_the_password() -> None:
    assert "segredo" not in repr(CONFIG)
