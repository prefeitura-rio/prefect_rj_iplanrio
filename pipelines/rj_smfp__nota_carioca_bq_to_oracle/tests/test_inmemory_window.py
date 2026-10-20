from datetime import date
from pathlib import Path
from types import SimpleNamespace

import oracledb
import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import inmemory
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import MANAGED_TABLE_MARKER
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.slots import SlotPlan
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.structure import LOADED_TABLE_INMEMORY


def bound(day: str) -> str:
    return f"TO_DATE(' {day} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"


@pytest.mark.parametrize(
    ("today", "months", "expected"),
    [
        (date(2026, 10, 20), 3, date(2026, 7, 1)),
        (date(2026, 10, 1), 3, date(2026, 7, 1)),
        (date(2026, 2, 28), 3, date(2025, 11, 1)),
        (date(2026, 1, 15), 12, date(2025, 1, 1)),
        (date(2026, 10, 20), 0, date(2026, 10, 1)),
    ],
)
def test_window_start_is_the_first_day_of_the_month_months_back(today, months, expected):
    assert inmemory.window_start(today, months) == expected


def test_window_start_refuses_a_negative_window():
    with pytest.raises(ValueError, match="negativa"):
        inmemory.window_start(date(2026, 10, 20), -1)


def test_partition_upper_bound_reads_dates_and_ignores_maxvalue():
    assert inmemory.partition_upper_bound(bound("2026-08-01")) == date(2026, 8, 1)
    assert inmemory.partition_upper_bound(" " + bound("2031-01-01") + " ") == date(2031, 1, 1)
    assert inmemory.partition_upper_bound("MAXVALUE") is None
    assert inmemory.partition_upper_bound("'RJ'") is None


def test_select_window_keeps_the_current_month_the_three_before_and_the_future():
    partitions = [
        {"partition_name": "P_INITIAL", "high_value": bound("2026-01-01")},
        {"partition_name": "P_202606", "high_value": bound("2026-07-01")},
        {"partition_name": "P_202607", "high_value": bound("2026-08-01")},
        {"partition_name": "P_202610", "high_value": bound("2026-11-01")},
        {"partition_name": "P_203012", "high_value": bound("2031-01-01")},
        {"partition_name": "SYS_P101", "high_value": bound("2026-10-01")},
        {"partition_name": "P_MAX", "high_value": "MAXVALUE"},
    ]
    start = inmemory.window_start(date(2026, 10, 20), 3)
    assert inmemory.select_window(partitions, start) == ["P_202607", "P_202610", "P_203012", "SYS_P101", "P_MAX"]


def oracle_error(code: int) -> oracledb.DatabaseError:
    return oracledb.DatabaseError(SimpleNamespace(code=code, message=f"ORA-{code:05d}"))


class FakeCursor:
    def __init__(self, failures: list[Exception] | None = None) -> None:
        self.failures = list(failures or [])
        self.calls: list[tuple[str, object]] = []

    def execute(self, statement: str, binds: object = None) -> None:
        self.calls.append((" ".join(statement.split()), binds))
        if self.failures and statement.lstrip().startswith("BEGIN"):
            raise self.failures.pop(0)

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None


CALL = 'BEGIN "DFEN".PRC_BQLOAD_INMEMORY(p_tabela => :tabela, p_particao => :particao, p_ativar => :ativar); END;'


def test_procedure_call_is_qualified_by_the_schema_of_the_tables():
    assert " ".join(inmemory.procedure_call("DFEN").split()) == CALL
    assert inmemory.procedure_binds("T_A", "P_1", enabled=True) == {"tabela": "T_A", "particao": "P_1", "ativar": 1}
    assert inmemory.procedure_binds("T_A", None, enabled=False) == {"tabela": "T_A", "particao": None, "ativar": 0}


def test_call_with_retry_retries_only_while_the_object_is_busy(monkeypatch):
    waits, messages = [], []
    monkeypatch.setattr(inmemory.time, "sleep", waits.append)
    monkeypatch.setattr(inmemory, "DDL_ATTEMPTS", 5)
    cursor = FakeCursor([oracle_error(54), oracle_error(54)])
    inmemory.call_with_retry(cursor, CALL, {"tabela": "X"}, messages.append)
    assert cursor.calls == [(CALL, {"tabela": "X"})] * 3
    assert waits == [inmemory.DDL_RETRY_SECONDS] * 2
    assert "tentativa 2/5" in messages[-1]


def test_call_with_retry_gives_up_after_the_last_attempt_and_on_other_errors(monkeypatch):
    monkeypatch.setattr(inmemory.time, "sleep", lambda _: None)
    monkeypatch.setattr(inmemory, "DDL_ATTEMPTS", 3)
    busy = FakeCursor([oracle_error(54)] * 3)
    with pytest.raises(oracledb.DatabaseError):
        inmemory.call_with_retry(busy, CALL, {}, lambda _: None)
    assert len(busy.calls) == inmemory.DDL_ATTEMPTS
    refused = FakeCursor([oracle_error(20904)])
    with pytest.raises(oracledb.DatabaseError):
        inmemory.call_with_retry(refused, CALL, {}, lambda _: None)
    assert len(refused.calls) == 1


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self._cursor


@pytest.fixture
def fake_oracle(monkeypatch):
    cursor = FakeCursor()
    state = SimpleNamespace(cursor=cursor, checked=[], rows=[], queries=[], config=SimpleNamespace(schema="DFEN"))

    def check(_cursor, _schema, table):
        state.checked.append(table)

    def fetch(_cursor, query, binds):
        state.queries.append((query, binds))
        return state.rows

    monkeypatch.setattr(inmemory, "connect", lambda config: FakeConnection(cursor))
    monkeypatch.setattr(inmemory, "assert_managed_existing_table", check)
    monkeypatch.setattr(inmemory, "fetch_rows", fetch)
    return state


def test_enable_window_marks_only_the_window_partitions(fake_oracle):
    fake_oracle.rows = [
        {"partition_name": "P_202605", "high_value": bound("2026-06-01")},
        {"partition_name": "P_202609", "high_value": bound("2026-10-01")},
        {"partition_name": "P_202610", "high_value": bound("2026-11-01")},
    ]
    chosen = inmemory.enable_window(fake_oracle.config, "BQLOAD_T_A", date(2026, 7, 1), lambda _: None)
    assert chosen == ["P_202609", "P_202610"]
    assert fake_oracle.checked == ["BQLOAD_T_A"]
    assert fake_oracle.cursor.calls == [
        (CALL, {"tabela": "BQLOAD_T_A", "particao": "P_202609", "ativar": 1}),
        (CALL, {"tabela": "BQLOAD_T_A", "particao": "P_202610", "ativar": 1}),
    ]


def test_enable_window_puts_an_unpartitioned_table_in_memory_whole(fake_oracle):
    assert inmemory.enable_window(fake_oracle.config, "BQLOAD_T_A", date(2026, 7, 1), lambda _: None) == ["BQLOAD_T_A"]
    assert fake_oracle.cursor.calls == [(CALL, {"tabela": "BQLOAD_T_A", "particao": None, "ativar": 1})]


def test_disable_takes_the_whole_table_out_of_memory(fake_oracle):
    inmemory.disable(fake_oracle.config, "BQLOAD_T_B", lambda _: None)
    assert fake_oracle.checked == ["BQLOAD_T_B"]
    assert fake_oracle.cursor.calls == [(CALL, {"tabela": "BQLOAD_T_B", "particao": None, "ativar": 0})]


def test_ensure_procedure_refuses_before_any_table_when_it_cannot_be_executed(fake_oracle):
    fake_oracle.rows = [{"executable": 0}]
    with pytest.raises(PermissionError, match=r"PRC_BQLOAD_INMEMORY.*DBA"):
        inmemory.ensure_procedure(fake_oracle.config)
    assert fake_oracle.queries == [("get_inmemory_procedure", {"owner": "DFEN", "name": "PRC_BQLOAD_INMEMORY"})]
    fake_oracle.rows = [{"executable": 1}]
    inmemory.ensure_procedure(fake_oracle.config)
    assert fake_oracle.cursor.calls == []


def test_dba_script_applies_the_same_inmemory_clause_and_marker_as_the_pipeline():
    script = (Path(tasks_module.__file__).parent / "dba" / "PRC_BQLOAD_INMEMORY.sql").read_text(encoding="utf-8")
    assert f"PROCEDURE DFEN.{inmemory.PROCEDURE_NAME}" in script
    assert f"'{LOADED_TABLE_INMEMORY}'" in script
    assert f"'{MANAGED_TABLE_MARKER}'" in script
    assert "AUTHID DEFINER" in script


def test_release_frees_the_tables_that_left_use_and_only_warns_on_failure(monkeypatch):
    released, logs, slept = [], [], []

    def disable(config, table, report):
        if table == "BQLOAD_X_B":
            raise oracledb.DatabaseError(SimpleNamespace(code=54, message="ORA-00054"))
        released.append(table)

    monkeypatch.setattr(tasks_module.inmemory, "disable", disable)
    monkeypatch.setattr(tasks_module.oracle, "read_oracle_config", lambda _: None)
    monkeypatch.setattr(tasks_module.time, "sleep", slept.append)
    monkeypatch.setattr(tasks_module, "log", lambda message, level="info": logs.append((level, message)))
    plans = [
        SlotPlan("BQLOAD_T", "BQLOAD_T_A", "BQLOAD_T_B"),
        SlotPlan("BQLOAD_X", "BQLOAD_X_B", "BQLOAD_X_A"),
        SlotPlan("BQLOAD_N", None, "BQLOAD_N_A"),
    ]
    failed = tasks_module.release_inmemory_task.fn(infisical_secret_path="/x", plans=plans, delay_seconds=120)
    assert released == ["BQLOAD_T_A"]
    assert failed == ["BQLOAD_X_B"]
    assert slept == [120]
    assert any(level == "warning" and "BQLOAD_X_B" in message for level, message in logs)


def test_release_does_nothing_on_the_first_swap(monkeypatch):
    slept = []
    monkeypatch.setattr(tasks_module.time, "sleep", slept.append)
    monkeypatch.setattr(tasks_module, "log", lambda *_, **__: None)
    first = [SlotPlan("B", None, "B_A")]
    assert tasks_module.release_inmemory_task.fn(infisical_secret_path="/x", plans=first, delay_seconds=120) == []
    assert slept == []
