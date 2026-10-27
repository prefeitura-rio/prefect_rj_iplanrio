from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date
from types import SimpleNamespace
from typing import cast

import oracledb
import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import DDL_RETRY_SECONDS
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers import inmemory, oracle
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import OracleConfig
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.slots import SlotPlan
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import LOADED_TABLE_INMEMORY
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.types import Load


def bound(day: str) -> str:
    """Return the `HIGH_VALUE` text that the dictionary gives for a date."""
    return f"TO_DATE(' {day} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"


@pytest.mark.parametrize(
    ("today", "months", "expected"),
    [
        pytest.param(date(2026, 10, 20), 3, date(2026, 7, 1), id="three_months_back"),
        pytest.param(date(2026, 10, 1), 3, date(2026, 7, 1), id="the_first_of_the_month"),
        pytest.param(date(2026, 2, 28), 3, date(2025, 11, 1), id="across_the_year"),
        pytest.param(date(2026, 1, 15), 12, date(2025, 1, 1), id="a_whole_year"),
        pytest.param(date(2026, 10, 20), 0, date(2026, 10, 1), id="only_the_current_month"),
    ],
)
def test_window_start_is_the_first_day_of_the_month_months_back(today: date, months: int, expected: date) -> None:
    """Go back `months` months from the month of `today`."""
    assert inmemory.window_start(today, months) == expected


def test_window_start_refuses_a_negative_window() -> None:
    """Raise for a window of less than zero months."""
    with pytest.raises(ValueError, match="negativa"):
        inmemory.window_start(date(2026, 10, 20), -1)


@pytest.mark.parametrize(
    ("high_value", "expected"),
    [
        pytest.param(bound("2026-08-01"), date(2026, 8, 1), id="a_date"),
        pytest.param(" " + bound("2031-01-01") + " ", date(2031, 1, 1), id="a_date_with_spaces"),
        pytest.param("MAXVALUE", None, id="maxvalue"),
        pytest.param("'RJ'", None, id="a_list_value"),
    ],
)
def test_partition_upper_bound_reads_dates_and_ignores_the_rest(high_value: str, expected: date | None) -> None:
    """Read the date of a bound, and give nothing for a bound that is not a date."""
    assert inmemory.partition_upper_bound(high_value) == expected


def test_select_window_keeps_the_current_month_the_three_before_and_the_future() -> None:
    """Keep the partitions with data from the window on, and the unbounded ones."""
    partitions: list[dict[str, object]] = [
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
    """Return an Oracle error with a code."""
    return oracledb.DatabaseError(SimpleNamespace(code=code, message=f"ORA-{code:05d}"))


class FakeCursor:
    """A cursor that records the statements and fails the first `ALTER TABLE`s."""

    def __init__(self, failures: Sequence[Exception] | None = None) -> None:
        """Start with the failures to raise, and no statement."""
        self.failures: list[Exception] = list(failures or [])
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        """Record the statement, or raise the next failure for an `ALTER TABLE`."""
        self.statements.append(" ".join(statement.split()))
        if self.failures and statement.startswith("ALTER TABLE"):
            raise self.failures.pop(0)

    def __enter__(self) -> "FakeCursor":
        """Return the cursor."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close nothing."""


def as_cursor(cursor: FakeCursor) -> oracledb.Cursor:
    """Give the fake cursor the type of a real one."""
    return cast("oracledb.Cursor", cast("object", cursor))


def never_wait(_: float) -> None:
    """Take a wait and do nothing."""


def never(_: str) -> None:
    """Take a message and do nothing."""


def test_execute_ddl_retries_only_while_the_object_is_busy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Try again after `ORA-00054`, waiting between attempts."""
    waits: list[float] = []
    messages: list[str] = []
    monkeypatch.setattr(inmemory, "sleep", waits.append)
    cursor = FakeCursor([oracle_error(54), oracle_error(54)])

    inmemory.execute_ddl(as_cursor(cursor), "ALTER TABLE X NO INMEMORY", messages.append, attempts=5)

    assert cursor.statements == ["ALTER TABLE X NO INMEMORY"] * 3
    assert waits == [DDL_RETRY_SECONDS] * 2
    assert "tentativa 2/5" in messages[-1]


def test_execute_ddl_gives_up_after_the_last_attempt_and_on_other_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise when the attempts end, and at once for an error that is not `ORA-00054`."""
    monkeypatch.setattr(inmemory, "sleep", never_wait)
    busy = FakeCursor([oracle_error(54)] * 3)
    refused = FakeCursor([oracle_error(1031)])

    with pytest.raises(oracledb.DatabaseError):
        inmemory.execute_ddl(as_cursor(busy), "ALTER TABLE X", never, attempts=3)
    with pytest.raises(oracledb.DatabaseError):
        inmemory.execute_ddl(as_cursor(refused), "ALTER TABLE X", never, attempts=5)

    assert len(busy.statements) == 3
    assert len(refused.statements) == 1


class FakeConnection:
    """A connection that gives its cursor, and works as a context manager."""

    def __init__(self, cursor: FakeCursor) -> None:
        """Keep the cursor to give."""
        self.held: FakeCursor = cursor

    def __enter__(self) -> "FakeConnection":
        """Return the connection."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close nothing."""

    def cursor(self) -> FakeCursor:
        """Return the cursor."""
        return self.held


@dataclass
class FakeOracle:
    """What the fake Oracle holds: cursor, config, checked tables and partitions."""

    cursor: FakeCursor
    config: OracleConfig
    checked: list[str] = field(default_factory=list[str])
    partitions: list[dict[str, object]] = field(default_factory=list[dict[str, object]])


@pytest.fixture
def fake_oracle(monkeypatch: pytest.MonkeyPatch) -> FakeOracle:
    """Replace the connection, the ownership check and the partitions read."""
    cursor = FakeCursor()
    state = FakeOracle(cursor, OracleConfig("u", "p", "h", "1521", "s", "DFEN"))

    def connect(config: object) -> FakeConnection:
        """Give the connection with the fake cursor."""
        del config

        return FakeConnection(cursor)

    def check(_cursor: object, _schema: str, table: str) -> None:
        """Record the table that was checked."""
        state.checked.append(table)

    def fetch_rows(cursor: object, query: str, binds: object) -> list[dict[str, object]]:
        """Give the partitions of the test."""
        del cursor, query, binds

        return state.partitions

    monkeypatch.setattr(inmemory, "connect", connect)
    monkeypatch.setattr(inmemory, "assert_managed_existing_table", check)
    monkeypatch.setattr(inmemory, "fetch_rows", fetch_rows)

    return state


def test_enable_window_marks_only_the_window_partitions(fake_oracle: FakeOracle) -> None:
    """Alter only the partitions of the window, after the check and the timeout."""
    fake_oracle.partitions = [
        {"partition_name": "P_202605", "high_value": bound("2026-06-01")},
        {"partition_name": "P_202609", "high_value": bound("2026-10-01")},
        {"partition_name": "P_202610", "high_value": bound("2026-11-01")},
    ]

    chosen = inmemory.enable_window(fake_oracle.config, "BQLOAD_T_A", date(2026, 7, 1), never)

    assert chosen == ["P_202609", "P_202610"]
    assert fake_oracle.checked == ["BQLOAD_T_A"]
    assert fake_oracle.cursor.statements == [
        "ALTER SESSION SET DDL_LOCK_TIMEOUT = 8",
        f'ALTER TABLE "DFEN"."BQLOAD_T_A" MODIFY PARTITION "P_202609" {LOADED_TABLE_INMEMORY}',
        f'ALTER TABLE "DFEN"."BQLOAD_T_A" MODIFY PARTITION "P_202610" {LOADED_TABLE_INMEMORY}',
    ]


def test_enable_window_puts_an_unpartitioned_table_in_memory_whole(fake_oracle: FakeOracle) -> None:
    """Alter the table itself when it has no partitions."""
    chosen = inmemory.enable_window(fake_oracle.config, "BQLOAD_T_A", date(2026, 7, 1), never)

    assert chosen == ["BQLOAD_T_A"]
    assert fake_oracle.cursor.statements[-1] == f'ALTER TABLE "DFEN"."BQLOAD_T_A" {LOADED_TABLE_INMEMORY}'


def test_disable_takes_the_whole_table_out_of_memory(fake_oracle: FakeOracle) -> None:
    """Alter the table to `NO INMEMORY`, after the check and the lock timeout."""
    inmemory.disable(fake_oracle.config, "BQLOAD_T_B", never)

    assert fake_oracle.checked == ["BQLOAD_T_B"]
    assert fake_oracle.cursor.statements == [
        "ALTER SESSION SET DDL_LOCK_TIMEOUT = 8",
        'ALTER TABLE "DFEN"."BQLOAD_T_B" NO INMEMORY',
    ]


def patch_release(
    monkeypatch: pytest.MonkeyPatch, disable: Callable[[object, str, Callable[[str], None]], None]
) -> list[float]:
    """Replace the `disable`, the config and the sleep of the release task."""
    slept: list[float] = []

    def read_config() -> None:
        """Give no config: the fake `disable` does not use it."""

    monkeypatch.setattr(inmemory, "disable", disable)
    monkeypatch.setattr(oracle, "read_oracle_config", read_config)
    monkeypatch.setattr("time.sleep", slept.append)

    return slept


def test_release_frees_the_tables_that_left_use_and_only_warns_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Free each old table after the delay, and report one that could not be freed."""
    released: list[str] = []

    def disable(config: object, table: str, report: Callable[[str], None]) -> None:
        """Fail for one table and record the others."""
        del config, report
        if table == "BQLOAD_X_B":
            raise oracle_error(54)
        released.append(table)

    slept = patch_release(monkeypatch, disable)
    plans = [
        SlotPlan("BQLOAD_T", "BQLOAD_T_A", "BQLOAD_T_B"),
        SlotPlan("BQLOAD_X", "BQLOAD_X_B", "BQLOAD_X_A"),
        SlotPlan("BQLOAD_N", None, "BQLOAD_N_A"),
    ]

    failed = tasks_module.release_inmemory_task.fn(plans=plans, load=Load(inmemory_release_delay_seconds=120))

    assert released == ["BQLOAD_T_A"]
    assert failed == ["BQLOAD_X_B"]
    assert slept == [120]


def test_release_does_nothing_on_the_first_swap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Free nothing, and wait for nothing, when no table left use."""

    def disable(config: object, table: str, report: Callable[[str], None]) -> None:
        """Fail the test if it is called."""
        del config, table, report
        raise AssertionError("nothing left use")

    slept = patch_release(monkeypatch, disable)
    first = [SlotPlan("B", None, "B_A")]

    assert tasks_module.release_inmemory_task.fn(plans=first, load=Load()) == []
    assert slept == []
