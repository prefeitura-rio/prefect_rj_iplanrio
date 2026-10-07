from dataclasses import dataclass, field

import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import ParallelRunError, encode_proof
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import TablePlan
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.publish import PublishRequest, preflight, publish_all
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import LayoutError, TableLayout, TableState

RUN_ID = "run-1"
SCN = 777
RESTORE_MS = 1_700_000_000_000
LAYOUT = TableLayout("DAY", "_airbyte_extracted_at", ("_airbyte_extracted_at",))
COLUMNS = (OracleColumn("C", "VARCHAR2", None, None),)
NAMES = ("DPS", "NOTAS_NACIONAIS", "PESSOAS_NACIONAIS")


class CopyFailedError(RuntimeError):
    pass


def plan_for(name: str) -> TablePlan:
    return TablePlan(name, "DFEN", COLUMNS, (), LAYOUT, None)


def temp_state(rows: int = 10, layout: TableLayout = LAYOUT, run_id: str = RUN_ID) -> TableState:
    return TableState((), layout, encode_proof(run_id, SCN, rows), rows)


def final_state(layout: TableLayout = LAYOUT) -> TableState:
    return TableState((), layout, {}, 1)


@dataclass
class FakeStore:
    tables: dict[str, TableState]
    fail_publish_of: str | None = None
    fail_restore_of: str | None = None
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def read(self, table_id: str) -> TableState | None:
        return self.tables.get(table_id)

    def publish(self, temp_id: str, final_id: str) -> None:
        if final_id == self.fail_publish_of:
            raise CopyFailedError(f"incompatible clustering fields in {final_id}")
        self.calls.append(("publish", final_id))

    def restore(self, table_id: str, timestamp_ms: int) -> None:
        if table_id == self.fail_restore_of:
            raise CopyFailedError("restore falhou")
        self.calls.append(("restore", table_id, str(timestamp_ms)))

    def drop(self, table_id: str) -> None:
        self.calls.append(("drop", table_id))


def healthy_tables() -> dict[str, TableState]:
    return {
        **{f"{name}__oracle_to_bq_tmp": temp_state() for name in NAMES},
        **{name: final_state() for name in NAMES},
    }


def request() -> PublishRequest:
    return PublishRequest([plan_for(name) for name in NAMES], RUN_ID, SCN, now_ms=lambda: RESTORE_MS)


def test_publish_replaces_every_table_and_restores_nothing_when_all_copies_succeed() -> None:
    store = FakeStore(healthy_tables())
    lines: list[str] = []

    publish_all(store, request(), lines.append)

    assert store.calls == [("publish", name) for name in NAMES]
    assert any(str(RESTORE_MS) in line for line in lines)


def test_incompatible_layout_of_the_last_table_blocks_before_the_first_copy() -> None:
    tables = healthy_tables()
    tables["PESSOAS_NACIONAIS"] = final_state(TableLayout("DAY", "_airbyte_extracted_at", ("PESSOA_NACIONAL",)))
    store = FakeStore(tables)

    with pytest.raises(LayoutError, match="PESSOAS_NACIONAIS"):
        publish_all(store, request(), lambda _: None)

    assert store.calls == []


def test_missing_temp_or_wrong_proof_of_any_table_blocks_before_the_first_copy() -> None:
    missing = healthy_tables()
    del missing["PESSOAS_NACIONAIS__oracle_to_bq_tmp"]
    stale = healthy_tables()
    stale["NOTAS_NACIONAIS__oracle_to_bq_tmp"] = temp_state(run_id="outro-run")
    unmarked = healthy_tables()
    unmarked["PESSOAS_NACIONAIS__oracle_to_bq_tmp"] = TableState((), LAYOUT, {}, 10)

    for tables in (missing, stale, unmarked):
        store = FakeStore(tables)
        with pytest.raises(ParallelRunError):
            publish_all(store, request(), lambda _: None)
        assert store.calls == []


def test_preflight_reports_which_finals_already_exist() -> None:
    tables = healthy_tables()
    del tables["DPS"]

    assert preflight(FakeStore(tables), request()) == {"NOTAS_NACIONAIS", "PESSOAS_NACIONAIS"}


def test_failure_in_the_third_copy_restores_only_the_first_two_and_reraises_the_original_error() -> None:
    store = FakeStore(healthy_tables(), fail_publish_of="PESSOAS_NACIONAIS")

    with pytest.raises(CopyFailedError, match="incompatible clustering"):
        publish_all(store, request(), lambda _: None)

    assert store.calls == [
        ("publish", "DPS"),
        ("publish", "NOTAS_NACIONAIS"),
        ("restore", "NOTAS_NACIONAIS", str(RESTORE_MS)),
        ("restore", "DPS", str(RESTORE_MS)),
    ]


def test_failure_in_the_first_copy_restores_nothing() -> None:
    store = FakeStore(healthy_tables(), fail_publish_of="DPS")

    with pytest.raises(CopyFailedError):
        publish_all(store, request(), lambda _: None)

    assert store.calls == []


def test_rollback_drops_a_final_table_that_this_run_created() -> None:
    tables = healthy_tables()
    del tables["DPS"]
    store = FakeStore(tables, fail_publish_of="PESSOAS_NACIONAIS")

    with pytest.raises(CopyFailedError):
        publish_all(store, request(), lambda _: None)

    assert store.calls[2:] == [("restore", "NOTAS_NACIONAIS", str(RESTORE_MS)), ("drop", "DPS")]


def test_rollback_keeps_restoring_after_a_restore_failure_and_names_the_unrestored_table() -> None:
    store = FakeStore(healthy_tables(), fail_publish_of="PESSOAS_NACIONAIS", fail_restore_of="NOTAS_NACIONAIS")

    with pytest.raises(CopyFailedError, match="incompatible clustering") as raised:
        publish_all(store, request(), lambda _: None)

    assert ("restore", "DPS", str(RESTORE_MS)) in store.calls
    assert any("NOTAS_NACIONAIS" in note for note in raised.value.__notes__)
