"""A remoção dos arquivos do GCS roda em paralelo à criação dos índices e uma falha dela ainda falha o flow."""

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle import flow as flow_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.bigquery import ExportedFile, TableSnapshot
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.slots import SlotPlan

TABLES = ["DET", "EXI"]
FILES = {name: [ExportedFile(f"p/{name}/part-0.csv.gz", 10)] for name in TABLES}


class FakeFlowRun:
    """Registra a ordem das tasks; a remoção só termina quando o teste (ou os índices) a liberam."""

    def __init__(self, monkeypatch, delete_error=None, validation_error=None):
        self.events: list[str] = []
        self.deleted: list[str] = []
        self.release_delete = threading.Event()
        self.indexes_ran_while_delete_pending = False
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.delete_error = delete_error
        self.validation_error = validation_error
        self.lock = threading.Lock()

        def record(name, result=None):
            def task(**kwargs):
                with self.lock:
                    self.events.append(name)
                return result(kwargs) if callable(result) else result

            return task

        snapshot = {
            name: TableSnapshot(name, {"fields": [], "num_rows": 1}, datetime(2026, 1, 1, tzinfo=UTC), FILES[name])
            for name in TABLES
        }
        fakes = {
            "rename_current_flow_run_task": record("rename"),
            "inject_bd_credentials_task": record("credentials"),
            "list_tables_task": record("list", TABLES),
            "export_snapshot_task": record("export", snapshot),
            "plan_load_task": record("plan_load", lambda kwargs: kwargs["raw_text_encoding"]),
            "plan_structure_task": record("plan_structure"),
            "resolve_slots_task": record("slots", lambda kwargs: SlotPlan(kwargs["table_id"], None, f"{kwargs['table_id']}_A")),
            "ensure_oracle_table_task": record("ensure", lambda kwargs: kwargs["table"]),
            "drop_oracle_indexes_task": record("drop_indexes", lambda kwargs: kwargs["table"]),
            "truncate_oracle_table_task": record("truncate", lambda kwargs: kwargs["table"]),
            "load_into_oracle_task": record("load", 1),
            "validate_row_count_task": self.validate,
            "create_oracle_indexes_task": self.create_indexes,
            "gather_oracle_stats_task": record("stats", lambda kwargs: kwargs["table"]),
            "grant_access_task": record("grant", lambda kwargs: kwargs["table"]),
            "record_load_task": record("record", lambda kwargs: kwargs["plan"]),
            "start_inmemory_population_task": record("inmemory"),
            "wait_inmemory_task": record("wait_inmemory", lambda kwargs: kwargs["plans"]),
            "swap_synonyms_task": self.swap,
        }
        for name, fake in fakes.items():
            monkeypatch.setattr(flow_module, name, fake)
        monkeypatch.setattr(flow_module, "delete_gcs_files_task", SimpleNamespace(submit=self.submit_delete))
        monkeypatch.setattr(flow_module, "flow_run", SimpleNamespace(id="run"))

    def validate(self, **kwargs):
        self.events.append("validate")
        if self.validation_error:
            raise self.validation_error
        assert kwargs["parallel_degree"] == 4
        return 1

    def submit_delete(self, **kwargs) -> Future:
        self.events.append("delete-submitted")

        def delete():
            assert self.release_delete.wait(timeout=10), "índices esperaram a remoção terminar"
            if self.delete_error:
                raise self.delete_error
            self.deleted += [exported.name for exported in kwargs["files"]]
            self.events.append("delete-finished")

        return self.pool.submit(delete)

    def create_indexes(self, **kwargs):
        self.events.append("indexes")
        self.indexes_ran_while_delete_pending |= "delete-finished" not in self.events
        self.release_delete.set()
        return kwargs["table"]

    def swap(self, **kwargs):
        self.events.append("swap")

    def run(self):
        flow_module.rj_smfp__nota_carioca_bq_to_oracle.fn(table_ids=TABLES, discord_notifications=False)


def test_indexes_run_while_the_files_are_still_being_deleted_and_every_table_is_cleaned(monkeypatch):
    fake = FakeFlowRun(monkeypatch)
    fake.run()
    assert fake.indexes_ran_while_delete_pending
    assert sorted(fake.deleted) == sorted(exported.name for files in FILES.values() for exported in files)
    first_submit = fake.events.index("delete-submitted")
    assert fake.events.index("validate") < first_submit < fake.events.index("indexes")


def test_a_failed_deletion_fails_the_flow_after_the_swap(monkeypatch):
    fake = FakeFlowRun(monkeypatch, delete_error=PermissionError("sem permissão no bucket"))
    with pytest.raises(PermissionError, match="sem permissão"):
        fake.run()
    assert "swap" in fake.events


def test_files_are_not_deleted_when_the_row_count_validation_fails(monkeypatch):
    fake = FakeFlowRun(monkeypatch, validation_error=ValueError("contagens divergentes"))
    with pytest.raises(ValueError, match="divergentes"):
        fake.run()
    assert "delete-submitted" not in fake.events
    assert "indexes" not in fake.events


def test_raw_text_encoding_reaches_the_load_plan(monkeypatch):
    fake = FakeFlowRun(monkeypatch)
    fake.release_delete.set()
    seen = []
    original = flow_module.plan_load_task
    monkeypatch.setattr(flow_module, "plan_load_task", lambda **kwargs: seen.append(kwargs["raw_text_encoding"]) or original(**kwargs))
    flow_module.rj_smfp__nota_carioca_bq_to_oracle.fn(table_ids=TABLES, raw_text_encoding="hex", discord_notifications=False)
    assert seen == ["hex", "hex"]
