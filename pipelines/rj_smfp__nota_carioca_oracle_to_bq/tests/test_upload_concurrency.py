from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq import flow as flow_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import Snapshot
from prefect_rj_iplanrio.sql import load_query

TAKEN_AT = datetime(2026, 10, 2, 12, 30, 5, tzinfo=UTC)


@dataclass(frozen=True)
class FakePlan:
    table_id: str


class Stop(Exception):
    """Interrompe o flow assim que os filhos seriam lançados."""


def test_upload_concurrency_defaults_to_four() -> None:
    # Given default options
    # Then four uploads run at once, to mitigate slow TCP flows
    assert ExtractOptions().upload_concurrency == 4


@pytest.mark.parametrize("value", [0, -1])
def test_upload_concurrency_below_one_is_rejected(value: int) -> None:
    # Given an invalid concurrency
    # When the options are built
    with pytest.raises(ValueError, match="upload_concurrency"):
        ExtractOptions(upload_concurrency=value)


def test_parent_passes_upload_concurrency_to_children(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a parent flow with tasks replaced by fakes and a custom upload_concurrency
    captured: dict[str, object] = {}

    def fake_launch(table_ids: list[str], snapshot: Snapshot, passthrough: dict[str, object]) -> None:
        captured.update(passthrough)
        raise Stop

    def fake_take_snapshot(infisical_secret_path: str) -> Snapshot:
        return Snapshot(scn=1, taken_at=TAKEN_AT)

    def fake_plan(**kwargs: object) -> FakePlan:
        return FakePlan(table_id=str(kwargs["table_id"]))

    def fake_budget(plans: list[FakePlan], options: ExtractOptions) -> ExtractOptions:
        return options

    def ignore(**_: object) -> None:
        return None

    for name, fake in {
        "rename_current_flow_run_task": ignore,
        "inject_bd_credentials_task": ignore,
        "ensure_exclusive_task": ignore,
        "drop_leftover_chunk_tasks_task": ignore,
        "take_snapshot_task": fake_take_snapshot,
        "plan_table_task": fake_plan,
        "check_memory_budget_task": fake_budget,
        "launch_children_task": fake_launch,
        "cleanup_task": ignore,
    }.items():
        monkeypatch.setattr(flow_module, name, fake)
    # When the parent runs up to the child launch
    with pytest.raises(Stop):
        flow_module.rj_smfp__nota_carioca_oracle_to_bq.fn(table_ids=["DPS"], upload_concurrency=3)
    # Then the child parameters carry it
    assert captured["upload_concurrency"] == 3


def test_select_chunk_always_reads_as_of_scn() -> None:
    # Given the SELECT rendered for every chunk
    sql = load_query(QUERIES_ANCHOR, "select_chunk", columns="A", schema="S", table="T")
    # Then it binds the SCN and starts with the explanatory comment
    assert "AS OF SCN :scn" in sql
    assert sql.startswith("--")
