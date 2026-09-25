"""Testes de caracterização do flow rj_rmi__run_dbt."""

import pytest

from pipelines.rj_rmi__run_dbt import flow


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict]]:
    """Troca as tasks do flow por funções que registram as chamadas."""
    record: list[tuple[str, dict]] = []

    def setup_credentials() -> None:
        record.append(("setup_credentials_task", {}))

    def clone_repository() -> str:
        record.append(("clone_repository_task", {}))
        return "/clone"

    def run_dbt(**parameters: str) -> None:
        record.append(("run_dbt_task", parameters))

    monkeypatch.setattr(flow, "setup_credentials_task", setup_credentials)
    monkeypatch.setattr(flow, "clone_repository_task", clone_repository)
    monkeypatch.setattr(flow, "run_dbt_task", run_dbt)
    return record


def test_flow_runs_the_tasks_in_order(calls: list[tuple[str, dict]]) -> None:
    flow.rj_rmi__run_dbt.fn(
        command="test", select="tag:daily", flag="--empty", target="prod"
    )

    assert calls == [
        ("setup_credentials_task", {}),
        ("clone_repository_task", {}),
        (
            "run_dbt_task",
            {
                "project_dir": "/clone",
                "command": "test",
                "select": "tag:daily",
                "flag": "--empty",
                "target": "prod",
            },
        ),
    ]


def test_flow_defaults_to_building_in_dev(
    calls: list[tuple[str, dict]],
) -> None:
    flow.rj_rmi__run_dbt.fn()

    assert calls[-1] == (
        "run_dbt_task",
        {
            "project_dir": "/clone",
            "command": "build",
            "select": "",
            "flag": "",
            "target": "dev",
        },
    )
