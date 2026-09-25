"""Testes do flow rj_rmi__run_dbt."""

import pytest

from pipelines.rj_rmi__run_dbt import flow


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict]]:
    """Troca as tasks do flow por funções que registram as chamadas.

    :param monkeypatch: Fixture do pytest que desfaz as trocas no fim.
    :returns: As chamadas, com a task e os parâmetros de cada uma.
    """
    record: list[tuple[str, dict]] = []

    def setup_credentials() -> None:
        """Registra a chamada."""
        record.append(("setup_credentials_task", {}))

    def clone_repository() -> str:
        """Registra a chamada e devolve o caminho do clone.

        :returns: O caminho ``/clone``.
        """
        record.append(("clone_repository_task", {}))
        return "/clone"

    def run_dbt(**parameters: str) -> None:
        """Registra a chamada com os parâmetros.

        :param parameters: Parâmetros passados ao ``run_dbt_task``.
        """
        record.append(("run_dbt_task", parameters))

    monkeypatch.setattr(flow, "setup_credentials_task", setup_credentials)
    monkeypatch.setattr(flow, "clone_repository_task", clone_repository)
    monkeypatch.setattr(flow, "run_dbt_task", run_dbt)
    return record


def test_flow_runs_the_tasks_in_order(calls: list[tuple[str, dict]]) -> None:
    """Confere a ordem das tasks e os parâmetros do ``run_dbt_task``."""
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
    """Confere o padrão: ``build`` em ``dev``, sem ``select`` nem ``flag``."""
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
