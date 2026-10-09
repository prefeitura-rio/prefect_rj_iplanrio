"""Tests for the self-managing schedule (pause when idle, resume when sessions exist)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pipelines.rj_iplanrio__nf_agent import tasks

SETTINGS = SimpleNamespace(nf_batch_jobs_table="p.d.t")


def test_submit_task_retries_on_transient_failure():
    assert tasks.submit_task.retries == 3  # noqa: PLR2004 -- valor de configuração, não cálculo
    assert tasks.submit_task.retry_delay_seconds == [30, 60, 120]


def test_poll_task_retries_on_transient_failure():
    assert tasks.poll_task.retries == 3  # noqa: PLR2004 -- valor de configuração, não cálculo
    assert tasks.poll_task.retry_delay_seconds == [30, 60, 120]


def test_schedule_tasks_retry_on_transient_failure():
    for task in (tasks.activate_schedule_task, tasks.pause_schedule_if_idle_task):
        assert task.retries == 3  # noqa: PLR2004 -- valor de configuração, não cálculo
        assert task.retry_delay_seconds == [30, 60, 120]


def fake_client(schedules):
    client = MagicMock()
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    client.read_deployment_schedules.return_value = schedules
    return client


def test_submit_task_only_submits():
    summary = SimpleNamespace(session_ids=["s1"])
    with (
        patch.object(tasks, "load_settings"),
        patch.object(tasks, "resolve_origem", return_value="gs://in"),
        patch.object(tasks, "build_client"),
        patch.object(tasks, "submit_pending", return_value=summary),
        patch.object(tasks, "get_client") as get_client,
    ):
        result = tasks.submit_task.fn(origem=None, mes_envio=None, max_paginas=None, versao_processamento=None)
    assert result is summary
    get_client.assert_not_called()


def test_poll_task_only_polls():
    with (
        patch.object(tasks, "load_settings"),
        patch.object(tasks, "build_client"),
        patch.object(tasks, "poll_sessions") as poll_sessions,
        patch.object(tasks, "get_client") as get_client,
    ):
        tasks.poll_task.fn()
    poll_sessions.assert_called_once()
    get_client.assert_not_called()


def test_activate_schedule_noop_outside_a_deployment():
    with (
        patch.object(tasks, "load_settings", return_value=SETTINGS),
        patch.object(tasks.deployment, "id", None),
        patch.object(tasks, "active_sessions", return_value=[SimpleNamespace()]),
        patch.object(tasks, "get_client") as get_client,
    ):
        tasks.activate_schedule_task.fn()
    get_client.assert_not_called()


def test_activate_schedule_reactivates_and_triggers_poll_when_sessions_are_active():
    schedule = SimpleNamespace(id="sched-1", active=False)
    client = fake_client([schedule])
    with (
        patch.object(tasks, "load_settings", return_value=SETTINGS),
        patch.object(tasks.deployment, "id", "dep-1"),
        patch.object(tasks, "active_sessions", return_value=[SimpleNamespace()]),
        patch.object(tasks, "get_client", return_value=client),
    ):
        tasks.activate_schedule_task.fn()
    client.update_deployment_schedule.assert_called_once_with("dep-1", "sched-1", active=True)
    client.create_flow_run_from_deployment.assert_called_once_with("dep-1", parameters={"acao": "acompanhar"})


def test_activate_schedule_leaves_an_already_active_schedule_alone_but_still_triggers_poll():
    schedule = SimpleNamespace(id="sched-1", active=True)
    client = fake_client([schedule])
    with (
        patch.object(tasks, "load_settings", return_value=SETTINGS),
        patch.object(tasks.deployment, "id", "dep-1"),
        patch.object(tasks, "active_sessions", return_value=[SimpleNamespace()]),
        patch.object(tasks, "get_client", return_value=client),
    ):
        tasks.activate_schedule_task.fn()
    client.update_deployment_schedule.assert_not_called()
    client.create_flow_run_from_deployment.assert_called_once()


def test_activate_schedule_covers_a_retried_submission_that_created_no_new_session():
    # Numa repetição, submit_pending vê tudo "em voo" e não cria sessão nova; as já
    # criadas ainda precisam de acompanhamento, então a decisão vem de active_sessions.
    schedule = SimpleNamespace(id="sched-1", active=False)
    client = fake_client([schedule])
    with (
        patch.object(tasks, "load_settings", return_value=SETTINGS),
        patch.object(tasks.deployment, "id", "dep-1"),
        patch.object(tasks, "active_sessions", return_value=[SimpleNamespace(), SimpleNamespace()]),
        patch.object(tasks, "get_client", return_value=client),
    ):
        tasks.activate_schedule_task.fn()
    client.update_deployment_schedule.assert_called_once_with("dep-1", "sched-1", active=True)


def test_activate_schedule_does_nothing_when_no_session_is_active():
    client = fake_client([SimpleNamespace(id="sched-1", active=False)])
    with (
        patch.object(tasks, "load_settings", return_value=SETTINGS),
        patch.object(tasks.deployment, "id", "dep-1"),
        patch.object(tasks, "active_sessions", return_value=[]),
        patch.object(tasks, "get_client", return_value=client),
    ):
        tasks.activate_schedule_task.fn()
    client.update_deployment_schedule.assert_not_called()
    client.create_flow_run_from_deployment.assert_not_called()


def test_pause_schedule_pauses_an_active_schedule_when_nothing_remains_active():
    client = fake_client([SimpleNamespace(id="sched-1", active=True)])
    with (
        patch.object(tasks, "load_settings", return_value=SETTINGS),
        patch.object(tasks.deployment, "id", "dep-1"),
        patch.object(tasks, "active_sessions", return_value=[]),
        patch.object(tasks, "get_client", return_value=client),
    ):
        tasks.pause_schedule_if_idle_task.fn()
    client.update_deployment_schedule.assert_called_once_with("dep-1", "sched-1", active=False)


def test_pause_schedule_leaves_it_alone_when_sessions_are_still_active():
    client = fake_client([SimpleNamespace(id="sched-1", active=True)])
    with (
        patch.object(tasks, "load_settings", return_value=SETTINGS),
        patch.object(tasks.deployment, "id", "dep-1"),
        patch.object(tasks, "active_sessions", return_value=[SimpleNamespace()]),
        patch.object(tasks, "get_client", return_value=client),
    ):
        tasks.pause_schedule_if_idle_task.fn()
    client.update_deployment_schedule.assert_not_called()


def test_pause_schedule_noop_outside_a_deployment():
    with (
        patch.object(tasks, "load_settings", return_value=SETTINGS),
        patch.object(tasks.deployment, "id", None),
        patch.object(tasks, "active_sessions", return_value=[]),
        patch.object(tasks, "get_client") as get_client,
    ):
        tasks.pause_schedule_if_idle_task.fn()
    get_client.assert_not_called()
