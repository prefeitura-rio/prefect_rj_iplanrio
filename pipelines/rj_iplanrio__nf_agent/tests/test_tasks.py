"""Tests for the self-managing schedule (pause when idle, resume on submit)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pipelines.rj_iplanrio__nf_agent import tasks


def fake_client(schedules):
    client = MagicMock()
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    client.read_deployment_schedules.return_value = schedules
    return client


def test_set_own_schedule_active_noop_outside_a_deployment():
    with (
        patch.object(tasks.deployment, "id", None),
        patch.object(tasks, "get_client") as get_client,
    ):
        tasks.set_own_schedule_active(True)
    get_client.assert_not_called()


def test_set_own_schedule_active_pauses_an_active_schedule():
    schedule = SimpleNamespace(id="sched-1", active=True)
    client = fake_client([schedule])
    with (
        patch.object(tasks.deployment, "id", "dep-1"),
        patch.object(tasks, "get_client", return_value=client),
    ):
        tasks.set_own_schedule_active(False)
    client.update_deployment_schedule.assert_called_once_with("dep-1", "sched-1", active=False)


def test_set_own_schedule_active_skips_when_already_in_the_right_state():
    schedule = SimpleNamespace(id="sched-1", active=True)
    client = fake_client([schedule])
    with (
        patch.object(tasks.deployment, "id", "dep-1"),
        patch.object(tasks, "get_client", return_value=client),
    ):
        tasks.set_own_schedule_active(True)
    client.update_deployment_schedule.assert_not_called()


def test_trigger_immediate_poll_noop_outside_a_deployment():
    with (
        patch.object(tasks.deployment, "id", None),
        patch.object(tasks, "get_client") as get_client,
    ):
        tasks.trigger_immediate_poll()
    get_client.assert_not_called()


def test_trigger_immediate_poll_creates_an_acompanhar_run():
    client = fake_client([])
    with (
        patch.object(tasks.deployment, "id", "dep-1"),
        patch.object(tasks, "get_client", return_value=client),
    ):
        tasks.trigger_immediate_poll()
    client.create_flow_run_from_deployment.assert_called_once_with("dep-1", parameters={"acao": "acompanhar"})


def test_submit_task_reactivates_and_triggers_poll_when_sessions_created():
    summary = SimpleNamespace(session_ids=["s1"])
    with (
        patch.object(tasks, "load_settings"),
        patch.object(tasks, "resolve_origem", return_value="gs://in"),
        patch.object(tasks, "build_client"),
        patch.object(tasks, "submit_pending", return_value=summary),
        patch.object(tasks, "set_own_schedule_active") as activate,
        patch.object(tasks, "trigger_immediate_poll") as trigger,
    ):
        result = tasks.submit_task.fn(origem=None, mes_envio=None, max_paginas=None, versao_processamento=None)
    assert result is summary
    activate.assert_called_once_with(True)
    trigger.assert_called_once()


def test_submit_task_does_nothing_extra_when_no_sessions_created():
    summary = SimpleNamespace(session_ids=[])
    with (
        patch.object(tasks, "load_settings"),
        patch.object(tasks, "resolve_origem", return_value="gs://in"),
        patch.object(tasks, "build_client"),
        patch.object(tasks, "submit_pending", return_value=summary),
        patch.object(tasks, "set_own_schedule_active") as activate,
        patch.object(tasks, "trigger_immediate_poll") as trigger,
    ):
        tasks.submit_task.fn(origem=None, mes_envio=None, max_paginas=None, versao_processamento=None)
    activate.assert_not_called()
    trigger.assert_not_called()


def test_poll_task_pauses_schedule_when_nothing_remains_active():
    with (
        patch.object(tasks, "load_settings") as load_settings,
        patch.object(tasks, "build_client"),
        patch.object(tasks, "poll_sessions") as poll_sessions,
        patch.object(tasks, "active_sessions", return_value=[]),
        patch.object(tasks, "set_own_schedule_active") as activate,
    ):
        load_settings.return_value = SimpleNamespace(nf_batch_jobs_table="p.d.t")
        tasks.poll_task.fn()
    activate.assert_called_once_with(False)
    poll_sessions.assert_called_once()


def test_poll_task_leaves_schedule_alone_when_sessions_still_active():
    with (
        patch.object(tasks, "load_settings") as load_settings,
        patch.object(tasks, "build_client"),
        patch.object(tasks, "poll_sessions"),
        patch.object(tasks, "active_sessions", return_value=[SimpleNamespace()]),
        patch.object(tasks, "set_own_schedule_active") as activate,
    ):
        load_settings.return_value = SimpleNamespace(nf_batch_jobs_table="p.d.t")
        tasks.poll_task.fn()
    activate.assert_not_called()
