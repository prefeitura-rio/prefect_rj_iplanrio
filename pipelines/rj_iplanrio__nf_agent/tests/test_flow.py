"""Tests for the flow's action routing."""

from unittest.mock import patch

import pytest

from pipelines.rj_iplanrio__nf_agent import flow as flow_mod


def run_flow(**params):
    return flow_mod.rj_iplanrio__nf_agent.fn(**params)


def test_default_action_polls_then_pauses_the_schedule_if_idle():
    with (
        patch.object(flow_mod, "inject_credentials_task"),
        patch.object(flow_mod, "poll_task") as poll_task,
        patch.object(flow_mod, "submit_task") as submit_task,
        patch.object(flow_mod, "activate_schedule_task") as activate,
        patch.object(flow_mod, "pause_schedule_if_idle_task") as pause,
    ):
        run_flow()
    poll_task.assert_called_once()
    pause.assert_called_once()
    submit_task.assert_not_called()
    activate.assert_not_called()


def test_submit_action_forwards_parameters_and_then_activates_the_schedule():
    with (
        patch.object(flow_mod, "inject_credentials_task"),
        patch.object(flow_mod, "poll_task") as poll_task,
        patch.object(flow_mod, "submit_task") as submit_task,
        patch.object(flow_mod, "activate_schedule_task") as activate,
        patch.object(flow_mod, "pause_schedule_if_idle_task") as pause,
    ):
        run_flow(acao="submeter", origem="gs://b/p", mes_envio="2021-11-01", max_paginas=10, versao_processamento="x")
    poll_task.assert_not_called()
    pause.assert_not_called()
    activate.assert_called_once()
    kwargs = submit_task.call_args.kwargs
    assert (kwargs["origem"], kwargs["mes_envio"], kwargs["max_paginas"], kwargs["versao_processamento"]) == (
        "gs://b/p",
        "2021-11-01",
        10,
        "x",
    )


def test_submit_action_still_activates_the_schedule_when_the_submission_fails():
    with (
        patch.object(flow_mod, "inject_credentials_task"),
        patch.object(flow_mod, "submit_task", side_effect=RuntimeError("no healthy upstream")),
        patch.object(flow_mod, "activate_schedule_task") as activate,
        pytest.raises(RuntimeError, match="no healthy upstream"),
    ):
        run_flow(acao="submeter")
    activate.assert_called_once()
