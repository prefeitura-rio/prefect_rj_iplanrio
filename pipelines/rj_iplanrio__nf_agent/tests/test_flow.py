"""Tests for the flow's action routing."""

from unittest.mock import patch

import pytest

from pipelines.rj_iplanrio__nf_agent import flow as flow_mod


def run_flow(**params):
    return flow_mod.rj_iplanrio__nf_agent.fn(**params)


def test_default_action_only_polls():
    with (
        patch.object(flow_mod, "inject_credentials_task"),
        patch.object(flow_mod, "poll_task") as poll_task,
        patch.object(flow_mod, "submit_task") as submit_task,
    ):
        run_flow()
    poll_task.assert_called_once()
    submit_task.assert_not_called()


def test_submit_action_forwards_parameters():
    with (
        patch.object(flow_mod, "inject_credentials_task"),
        patch.object(flow_mod, "poll_task") as poll_task,
        patch.object(flow_mod, "submit_task") as submit_task,
    ):
        run_flow(acao="submeter", origem="gs://b/p", mes_envio="2021-11-01", max_paginas=10, versao_processamento="x")
    poll_task.assert_not_called()
    kwargs = submit_task.call_args.kwargs
    assert (kwargs["origem"], kwargs["mes_envio"], kwargs["max_paginas"], kwargs["versao_processamento"]) == (
        "gs://b/p",
        "2021-11-01",
        10,
        "x",
    )


def test_unknown_action_fails():
    with patch.object(flow_mod, "inject_credentials_task"), pytest.raises(ValueError, match="acao"):
        run_flow(acao="rodar_tudo")
