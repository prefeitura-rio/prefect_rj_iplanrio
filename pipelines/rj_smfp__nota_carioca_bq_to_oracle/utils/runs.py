"""Chamadas ao Prefect para criar, consultar e cancelar o run filho de dbt."""

from dataclasses import dataclass
from typing import cast
from uuid import UUID

from prefect import get_client
from prefect.client.schemas.objects import FlowRun
from prefect.deployments import run_deployment
from prefect.settings import PREFECT_UI_URL
from prefect.states import Cancelling

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.dbt import RunStatus


@dataclass(frozen=True)
class StartedRun:
    """Flow run criado para o deployment de dbt.

    :param id: Id do flow run.
    :param name: Nome do flow run.
    :param url: Link do run na UI do Prefect, se ``PREFECT_UI_URL`` estiver definida.
    """

    id: str
    name: str
    url: str | None


def start_deployment_run(deployment: str, parameters: dict[str, object]) -> StartedRun:
    """Cria o run do deployment como subflow do run atual, sem esperar o término.

    :param deployment: Deployment no formato ``<flow>/<deployment>``.
    :param parameters: Parâmetros do flow de dbt.
    :returns: Run criado.
    """
    # run_deployment é sync_compatible: devolve o FlowRun direto quando chamado de código síncrono.
    flow_run = cast("FlowRun", run_deployment(name=deployment, parameters=parameters, timeout=0, as_subflow=True))
    ui_url = PREFECT_UI_URL.value()
    url = f"{ui_url.rstrip('/')}/runs/flow-run/{flow_run.id}" if ui_url else None
    return StartedRun(id=str(flow_run.id), name=flow_run.name, url=url)


class PrefectRuns:
    """Lê o estado e cancela flow runs pela API do Prefect."""

    def read_status(self, run_id: str) -> RunStatus:
        """Lê o estado atual do flow run.

        :param run_id: Id do flow run.
        :returns: Estado e fim do run.
        :raises ValueError: Se o run não tiver estado.
        """
        with get_client(sync_client=True) as client:
            flow_run = client.read_flow_run(UUID(run_id))
        state = flow_run.state
        if state is None:
            raise ValueError(f"O flow run {run_id} não tem estado.")
        return RunStatus(
            state_type=state.type.value,
            state_name=state.name or state.type.value,
            message=state.message,
            end_time=flow_run.end_time,
        )

    def cancel(self, run_id: str) -> None:
        """Move o flow run para ``Cancelling``, o que o worker transforma em cancelamento.

        :param run_id: Id do flow run.
        """
        with get_client(sync_client=True) as client:
            client.set_flow_run_state(UUID(run_id), Cancelling(), force=True)
