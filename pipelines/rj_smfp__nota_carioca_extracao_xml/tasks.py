"""Task da extração XML: monta as ações reais (webservice, Oracle, Discord, relógio) e roda :func:`run_extraction`."""

import time

import httpx
from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.config import read_oracle_config, read_token
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.discord import Environment
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.notify import make_notifier
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.run import ExtractionServices, run_extraction
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.soap import RoutineResponse, call_extraction
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.tasks_db import OracleTaskStore
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.wait import Clock, Limits


@task(cache_policy=NO_CACHE)
def run_extraction_task(  # noqa: PLR0913, PLR0917
    url: str,
    mode: int,
    infisical_secret_path: str,
    limits: Limits,
    discord_notifications: bool,
    environment: Environment,
) -> str:
    """Lê token e Oracle do ambiente (falha antes de qualquer chamada se faltarem) e roda a extração.

    :param url: Endereço do ``RotinasAutomaticas.asmx`` do ambiente.
    :param mode: Modo de execução do webservice.
    :param infisical_secret_path: Pasta do Infisical com o Oracle.
    :param limits: Ritmo e limites do acompanhamento.
    :param discord_notifications: ``False`` desliga o Discord.
    :param environment: Ambiente do deployment (webhook e marcador de staging).
    :returns: Resumo do que aconteceu.
    """
    token = read_token()
    store = OracleTaskStore(read_oracle_config(infisical_secret_path))
    notify = make_notifier(discord_notifications, environment)
    log(f"Extração XML: webservice {url}, modo {mode}; acompanhamento em NFSE_OWNER.TAREFAS.")
    with httpx.Client(follow_redirects=False) as client:

        def call() -> RoutineResponse:
            return call_extraction(client, url, token, mode)

        services = ExtractionServices(
            call=call,
            store=store,
            notify=notify,
            log=log,
            clock=Clock(now=time.monotonic, sleep=time.sleep),
        )
        return run_extraction(services, limits)
