"""Classifica páginas de PDFs e extrai dados de notas fiscais em batch via Bifrost, para a CGM.

``acao="submeter"`` (manual): submete todos os PDFs pendentes de ``origem`` — ou,
se ``origem`` não for informada, da base padrão (``GCS_BUCKET``/``PDFS_BASE_PATH``
do Infisical), opcionalmente restrita a ``mes_envio=<data>/`` via ``mes_envio``.
``acao="acompanhar"`` (padrão, agendado em prod): avança as sessões em andamento.
"""

from typing import Literal

from prefect import flow

from .tasks import (
    activate_schedule_task,
    inject_credentials_task,
    pause_schedule_if_idle_task,
    poll_task,
    submit_task,
)


@flow(log_prints=True)
def rj_iplanrio__nf_agent(
    acao: Literal["submeter", "acompanhar"] = "acompanhar",
    origem: str | None = None,
    mes_envio: str | None = None,
    max_paginas: int | None = None,
    versao_processamento: str | None = None,
) -> None:
    """Roteia ``acao``: submete PDFs pendentes ou avança as sessões em andamento.

    Depois de submeter, o agendamento é reativado em ``finally`` — mesmo se a submissão
    falhar no meio, as sessões já criadas precisam de acompanhamento. Depois de acompanhar,
    o agendamento é pausado se não sobrou sessão ativa.
    """
    credentials = inject_credentials_task()
    if acao == "submeter":
        try:
            submit_task(
                origem=origem,
                mes_envio=mes_envio,
                max_paginas=max_paginas,
                versao_processamento=versao_processamento,
                wait_for=[credentials],
            )
        finally:
            activate_schedule_task()
    else:
        poll_task(wait_for=[credentials])
        pause_schedule_if_idle_task()
