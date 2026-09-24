"""Classifica páginas de PDFs e extrai dados de notas fiscais em batch via Bifrost, para a CGM.

``acao="submeter"`` (manual): submete todos os PDFs pendentes de ``origem``.
``acao="acompanhar"`` (padrão, agendado em prod): avança as sessões em andamento.
"""

from prefect import flow

from .tasks import inject_credentials_task, poll_task, submit_task


@flow(log_prints=True)
def rj_iplanrio__nf_agent(
    acao: str = "acompanhar",
    origem: str | None = None,
    max_paginas: int | None = None,
    versao_processamento: str | None = None,
) -> None:
    credentials = inject_credentials_task()
    if acao == "submeter":
        submit_task(
            origem=origem,
            max_paginas=max_paginas,
            versao_processamento=versao_processamento,
            wait_for=[credentials],
        )
    elif acao == "acompanhar":
        poll_task(wait_for=[credentials])
    else:
        raise ValueError(f"acao inválida: {acao!r}. Use 'submeter' ou 'acompanhar'.")
