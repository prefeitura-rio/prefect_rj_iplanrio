"""Flow principal de orquestração da pipeline de triagem de recursos SMTR-RJ.

Descrição Técnica & Arquitetura:
Orquestra o grafo de execução (DAG) do Prefect 3.0 para a esteira analítica de
recursos de multas da SMTR-RJ. Gerencia a concorrência dos workers em thread pool,
a resolução de ambiente (banco local vs. Cloud SQL) e o ciclo de vida completo
dos processos administrativos: desde a busca de lotes (na fila do banco ou via
API REST do SEI-Rio), passando pela calibração estatística e predição com IA,
até a persistência dos resultados e consolidação do sumário executivo.

Fluxo de Dependências:
- Consumidores Diretos:
  * `scripts/run_local.py`: invoca este flow para depuração, testes e execução local.
  * `prefect.yaml` / Deployments: registra o ponto de entrada da pipeline em produção.
- Dependências Diretas:
  * `tasks.py`: delega a execução das tarefas atômicas e observáveis da pipeline.
  * `constants.py`: importa parâmetros centrais de operação, limiares de decisão e timeouts.
  * `utils/sei_client.py`: instancia o cliente HTTP quando a fonte de entrada é o SEI-Rio.
"""

from typing import Any

from prefect import flow, get_run_logger
from prefect.futures import as_completed
from prefect.task_runners import ThreadPoolTaskRunner
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

from pipelines.rj_smtr__triagem_recursos.constants import (
    LIMIAR_ALTA,
    LIMIAR_BAIXA,
    LIMIAR_DISPERSAO_COT_MAX,
    MAX_DOWNLOADS_SEI_SIMULTANEOS,
    MAX_PAGINAS_PDF,
    MAX_WORKERS_PIPELINE,
    VERSAO_PROMPT_PADRAO,
    VERTEX_LLM_MODEL,
)
from pipelines.rj_smtr__triagem_recursos.helpers.sei_client import ModWsseiClient
from pipelines.rj_smtr__triagem_recursos.helpers.triage import executar_lote_em_janelas
from pipelines.rj_smtr__triagem_recursos.tasks import (
    task_baixar_e_preparar_pdf,
    task_buscar_processos_pendentes,
    task_carregar_artefatos_referencia,
    task_gerar_sumario_lote,
    task_persistir_lote_resultados,
    task_triar_pdf_preparado,
)


@flow(
    task_runner=ThreadPoolTaskRunner(max_workers=MAX_WORKERS_PIPELINE),
    log_prints=True,
    description="Pipeline de triagem e classificação seletiva de recursos da SMTR-RJ.",
)
def rj_smtr__triagem_recursos(
    is_local: bool = False,
    fonte_pdf: str = "sei",
    cliente_sei: Any | None = None,
    modo_execucao: str = "INCREMENTAL",
    limite_lote: int = 300,
    limiar_alta: float = LIMIAR_ALTA,
    limiar_baixa: float = LIMIAR_BAIXA,
    limiar_dispersao: float = LIMIAR_DISPERSAO_COT_MAX,
    max_paginas: int = MAX_PAGINAS_PDF,
    modelo_llm: str = VERTEX_LLM_MODEL,
    arquivo_prompt: str = f"{VERSAO_PROMPT_PADRAO}.txt",
    data_inicio: str | None = None,
    data_fim: str | None = None,
    infisical_secret_path: str | None = "/rj_smtr__triagem_recursos",
    reasoning_effort: str = "medium",
    arquivos_especificos: list[Any] | None = None,
    executar_n2: bool = True,
    max_downloads_sei: int = MAX_DOWNLOADS_SEI_SIMULTANEOS,
) -> dict[str, Any]:
    """Orchestrate automated triage and selective classification of SMTR appeals."""
    logger = get_run_logger()
    logger.debug("Telemetria ativa para rj_smtr__triagem_recursos (local=%s).", is_local)
    logger.info(
        "Iniciando pipeline | Local=%s | Fonte=%s | Modo=%s | Limite=%d | Workers=%d | DownloadsSEI=%d",
        is_local,
        fonte_pdf,
        modo_execucao,
        limite_lote,
        MAX_WORKERS_PIPELINE,
        max_downloads_sei,
    )

    artefatos = task_carregar_artefatos_referencia(
        is_local=is_local,
        arquivo_prompt=arquivo_prompt,
        infisical_secret_path=infisical_secret_path,
    )

    cliente_proprio = False
    if fonte_pdf == "sei":
        if cliente_sei is None:
            cliente_sei = ModWsseiClient(is_local=is_local)
            cliente_proprio = True
        if not getattr(cliente_sei, "token", None):
            cliente_sei.autenticar()

    try:
        processos = task_buscar_processos_pendentes(
            is_local=is_local,
            fonte_pdf=fonte_pdf,
            cliente_sei=cliente_sei,
            modo=modo_execucao,
            limite=limite_lote,
            data_inicio=data_inicio,
            data_fim=data_fim,
            arquivos_especificos=arquivos_especificos,
        )

        if not processos:
            logger.info("Nenhum processo pendente para triagem. Encerrando execução.")
            return {"status": "SEM_DADOS", "total": 0}

        resultados = executar_lote_em_janelas(
            processos=processos,
            submeter_download=lambda proc: task_baixar_e_preparar_pdf.submit(
                processo=proc,
                artefatos=artefatos,
                fonte_pdf=fonte_pdf,
                cliente_sei=cliente_sei,
                max_paginas=max_paginas,
                modelo_llm=modelo_llm,
            ),
            submeter_classificacao=lambda doc: task_triar_pdf_preparado.submit(
                documento_preparado=doc,
                artefatos=artefatos,
                limiar_alta=limiar_alta,
                limiar_baixa=limiar_baixa,
                limiar_dispersao_max=limiar_dispersao,
                modelo_llm=modelo_llm,
                reasoning_effort=reasoning_effort,
                executar_n2=executar_n2,
            ),
            aguardar_concluidos=as_completed,
            max_downloads_simultaneos=max_downloads_sei,
            aquecer_primeiro=True,
        )

        if resultados:
            task_persistir_lote_resultados(resultados, is_local=is_local)

        return task_gerar_sumario_lote(resultados)
    finally:
        if cliente_proprio and cliente_sei is not None:
            close_fn = getattr(cliente_sei, "close", None)
            if callable(close_fn):
                close_fn()
