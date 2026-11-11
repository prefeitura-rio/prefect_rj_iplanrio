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

from prefect import flow, get_run_logger, unmapped
from prefect.task_runners import ThreadPoolTaskRunner
from toolkit.telemetry import logger as telemetry_logger

from .constants import (
    LIMIAR_ALTA,
    LIMIAR_BAIXA,
    LIMIAR_DISPERSAO_COT_MAX,
    MAX_PAGINAS_PDF,
    VERSAO_PROMPT_PADRAO,
    VERTEX_LLM_MODEL,
)
from .tasks import (
    task_buscar_processos_pendentes,
    task_carregar_artefatos_referencia,
    task_gerar_sumario_lote,
    task_persistir_lote_resultados,
    task_triar_processo_individual,
)
from .utils.sei_client import ModWsseiClient


@flow(
    name="rj_smtr__triagem_recursos",
    task_runner=ThreadPoolTaskRunner(max_workers=4),
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
) -> dict[str, Any]:
    """Orchestrate automated triage and selective classification of SMTR appeals."""
    logger = get_run_logger()
    telemetry_logger.debug("Telemetria ativa para rj_smtr__triagem_recursos (local=%s).", is_local)
    logger.info(
        "Iniciando pipeline | Local=%s | Fonte=%s | Modo=%s | Limite=%d",
        is_local,
        fonte_pdf,
        modo_execucao,
        limite_lote,
    )

    # 1. Carrega artefatos da baseline (PostgreSQL em Cloud SQL; fallback local apenas se is_local=True)
    artefatos = task_carregar_artefatos_referencia(
        is_local=is_local,
        arquivo_prompt=arquivo_prompt,
        infisical_secret_path=infisical_secret_path,
    )

    # Inicializa e autentica cliente SEI se a fonte for 'sei'
    if fonte_pdf == "sei":
        if cliente_sei is None:
            cliente_sei = ModWsseiClient(is_local=is_local)
        if not getattr(cliente_sei, "token", None):
            cliente_sei.autenticar()

    # 2. Busca processos na fila ou na caixa da JARI no SEI
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

    # 3. Mapeamento concorrente sobre cada processo
    resultados_futuros = task_triar_processo_individual.map(
        processo=processos,
        artefatos=unmapped(artefatos),
        is_local=unmapped(is_local),
        fonte_pdf=unmapped(fonte_pdf),
        cliente_sei=unmapped(cliente_sei),
        limiar_alta=unmapped(limiar_alta),
        limiar_baixa=unmapped(limiar_baixa),
        limiar_dispersao_max=unmapped(limiar_dispersao),
        max_paginas=unmapped(max_paginas),
        modelo_llm=unmapped(modelo_llm),
        reasoning_effort=unmapped(reasoning_effort),
        executar_n2=unmapped(executar_n2),
    )

    # Resolve os futuros concorrentes dos workers protegendo contra interrupções globais
    resultados = []
    for f in resultados_futuros:
        try:
            r = f.result(raise_on_failure=False)
            if isinstance(r, dict) and "processo_id" in r:
                resultados.append(r)
        except Exception as exc:
            logger.error("Falha irrecuperável em worker individual: %s", exc)

    # 4. Persiste o lote de predições
    if resultados:
        task_persistir_lote_resultados(resultados, is_local=is_local)

    # 5. Consolida sumário operacional
    return task_gerar_sumario_lote(resultados)
