"""Tarefas atômicas (Tasks) da pipeline de triagem SMTR no Prefect 3.0.

Descrição Técnica & Arquitetura:
Define as unidades atômicas de trabalho, retentativas automáticas e observabilidade
da pipeline. Cada task funciona como uma camada de orquestração fina que recebe os
parâmetros do flow e delega as operações de I/O de banco, inferência multimodal,
cálculos matemáticos e compilação estatística para os submódulos especializados
em `utils/`.

Fluxo de Dependências:
- Consumidores Diretos:
  * `flow.py`: consome todas as tasks para compor o fluxo serial e o mapeamento concorrente (.map).
- Dependências Diretas:
  * `constants.py`: importa limiares de decisão, modelos e resolvedor de URL do banco.
  * `utils/artifacts.py`: invocado por `task_carregar_artefatos_referencia` para obter
    o calibrador LightGBM e os quantis conformais CCCP ativos.
  * `utils/db_repository.py`: invocado por `task_buscar_processos_pendentes` e
    `task_persistir_lote_resultados` para consultar filas e salvar predições com transação atômica.
  * `utils/triage.py`: invocado por `task_triar_processo_individual` para processar
    cada recurso administrativo e por `task_gerar_sumario_lote` para consolidar métricas e custos.
"""

from typing import Any

from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE

from pipelines.rj_smtr__triagem_recursos.constants import (
    LIMIAR_ALTA,
    LIMIAR_BAIXA,
    LIMIAR_DISPERSAO_COT_MAX,
    MAX_PAGINAS_PDF,
    VERSAO_PROMPT_PADRAO,
    VERTEX_LLM_MODEL,
    resolver_banco_url,
)
from pipelines.rj_smtr__triagem_recursos.helpers.artifacts import carregar_artefatos_pipeline
from pipelines.rj_smtr__triagem_recursos.helpers.db_repository import (
    buscar_processos_para_triagem_ou_fallback,
    persistir_lote_resultados_ia,
)
from pipelines.rj_smtr__triagem_recursos.helpers.triage import (
    classificar_documento_preparado,
    gerar_sumario_lote,
    preparar_documento_processo,
    triar_processo,
)


@task(name="carregar_artefatos_referencia", cache_policy=NO_CACHE, retries=2, retry_delay_seconds=5)
def task_carregar_artefatos_referencia(
    is_local: bool = False,
    arquivo_prompt: str = f"{VERSAO_PROMPT_PADRAO}.txt",
    infisical_secret_path: str | None = None,
) -> dict[str, Any]:
    """Carrega parâmetros de calibração do PostgreSQL (com fallback local apenas se is_local=True)."""
    return carregar_artefatos_pipeline(
        is_local=is_local,
        arquivo_prompt=arquivo_prompt,
        infisical_secret_path=infisical_secret_path,
    )


@task(name="buscar_processos_pendentes", cache_policy=NO_CACHE, retries=2, retry_delay_seconds=5)
def task_buscar_processos_pendentes(
    is_local: bool = False,
    fonte_pdf: str = "sei",
    cliente_sei: Any | None = None,
    modo: str = "INCREMENTAL",
    limite: int = 300,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    arquivos_especificos: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Consulta ou filtra os processos elegiveis para triagem"""
    db_url = resolver_banco_url(is_local)
    return buscar_processos_para_triagem_ou_fallback(
        db_url=db_url,
        is_local=is_local,
        fonte_pdf=fonte_pdf,
        cliente_sei=cliente_sei,
        modo=modo,
        limite=limite,
        data_inicio=data_inicio,
        data_fim=data_fim,
        arquivos_especificos=arquivos_especificos,
    )


@task(
    name="baixar_e_preparar_pdf",
    cache_policy=NO_CACHE,
    retries=1,
    retry_delay_seconds=3,
    timeout_seconds=180,
)
def task_baixar_e_preparar_pdf(
    processo: dict[str, Any],
    artefatos: dict[str, Any],
    fonte_pdf: str = "sei",
    cliente_sei: Any | None = None,
    max_paginas: int = MAX_PAGINAS_PDF,
    modelo_llm: str = VERTEX_LLM_MODEL,
) -> dict[str, Any]:
    """Executa estágio 1 de download em RAM e recorte Smart Window do PDF."""
    return preparar_documento_processo(
        processo=processo,
        artefatos=artefatos,
        fonte_pdf=fonte_pdf,
        cliente_sei=cliente_sei,
        max_paginas=max_paginas,
        modelo_llm=modelo_llm,
    )


@task(
    name="triar_pdf_preparado",
    cache_policy=NO_CACHE,
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=240,
)
def task_triar_pdf_preparado(
    documento_preparado: dict[str, Any],
    artefatos: dict[str, Any],
    limiar_alta: float = LIMIAR_ALTA,
    limiar_baixa: float = LIMIAR_BAIXA,
    limiar_dispersao_max: float = LIMIAR_DISPERSAO_COT_MAX,
    modelo_llm: str = VERTEX_LLM_MODEL,
    reasoning_effort: str = "medium",
    executar_n2: bool = True,
) -> dict[str, Any]:
    """Executa estágio 2 de inferência LLM, OCR e calibração em documento preparado."""
    return classificar_documento_preparado(
        documento_preparado=documento_preparado,
        artefatos=artefatos,
        limiar_alta=limiar_alta,
        limiar_baixa=limiar_baixa,
        limiar_dispersao_max=limiar_dispersao_max,
        modelo_llm=modelo_llm,
        reasoning_effort=reasoning_effort,
        executar_n2=executar_n2,
    )


@task(
    name="triar_processo_individual",
    cache_policy=NO_CACHE,
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=240,
)
def task_triar_processo_individual(
    processo: dict[str, Any],
    artefatos: dict[str, Any],
    is_local: bool = False,
    fonte_pdf: str = "sei",
    cliente_sei: Any | None = None,
    limiar_alta: float = LIMIAR_ALTA,
    limiar_baixa: float = LIMIAR_BAIXA,
    limiar_dispersao_max: float = LIMIAR_DISPERSAO_COT_MAX,
    max_paginas: int = MAX_PAGINAS_PDF,
    modelo_llm: str = VERTEX_LLM_MODEL,
    reasoning_effort: str = "medium",
    executar_n2: bool = True,
) -> dict[str, Any]:
    """Executa a triagem individual com inferencia deterministica e calibracao tabular."""
    return triar_processo(
        processo=processo,
        artefatos=artefatos,
        is_local=is_local,
        fonte_pdf=fonte_pdf,
        cliente_sei=cliente_sei,
        limiar_alta=limiar_alta,
        limiar_baixa=limiar_baixa,
        limiar_dispersao_max=limiar_dispersao_max,
        max_paginas=max_paginas,
        modelo_llm=modelo_llm,
        reasoning_effort=reasoning_effort,
        executar_n2=executar_n2,
    )


@task(name="persistir_lote_resultados", cache_policy=NO_CACHE, retries=2, retry_delay_seconds=5)
def task_persistir_lote_resultados(resultados: list[dict[str, Any]], is_local: bool = False) -> None:
    """Persiste as predicoes e atualiza a fila de trabalho no banco de dados."""
    try:
        logger = get_run_logger()
    except Exception:
        from prefect.logging import get_logger

        logger = get_logger("toolkit")
    db_url = resolver_banco_url(is_local)

    logger.info("Gravando %d resultado(s) no banco de dados...", len(resultados))
    try:
        persistir_lote_resultados_ia(db_url, resultados)
        logger.info("Resultados persistidos com sucesso.")
    except Exception as exc:
        logger.error("Falha ao persistir no banco de dados: %s", exc)
        raise


@task(name="gerar_sumario_lote", cache_policy=NO_CACHE)
def task_gerar_sumario_lote(resultados: list[dict[str, Any]]) -> dict[str, Any]:
    """Consolida as metricas operacionais e financeiras da execucao do lote."""
    return gerar_sumario_lote(resultados)
