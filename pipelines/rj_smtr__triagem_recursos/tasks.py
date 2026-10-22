"""Tarefas atômicas (Tasks) da pipeline SMTR no Prefect 3.0.

Cada task é um wrapper fino que delega a lógica de negócio e cálculos
para os submódulos especializados em ``utils/``, conforme o Styleguide da Seção 4.2.
"""

from typing import Any
from prefect import get_run_logger, task

from .constants import (
    LIMIAR_ALTA,
    LIMIAR_BAIXA,
    LIMIAR_DISPERSAO_COT_MAX,
    MAX_PAGINAS_PDF,
    VERSAO_PROMPT_PADRAO,
    VERTEX_LLM_MODEL,
    resolver_banco_url,
)
from .utils.artifacts import carregar_artefatos_pipeline
from .utils.db_repository import (
    buscar_processos_para_triagem_ou_fallback,
    persistir_lote_resultados_ia,
)
from .utils.triage import gerar_sumario_lote, triar_processo


@task(name="carregar_artefatos_referencia", retries=2, retry_delay_seconds=5)
def task_carregar_artefatos_referencia(
    is_local: bool = True,
    arquivo_prompt: str = f"{VERSAO_PROMPT_PADRAO}.txt",
) -> dict[str, Any]:
    """Carrega parâmetros de calibração do PostgreSQL com fallback para artefatos congelados.

    :param is_local: Booleano que define se a conexão é local ou Cloud SQL.
    :param arquivo_prompt: Nome do arquivo de prompt a carregar de prompts/.
    :returns: Dicionário contendo o calibrador, artefatos conformais, prompt e metadados.
    """
    return carregar_artefatos_pipeline(
        is_local=is_local, arquivo_prompt=arquivo_prompt
    )


@task(name="buscar_processos_pendentes", retries=2, retry_delay_seconds=5)
def task_buscar_processos_pendentes(
    is_local: bool = True,
    fonte_pdf: str = "local",
    cliente_sei: Any | None = None,
    modo: str = "INCREMENTAL",
    limite: int = 300,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    arquivos_especificos: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Consulta ou filtra os processos elegíveis para triagem.

    :param is_local: Flag para resolução de banco local vs nuvem.
    :param fonte_pdf: "local" para PDFs em disco ou "sei" para integração direta.
    :param cliente_sei: Cliente do SEI fornecendo listar_processos_caixa().
    :param modo: Modo de processamento ("INCREMENTAL" ou "PASSIVO").
    :param limite: Quantidade máxima de processos a recuperar.
    :param data_inicio: Filtro temporal inicial opcional.
    :param data_fim: Filtro temporal final opcional.
    :param arquivos_especificos: Lista opcional pré-selecionada de arquivos.
    :returns: Lista de processos prontos para a esteira analítica.
    """
    db_url = resolver_banco_url(is_local)
    return buscar_processos_para_triagem_ou_fallback(
        db_url=db_url,
        fonte_pdf=fonte_pdf,
        cliente_sei=cliente_sei,
        modo=modo,
        limite=limite,
        data_inicio=data_inicio,
        data_fim=data_fim,
        arquivos_especificos=arquivos_especificos,
    )


@task(
    name="triar_processo_individual",
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=240,
)
def task_triar_processo_individual(
    processo: dict[str, Any],
    artefatos: dict[str, Any],
    is_local: bool = True,
    fonte_pdf: str = "local",
    cliente_sei: Any | None = None,
    limiar_alta: float = LIMIAR_ALTA,
    limiar_baixa: float = LIMIAR_BAIXA,
    limiar_dispersao_max: float = LIMIAR_DISPERSAO_COT_MAX,
    max_paginas: int = MAX_PAGINAS_PDF,
    modelo_llm: str = VERTEX_LLM_MODEL,
    reasoning_effort: str = "medium",
    executar_n2: bool = False,
) -> dict[str, Any]:
    """Executa a triagem individual com inferência determinística e calibração tabular.

    :param processo: Registro do processo com metadados e identificadores SEI / arquivo.
    :param artefatos: Estrutura com modelo calibrador e quantis conformais.
    :param is_local: Flag para resolução de infraestrutura.
    :param fonte_pdf: "local" para PDFs em disco ou "sei" para download em memória.
    :param cliente_sei: Cliente do SEI com baixar_documento().
    :param limiar_alta: Ponto de corte para aprovação na Faixa Verde.
    :param limiar_baixa: Ponto de corte inferior para envio direto ao HITL.
    :param limiar_dispersao_max: Limite máximo tolerável de dispersão textual de CoT.
    :param max_paginas: Quantidade máxima de páginas a enviar para o LLM.
    :param modelo_llm: Identificador do modelo na Vertex AI / Gateway.
    :param reasoning_effort: Nível de esforço de raciocínio da LLM.
    :param executar_n2: Habilita segundo estágio na faixa de incerteza.
    :returns: Dicionário detalhado contendo a predição e o vetor epistêmico.
    """
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


@task(name="persistir_lote_resultados", retries=2, retry_delay_seconds=5)
def task_persistir_lote_resultados(
    resultados: list[dict[str, Any]], is_local: bool = True
) -> None:
    """Persiste as predições e auditorias na base e atualiza a fila de trabalho.

    :param resultados: Lista de predições consolidadas pelo pipeline.
    :param is_local: Flag de ambiente local versus nuvem.
    """
    logger = get_run_logger()
    db_url = resolver_banco_url(is_local)

    logger.info("Gravando %d resultado(s) no banco de dados...", len(resultados))
    try:
        persistir_lote_resultados_ia(db_url, resultados)
        logger.info("Resultados persistidos com sucesso.")
    except Exception as exc:
        logger.error("Falha ao persistir no banco de dados: %s", exc)
        if not is_local:
            raise


@task(name="gerar_sumario_lote")
def task_gerar_sumario_lote(resultados: list[dict[str, Any]]) -> dict[str, Any]:
    """Consolida as métricas operacionais e financeiras da execução do lote.

    :param resultados: Lista de dicionários de predição do lote.
    :returns: Sumário operacional e financeiro consolidado.
    """
    return gerar_sumario_lote(resultados)
