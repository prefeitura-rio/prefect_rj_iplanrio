"""Flow principal Prefect 3 para triagem de recursos SMTR-RJ.

Suporta processamento em memória a partir de PDFs locais ou integração direta com o SEI.
Adere estritamente ao Styleguide do repositório (Seções 4.1 e 6.1 a 6.4).
"""

from typing import Any
from prefect import flow, get_run_logger, unmapped
from prefect.task_runners import ThreadPoolTaskRunner

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


@flow(
    name="rj_smtr__triagem_recursos",
    task_runner=ThreadPoolTaskRunner(max_workers=8),
    log_prints=True,
    description="Pipeline de triagem e classificação seletiva de recursos da SMTR-RJ.",
)
def rj_smtr__triagem_recursos(
    is_local: bool = True,
    fonte_pdf: str = "local",
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
    infisical_secret_path: str | None = None,
    reasoning_effort: str = "medium",
    arquivos_especificos: list[Any] | None = None,
    executar_n2: bool = False,
) -> dict[str, Any]:
    """Orquestra a triagem automatizada e calibração de recursos da SMTR.

    :param is_local: Se True, opera contra banco local; False para Cloud SQL.
    :param fonte_pdf: Origem dos documentos ("local" ou "sei").
    :param cliente_sei: Cliente do SEI opcional para download em memória.
    :param modo_execucao: Modo de carga ("INCREMENTAL" ou "PASSIVO").
    :param limite_lote: Quantidade máxima de processos a triar nesta execução.
    :param limiar_alta: Ponto de corte para admissão na Faixa Verde.
    :param limiar_baixa: Limiar inferior para envio direto ao HITL.
    :param limiar_dispersao: Tolerância máxima de dispersão de CoT para N=2.
    :param max_paginas: Quantidade máxima de laudas da petição (smart windowing).
    :param modelo_llm: Nome do modelo de linguagem no provedor/gateway.
    :param arquivo_prompt: Nome do arquivo em prompts/ a utilizar.
    :param data_inicio: Filtro temporal inicial opcional.
    :param data_fim: Filtro temporal final opcional.
    :param infisical_secret_path: Caminho de credenciais no Infisical.
    :param reasoning_effort: Esforço de raciocínio da LLM ("high", "medium", "low", "off").
    :param arquivos_especificos: Lista arbitrária opcional de arquivos para triagem pontual.
    :param executar_n2: Flag para habilitar o segundo estágio adaptativo sob incerteza.
    :returns: Sumário operacional e financeiro consolidado do lote.
    """
    logger = get_run_logger()
    logger.info(
        "Iniciando pipeline | Local=%s | Fonte=%s | Modo=%s | Limite=%d",
        is_local,
        fonte_pdf,
        modo_execucao,
        limite_lote,
    )

    # 1. Carrega artefatos da baseline (PostgreSQL com fallback para joblib congelado)
    artefatos = task_carregar_artefatos_referencia(
        is_local=is_local, arquivo_prompt=arquivo_prompt
    )

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

    # Resolve os futuros concorrentes dos workers
    resultados = [f.result() for f in resultados_futuros]

    # 4. Persiste o lote de predições
    task_persistir_lote_resultados(resultados, is_local=is_local)

    # 5. Consolida sumário operacional
    return task_gerar_sumario_lote(resultados)
