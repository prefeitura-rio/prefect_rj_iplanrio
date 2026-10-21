"""
Flow principal Prefect 3 para triagem de recursos SMTR-RJ.
Suporta processamento em memória a partir de PDFs locais ou integração direta com o SEI.
"""

import argparse
from typing import Any, Dict, Optional
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
    task_runner=ThreadPoolTaskRunner(max_workers=4),
    log_prints=True,
    description="Pipeline de triagem e classificação seletiva de recursos da SMTR-RJ.",
)
def rj_smtr__triagem_recursos(
    is_local: bool = True,
    fonte_pdf: str = "local",
    cliente_sei: Optional[Any] = None,
    modo_execucao: str = "INCREMENTAL",
    limite_lote: int = 300,
    limiar_alta: float = LIMIAR_ALTA,
    limiar_baixa: float = LIMIAR_BAIXA,
    limiar_dispersao: float = LIMIAR_DISPERSAO_COT_MAX,
    max_paginas: int = MAX_PAGINAS_PDF,
    modelo_llm: str = VERTEX_LLM_MODEL,
    arquivo_prompt: str = f"{VERSAO_PROMPT_PADRAO}.txt",
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    infisical_secret_path: Optional[str] = None,
) -> Dict[str, Any]:
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
    )

    # 4. Persiste o lote de predições
    task_persistir_lote_resultados(resultados_futuros, is_local=is_local)

    # 5. Consolida sumário operacional
    return task_gerar_sumario_lote(resultados_futuros)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Executa o flow de triagem SMTR localmente ou integrado ao SEI."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        default=True,
        help="Executa em modo local.",
    )
    parser.add_argument(
        "--cloud",
        action="store_false",
        dest="local",
        help="Executa em modo nuvem (Cloud SQL).",
    )
    parser.add_argument(
        "--fonte-pdf",
        default="local",
        choices=["local", "sei"],
        help="Origem dos PDFs: 'local' (pasta pdfs/) ou 'sei' (download em memória).",
    )
    parser.add_argument(
        "--modo",
        default="INCREMENTAL",
        choices=["INCREMENTAL", "PASSIVO"],
        help="Modo de carga.",
    )
    parser.add_argument(
        "--limite",
        type=int,
        default=5,
        help="Quantidade máxima de processos no lote de teste.",
    )
    args = parser.parse_args()

    rj_smtr__triagem_recursos(
        is_local=args.local,
        fonte_pdf=args.fonte_pdf,
        modo_execucao=args.modo,
        limite_lote=args.limite,
    )
