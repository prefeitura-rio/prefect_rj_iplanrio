"""
Flow principal Prefect 3 com suporte nativo a --local via CLI ou parâmetros.
"""

import argparse
from typing import Any, Dict, Optional
from prefect import flow, get_run_logger, unmapped

from .constants import (
    DEFAULT_ENVIRONMENT,
    LIMIAR_ALTA,
    LIMIAR_BAIXA,
    LIMIAR_DISPERSAO_COT_MAX,
    MAX_PAGINAS_PDF,
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
    log_prints=True,
    description="Pipeline de triagem e classificação seletiva de recursos da SMTR-RJ.",
)
def rj_smtr__triagem_recursos(
    is_local: bool = True,
    modo_execucao: str = "INCREMENTAL",
    limite_lote: int = 300,
    limiar_alta: float = LIMIAR_ALTA,
    limiar_baixa: float = LIMIAR_BAIXA,
    limiar_dispersao: float = LIMIAR_DISPERSAO_COT_MAX,
    max_paginas: int = MAX_PAGINAS_PDF,
    modelo_llm: str = VERTEX_LLM_MODEL,
    arquivo_prompt: str = "v3_baseline.txt",
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    infisical_secret_path: Optional[str] = None,
) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info(
        f"Iniciando pipeline | Local={is_local} | Modo={modo_execucao} |"
        f" Limite={limite_lote}"
    )

    # Carrega artefatos da baseline
    artefatos = task_carregar_artefatos_referencia(
        is_local=is_local, arquivo_prompt=arquivo_prompt
    )

    # Busca processos na fila
    processos = task_buscar_processos_pendentes(
        is_local=is_local,
        modo=modo_execucao,
        limite=limite_lote,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

    if not processos:
        logger.info("Nenhum processo pendente na fila. Encerrando execução.")
        return {"status": "SEM_DADOS", "total": 0}

    # Mapeamento concorrente sobre cada processo
    resultados_futuros = task_triar_processo_individual.map(
        processo=processos,
        artefatos=unmapped(artefatos),
        is_local=unmapped(is_local),
        limiar_alta=unmapped(limiar_alta),
        limiar_baixa=unmapped(limiar_baixa),
        limiar_dispersao_max=unmapped(limiar_dispersao),
        max_paginas=unmapped(max_paginas),
        modelo_llm=unmapped(modelo_llm),
    )
    
    task_persistir_lote_resultados(resultados_futuros, is_local=is_local)

    return task_gerar_sumario_lote(resultados_futuros)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Executa o flow de triagem SMTR localmente ou na nuvem."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        default=True,
        help="Força execução local (Postgres Docker e PDFs locais).",
    )
    parser.add_argument(
        "--cloud",
        action="store_false",
        dest="local",
        help="Executa em modo nuvem (Cloud SQL e GCS).",
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
        modo_execucao=args.modo,
        limite_lote=args.limite,
    )