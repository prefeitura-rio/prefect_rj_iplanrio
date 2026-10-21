"""Tarefas atômicas (Tasks) da pipeline SMTR no Prefect 3.0."""

from datetime import datetime
import time
from typing import Any, Dict, List, Optional
import pandas as pd
from prefect import get_run_logger, task

from .constants import (
    BETA_CONSENSO,
    BETA_DIVERGENCIA,
    FaixaOperacional,
    LIMIAR_ALTA,
    LIMIAR_BAIXA,
    LIMIAR_DISPERSAO_COT_MAX,
    LIMIAR_FAIXA_AMARELA_PISO,
    MAX_PAGINAS_PDF,
    PROMPTS_DIR,
    RotaExecucao,
    VERSAO_PROMPT_PADRAO,
    VERTEX_LLM_MODEL,
    resolver_banco_url,
)
from .utils.artifacts import carregar_baseline_referencia
from .utils.conformal import criar_artefatos_conforme_do_banco
from .utils.cost_analyzer import calcular_custo_chamada
from .utils.db_repository import (
    buscar_configuracao_calibracao_ativa,
    buscar_processos_para_triagem,
    filtrar_processos_ja_triados,
    persistir_lote_resultados_ia,
)
from .utils.feature_extractor import extrair_sinais_epistemicos
from .utils.guardrails import aplicar_guardrail_epistemico
from .utils.meta_calibrator import (
    criar_calibrador_dos_parametros_banco,
    predizer_probabilidade_calibrada,
)
from .utils.ocr_engine import extrair_ocr_pagina_especifica
from .utils.providers.openai_client import OpenAILLMClient
from .utils.router import (
    atualizar_probabilidade_bayesiana_log_odds,
    calcular_dispersao_cot,
    construir_prediction_set_aps,
)
from .utils.storage import carregar_bytes_pdf
from .utils.windowing import extrair_smart_window_pdf


@task(name="carregar_artefatos_referencia", retries=2, retry_delay_seconds=5)
def task_carregar_artefatos_referencia(
    is_local: bool = True,
    arquivo_prompt: str = f"{VERSAO_PROMPT_PADRAO}.txt",
) -> Dict[str, Any]:
    """Carrega parâmetros de calibração do PostgreSQL com fallback automático para artefatos congelados.

    :param is_local: Booleano que define se a conexão é local ou Cloud SQL.
    :param arquivo_prompt: Nome do arquivo de prompt a carregar de prompts/.
    :returns: Dicionário contendo o calibrador, artefatos conformais, prompt e metadados.
    """
    logger = get_run_logger()
    db_url = resolver_banco_url(is_local)

    calibrador = None
    conformal = None
    calibracao_id = None
    versao_modelo = "baseline_congelada_v3"

    try:
        logger.info("Recuperando configuração de calibração ativa no PostgreSQL...")
        config_db = buscar_configuracao_calibracao_ativa(db_url)
        calibrador = criar_calibrador_dos_parametros_banco(config_db)
        conformal = criar_artefatos_conforme_do_banco(config_db)
        calibracao_id = config_db.get("calibracao_id")
        versao_modelo = config_db.get("versao_modelo", "db_ativa")
        logger.info(
            "Configuração ativa do banco carregada com sucesso | Versão: %s | Calibração ID: %s",
            versao_modelo,
            calibracao_id,
        )
    except Exception as exc:
        logger.warning(
            "Configuração ativa no banco indisponível (%s). Carregando baseline congelada dos artefatos...",
            exc,
        )
        baseline = carregar_baseline_referencia()
        calibrador = baseline.calibrador
        conformal = baseline.conformal

    caminho_prompt = PROMPTS_DIR / arquivo_prompt
    if not caminho_prompt.exists():
        # Fallback para o prompt padrão
        caminho_prompt = PROMPTS_DIR / f"{VERSAO_PROMPT_PADRAO}.txt"

    prompt_sistema = caminho_prompt.read_text(encoding="utf-8")
    versao_prompt = caminho_prompt.stem

    return {
        "calibrador": calibrador,
        "conformal": conformal,
        "prompt_sistema": prompt_sistema,
        "calibracao_id": calibracao_id,
        "versao_modelo": versao_modelo,
        "versao_prompt": versao_prompt,
    }


@task(name="buscar_processos_pendentes", retries=2, retry_delay_seconds=5)
def task_buscar_processos_pendentes(
    is_local: bool = True,
    fonte_pdf: str = "local",
    cliente_sei: Optional[Any] = None,
    modo: str = "INCREMENTAL",
    limite: int = 300,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Consulta ou filtra os processos elegíveis para triagem.

    No modo 'sei', lista os processos da caixa da JARI via cliente SEI e filtra os já triados no banco.
    No modo 'local', busca no banco os processos com status PENDENTE_IA.
    """
    logger = get_run_logger()
    db_url = resolver_banco_url(is_local)

    # 1. Integração Direta com a Caixa do SEI
    if fonte_pdf == "sei" and cliente_sei is not None:
        logger.info("Listando processos diretamente da caixa da JARI no SEI...")
        processos_caixa = cliente_sei.listar_processos_caixa()
        if not processos_caixa:
            logger.info("Caixa da JARI no SEI vazia.")
            return []

        numeros_sei = [
            str(p.get("numero_processo_sei") or p.get("numero_processo"))
            for p in processos_caixa
        ]

        try:
            ja_triados = set(filtrar_processos_ja_triados(db_url, numeros_sei))
        except Exception as exc:
            logger.warning(
                "Falha ao consultar banco para idempotência (%s). Processando lote da caixa.",
                exc,
            )
            ja_triados = set()

        processos_pendentes = [
            p for p in processos_caixa
            if str(p.get("numero_processo_sei") or p.get("numero_processo")) not in ja_triados
        ]
        logger.info(
            "SEI: %d processo(s) na caixa | %d já triado(s) | %d a triar.",
            len(processos_caixa),
            len(ja_triados),
            len(processos_pendentes),
        )
        return processos_pendentes[:limite]

    # 2. Modo Padrão via Banco de Dados
    logger.info(
        "Buscando processos na fila do banco | Modo: %s | Limite: %d",
        modo,
        limite,
    )
    try:
        processos = buscar_processos_para_triagem(
            db_url=db_url,
            modo=modo,
            limite=limite,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )
        logger.info("%d processo(s) elegível(is) encontrado(s) no banco.", len(processos))
        return processos
    except Exception as exc:
        logger.warning(
            "Falha ao consultar fila no banco (%s). Verificando PDFs na pasta local...",
            exc,
        )
        # Fallback local para desenvolvimento: lê os PDFs da pasta local
        from .constants import LOCAL_PDFS_DIR
        def _chave_ordenacao(p):
            try:
                return int(p.stem.split("(")[1].split(")")[0])
            except Exception:
                return 0
        arquivos_req = sorted(LOCAL_PDFS_DIR.glob("REQUERIMENTOS INTEMPESTIVOS*.pdf"), key=_chave_ordenacao)
        arquivos_locais = arquivos_req if arquivos_req else sorted(LOCAL_PDFS_DIR.glob("*.pdf"))
        return [
            {
                "processo_id": f"00000000-0000-0000-0000-{i:012d}",
                "numero_processo_sei": f"SEI-{arq.stem[:28]}",
                "id_documento_sei": None,
                "id_procedimento_sei": None,
                "numero_auto_infracao": "AUTO_LOCAL",
                "nome_arquivo_origem": arq.name,
                "data_autuacao": None,
            }
            for i, arq in enumerate(arquivos_locais[:limite], 1)
        ]


@task(
    name="triar_processo_individual",
    retries=2,
    retry_delay_seconds=5,
    timeout_seconds=240,
)
def task_triar_processo_individual(
    processo: Dict[str, Any],
    artefatos: Dict[str, Any],
    is_local: bool = True,
    fonte_pdf: str = "local",
    cliente_sei: Optional[Any] = None,
    limiar_alta: float = LIMIAR_ALTA,
    limiar_baixa: float = LIMIAR_BAIXA,
    limiar_dispersao_max: float = LIMIAR_DISPERSAO_COT_MAX,
    max_paginas: int = MAX_PAGINAS_PDF,
    modelo_llm: str = VERTEX_LLM_MODEL,
) -> Dict[str, Any]:
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
    :returns: Dicionário detalhado contendo a predição e o vetor epistêmico.
    """
    logger = get_run_logger()

    nome_arquivo = processo.get("nome_arquivo_origem") or f"{processo.get('numero_processo_sei', 'doc')}.pdf"
    id_doc_sei = processo.get("id_documento_sei")
    processo_id = str(processo.get("processo_id", ""))
    numero_sei = str(processo.get("numero_processo_sei", "SEI_SEM_NUMERO"))

    identificador_pdf = id_doc_sei if (fonte_pdf == "sei" and id_doc_sei) else nome_arquivo

    t_inicio = time.time()
    logger.info("Iniciando triagem do processo SEI: %s (Fonte: %s)", numero_sei, fonte_pdf)

    calibrador = artefatos["calibrador"]
    conformal = artefatos["conformal"]
    prompt_sistema = artefatos["prompt_sistema"]
    calibracao_id = artefatos.get("calibracao_id")
    versao_prompt = artefatos.get("versao_prompt", "v3_baseline")
    stats_classes = getattr(calibrador, "estatisticas_classes", None)

    cliente_llm = OpenAILLMClient(default_model=modelo_llm)

    # 1. Download em memória RAM (sem salvar em disco ou GCS)
    pdf_bytes = carregar_bytes_pdf(
        identificador=identificador_pdf,
        fonte_pdf=fonte_pdf,
        cliente_sei=cliente_sei,
    )
    pdf_window_bytes = extrair_smart_window_pdf(pdf_bytes, max_paginas=max_paginas)

    # 2. Inferência Primária N=1 (T=0.0)
    decisao_1, usage_1 = cliente_llm.classificar(
        pdf_bytes=pdf_window_bytes,
        prompt_sistema=prompt_sistema,
        modelo=modelo_llm,
        temperature=0.0,
    )
    if decisao_1 is None:
        raise ValueError(f"A LLM retornou decisão nula para o processo {numero_sei}")

    decisao_1 = aplicar_guardrail_epistemico(decisao_1)
    hipotese_1_str = str(
        decisao_1.hipotese_legal.value
        if hasattr(decisao_1.hipotese_legal, "value")
        else decisao_1.hipotese_legal
    )

    # 3. Lazy OCR com tolerância de lauda adjacente
    texto_ocr, ocr_conf = "", 0.0
    if decisao_1.pagina_evidencia and decisao_1.pagina_evidencia > 0:
        resultado_ocr = extrair_ocr_pagina_especifica(
            pdf_origem=pdf_bytes,
            num_pagina=decisao_1.pagina_evidencia,
            dpi=150,
            idioma="por",
            frase_procurada=decisao_1.frase_justificativa,
        )
        if resultado_ocr and isinstance(resultado_ocr, tuple) and len(resultado_ocr) == 2:
            texto_ocr, ocr_conf = resultado_ocr

    # 4. Extração das 8 features ativas
    sinais_1 = extrair_sinais_epistemicos(
        decisao=decisao_1,
        texto_ocr_pagina=texto_ocr,
        tesseract_mean_conf=ocr_conf,
        estatisticas_classes=stats_classes,
    )

    # 5. Meta-calibração tabular supervisionada
    p_cal_1 = predizer_probabilidade_calibrada(sinais_1, calibrador)
    set_conformal = construir_prediction_set_aps(
        decisao=decisao_1,
        p_cal=p_cal_1,
        estatisticas_classes=stats_classes,
        alpha=getattr(conformal, "alpha", 0.05),
    )

    # 6. Avaliação do roteamento adaptativo
    if p_cal_1 >= limiar_alta and len(set_conformal) <= 1:
        rota_inicial = RotaExecucao.N1_DIRETO.value
    elif p_cal_1 < limiar_baixa or len(set_conformal) == 0:
        rota_inicial = RotaExecucao.HITL_BAIXA_CONFIANCA.value
    else:
        rota_inicial = RotaExecucao.N2_CONSENSO.value

    estagio2_executado = False
    delta_cot = None
    log_odds_atualizado = None
    p_final = p_cal_1
    rota_final = rota_inicial
    decisao_2 = None
    usage_2 = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0}

    # 7. Estágio N=2 Condicional (T=0.7)
    if rota_inicial not in [
        RotaExecucao.N1_DIRETO.value,
        RotaExecucao.HITL_BAIXA_CONFIANCA.value,
    ]:
        logger.info(
            "Incerteza operacional em %s (p1=%.3f). Disparando N=2 (T=0.7)...",
            numero_sei,
            p_cal_1,
        )
        decisao_2, usage_2 = cliente_llm.classificar(
            pdf_bytes=pdf_window_bytes,
            prompt_sistema=prompt_sistema,
            modelo=modelo_llm,
            temperature=0.7,
        )
        decisao_2 = aplicar_guardrail_epistemico(decisao_2)
        estagio2_executado = True

        hip_1 = hipotese_1_str
        hip_2 = str(
            decisao_2.hipotese_legal.value
            if hasattr(decisao_2.hipotese_legal, "value")
            else decisao_2.hipotese_legal
        )

        delta_cot = calcular_dispersao_cot(
            decisao_1.justificativa_analise_previa,
            decisao_2.justificativa_analise_previa,
        )

        resultado_bayes = atualizar_probabilidade_bayesiana_log_odds(
            p_1=p_cal_1,
            hip_1=hip_1,
            hip_2=hip_2,
            delta_cot=delta_cot,
            beta_consenso=BETA_CONSENSO,
            beta_divergencia=BETA_DIVERGENCIA,
        )
        if resultado_bayes and isinstance(resultado_bayes, tuple) and len(resultado_bayes) == 2:
            p_final, rota_final = resultado_bayes
        else:
            p_final = p_cal_1

        log_odds_atualizado = p_final
        if rota_final == RotaExecucao.HITL_DIVERGENCIA_DUPLA.value:
            set_conformal = {hip_1, hip_2}

    # 8. Faixa Operacional (Piso de 0.50 fixo para Faixa Amarela)
    if p_final >= limiar_alta:
        faixa = FaixaOperacional.VERDE.value
    elif p_final >= LIMIAR_FAIXA_AMARELA_PISO:
        faixa = FaixaOperacional.AMARELA.value
    else:
        faixa = FaixaOperacional.VERMELHA.value

    # 9. Consolidação dos tokens reais consumidos
    total_prompt_tokens = usage_1.get("prompt_tokens", 0) + usage_2.get("prompt_tokens", 0)
    total_completion_tokens = usage_1.get("completion_tokens", 0) + usage_2.get("completion_tokens", 0)
    total_cached_tokens = usage_1.get("cached_tokens", 0) + usage_2.get("cached_tokens", 0)

    tempo_total = round(time.time() - t_inicio, 2)
    info_custo = calcular_custo_chamada(
        modelo=modelo_llm,
        prompt_tokens=total_prompt_tokens,
        completion_tokens=total_completion_tokens,
        cached_tokens=total_cached_tokens,
        aplicar_cache_estatico=True,
    )

    # Extrai dados estruturados de auditoria de sistema
    aud_dict = {}
    if hasattr(decisao_1, "auditoria_sistema") and decisao_1.auditoria_sistema:
        aud = decisao_1.auditoria_sistema
        aud_dict = {
            "prefixo_auto": getattr(aud, "prefixo_auto", None),
            "codigo_status": getattr(aud, "codigo_status", None),
            "fase_descricao": getattr(aud, "fase_descricao", None),
            "possui_espelho_nada_consta": getattr(aud, "possui_espelho_nada_consta", True),
        }

    logger.info(
        "Triagem de %s concluída em %.2fs | Faixa: %s | p_final: %.4f | Rota: %s | Tokens: In=%d, Out=%d",
        numero_sei,
        tempo_total,
        faixa,
        p_final,
        rota_final,
        total_prompt_tokens,
        total_completion_tokens,
    )

    return {
        "processo_id": processo_id,
        "numero_processo_sei": numero_sei,
        "calibracao_id": calibracao_id,
        "modelo_llm": modelo_llm,
        "versao_prompt": versao_prompt,
        "id_documento_sei": id_doc_sei,
        "n1_classe_predita": hipotese_1_str,
        "n1_encaminhamento": str(
            decisao_1.encaminhamento.value
            if hasattr(decisao_1.encaminhamento, "value")
            else decisao_1.encaminhamento
        ),
        "n1_cot_justificativa": decisao_1.justificativa_analise_previa,
        "n1_lauda_evidencia": decisao_1.pagina_evidencia,
        "n1_frase_citada": decisao_1.frase_justificativa,
        "n1_tipo_citacao": str(
            decisao_1.tipo_citacao.value
            if hasattr(decisao_1.tipo_citacao, "value")
            else decisao_1.tipo_citacao
        ),
        "sinais_epistemicos": sinais_1.to_dict(),
        "auditoria_sistema": aud_dict,
        "probabilidade_calibrada": p_cal_1,
        "conjunto_conformal": list(set_conformal),
        "tamanho_conjunto_conformal": len(set_conformal),
        "estagio2_executado": estagio2_executado,
        "n2_classe_predita": (
            str(
                decisao_2.hipotese_legal.value
                if hasattr(decisao_2.hipotese_legal, "value")
                else decisao_2.hipotese_legal
            )
            if decisao_2
            else None
        ),
        "n2_cot_justificativa": (
            decisao_2.justificativa_analise_previa if decisao_2 else None
        ),
        "n2_delta_cot_dispersao": delta_cot,
        "n2_log_odds_atualizado": log_odds_atualizado,
        "probabilidade_final": p_final,
        "rota_execucao": rota_final,
        "faixa_operacional": faixa,
        "tokens_prompt_total": total_prompt_tokens,
        "tokens_completion": total_completion_tokens,
        "tokens_cached": total_cached_tokens,
        "custo_real_usd": info_custo["custo_total_usd"],
        "custo_sem_cache_usd": info_custo["custo_sem_cache_usd"],
        "economia_cache_usd": info_custo["economia_cache_usd"],
        "custo_estimado_usd": info_custo["custo_total_usd"],
        "tempo_execucao_s": tempo_total,
    }


@task(name="persistir_lote_resultados", retries=2, retry_delay_seconds=5)
def task_persistir_lote_resultados(
    resultados: List[Dict[str, Any]], is_local: bool = True
) -> None:
    """Persiste as predições e auditorias na base e atualiza a fila de trabalho."""
    logger = get_run_logger()
    db_url = resolver_banco_url(is_local)
    logger.info("Gravando %d resultado(s) no banco de dados...", len(resultados))
    try:
        persistir_lote_resultados_ia(db_url, resultados)
        logger.info("Resultados persistidos com sucesso.")
    except Exception as exc:
        logger.error("Falha ao persistir no banco de dados: %s", exc)
        # Em modo local/teste, não interrompe abruptamente se for falha de conexão
        if not is_local:
            raise


@task(name="gerar_sumario_lote")
def task_gerar_sumario_lote(resultados: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Consolida as métricas operacionais e financeiras da execução do lote."""
    logger = get_run_logger()
    total = len(resultados)
    if total == 0:
        return {}

    df = pd.DataFrame(resultados)
    qtd_verde = int((df["faixa_operacional"] == FaixaOperacional.VERDE.value).sum())
    qtd_amarela = int((df["faixa_operacional"] == FaixaOperacional.AMARELA.value).sum())
    qtd_vermelha = int((df["faixa_operacional"] == FaixaOperacional.VERMELHA.value).sum())

    total_chamadas = int(df["estagio2_executado"].sum()) + total
    mult_chamadas = round(total_chamadas / total, 2)
    custo_total = round(float(df["custo_estimado_usd"].sum()), 4)
    custo_sem_cache = round(float(df["custo_sem_cache_usd"].sum()), 4) if "custo_sem_cache_usd" in df else custo_total
    economia_cache = round(float(df["economia_cache_usd"].sum()), 4) if "economia_cache_usd" in df else 0.0

    total_tokens_in = int(df["tokens_prompt_total"].sum()) if "tokens_prompt_total" in df else 0
    total_tokens_out = int(df["tokens_completion"].sum()) if "tokens_completion" in df else 0
    total_tokens_cache = int(df["tokens_cached"].sum()) if "tokens_cached" in df else 0
    pct_cache = round((total_tokens_cache / total_tokens_in * 100), 1) if total_tokens_in > 0 else 0.0

    sumario = {
        "timestamp": datetime.now().isoformat(),
        "total": total,
        "faixa_verde": qtd_verde,
        "faixa_amarela": qtd_amarela,
        "faixa_vermelha": qtd_vermelha,
        "multiplicador_chamadas": mult_chamadas,
        "tokens_prompt_total": total_tokens_in,
        "tokens_cached": total_tokens_cache,
        "tokens_completion": total_tokens_out,
        "taxa_aproveitamento_cache_pct": pct_cache,
        "custo_real_usd": custo_total,
        "custo_sem_cache_usd": custo_sem_cache,
        "economia_cache_usd": economia_cache,
        "custo_usd": custo_total,
    }

    logger.info(
        "SUMÁRIO OPERACIONAL: Total: %d | Verde: %d | Amarela: %d | Vermelha: %d | Chamadas: %.2fx",
        total,
        qtd_verde,
        qtd_amarela,
        qtd_vermelha,
        mult_chamadas,
    )
    logger.info(
        "SUMÁRIO FINANCEIRO / CACHE: Tokens In: %d (Cached: %d | %.1f%%) | Tokens Out: %d | Custo Real: $%.4f USD | Economia Cache: $%.4f USD",
        total_tokens_in,
        total_tokens_cache,
        pct_cache,
        total_tokens_out,
        custo_total,
        economia_cache,
    )
    return sumario
