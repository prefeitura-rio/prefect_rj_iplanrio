"""Motor analítico de triagem processual individual e consolidação operacional do lote.

Descrição Técnica & Arquitetura:
Coordena a esteira analítica completa para cada recurso administrativo individual.
Opera estritamente em memória RAM para garantir conformidade com a LGPD (sem salvar PDFs
em disco nem no Cloud Storage). Executa o fluxo em etapas:
1. Obtenção do stream binário do PDF (via pasta local ou API REST do SEI-Rio).
2. Smart Windowing para retenção seletiva das primeiras laudas (núcleo da petição).
3. Inferência primária N=1 (T=0.0) com a LLM (Gemini 3.7 Flash via Bifrost).
4. Saneamento por guardrails epistêmicos e extração lazy de OCR na lauda citada.
5. Extração dos 8 sinais epistêmicos e meta-calibração probabilística via LightGBM (P_cal).
6. Construção do conjunto conformal condicional por classe (CCCP).
7. Bifurcação sequencial adaptativa:
   - Se P_cal >= 0.85 e |C(X)| == 1: promoção direta para Faixa Verde.
   - Se 0.50 <= P_cal < 0.85: disparo obrigatório do Estágio N=2 (T=0.70), medição de
     dispersão semântica (Delta_CoT) e atualização Bayesiana em escala de log-odds.
   - Se P_cal < 0.50: roteamento direto para Faixa Vermelha / HITL.
8. Contabilidade analítica de tokens, custos reais e economia de Prompt Caching.

Fluxo de Dependências:
- Consumidores Diretos:
  * `tasks.py`: invocado por `task_triar_processo_individual` para cada processo do lote
    e por `task_gerar_sumario_lote` para consolidar o sumário estatístico-financeiro.
- Dependências Diretas:
  * `constants.py`: importa limiares de confiança (0.85 e 0.50), parâmetros e enums de rotas e faixas.
  * `utils/storage.py`: carrega os bytes do PDF diretamente em buffer de memória RAM.
  * `utils/windowing.py`: trunca o PDF para a janela de contexto configurada (máximo 20 laudas).
  * `utils/providers/openai_client.py`: efetua a chamada multimodal estruturada no gateway Bifrost.
  * `utils/guardrails.py`: valida e corrige alucinações epistêmicas na resposta da LLM.
  * `utils/ocr_engine.py`: extrai o texto físico da página apontada como evidência pelo modelo.
  * `utils/feature_extractor.py`: calcula os 8 sinais epistêmicos (similaridade, Mahalanobis, k-NN).
  * `utils/meta_calibrator.py`: computa a probabilidade calibrada supervisionada P_cal.
  * `utils/conformal.py`: constrói o prediction set conforme para mensurar ambiguidade de classes.
  * `utils/router.py`: provê a lógica de N=2 sob incerteza moderada, cálculo de Delta_CoT e Bayes log-odds.
  * `utils/cost_analyzer.py`: apura o custo de tokens em dólares e o valor economizado com cache.
"""

import time
from datetime import datetime
from typing import Any

import pandas as pd

import logging

try:
    from prefect_rj_iplanrio.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

from ..constants import (
    BETA_CONSENSO,
    BETA_DIVERGENCIA,
    LIMIAR_ALTA,
    LIMIAR_BAIXA,
    LIMIAR_DISPERSAO_COT_MAX,
    LIMIAR_FAIXA_AMARELA_PISO,
    MAX_PAGINAS_PDF,
    VERTEX_LLM_MODEL,
    FaixaOperacional,
    RotaExecucao,
)
from .cost_analyzer import calcular_custo_chamada
from .feature_extractor import extrair_sinais_epistemicos
from .guardrails import aplicar_guardrail_epistemico
from .meta_calibrator import predizer_probabilidade_calibrada
from .ocr_engine import extrair_ocr_pagina_especifica
from .providers.openai_client import OpenAILLMClient
from .router import (
    atualizar_probabilidade_bayesiana_log_odds,
    calcular_dispersao_cot,
    construir_prediction_set_aps,
)
from .storage import carregar_bytes_pdf
from .windowing import extrair_smart_window_pdf



def triar_processo(
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
    :raises ValueError: Se a resposta da LLM for nula.
    """
    nome_arquivo = processo.get("nome_arquivo_origem") or f"{processo.get('numero_processo_sei', 'doc')}.pdf"
    id_procedimento_sei = processo.get("id_procedimento_sei")
    id_doc_sei = processo.get("id_documento_sei")
    processo_id = str(processo.get("processo_id", ""))
    numero_sei = str(processo.get("numero_processo_sei", "SEI_SEM_NUMERO"))

    t_inicio = time.time()
    logger.info(
        "Iniciando triagem do processo SEI: %s (Fonte: %s)",
        numero_sei,
        fonte_pdf,
    )

    calibrador = artefatos["calibrador"]
    conformal = artefatos["conformal"]
    prompt_sistema = artefatos["prompt_sistema"]
    calibracao_id = artefatos.get("calibracao_id")
    versao_prompt = artefatos.get("versao_prompt", "v4_baseline")
    stats_classes = getattr(calibrador, "estatisticas_classes", None)

    cliente_llm = OpenAILLMClient(default_model=modelo_llm)

    # 1. Download em memória RAM (sem salvar em disco ou GCS) e Smart Windowing
    try:
        if fonte_pdf == "sei" and cliente_sei is not None:
            if hasattr(cliente_sei, "obter_pdf_recurso_processo") and id_procedimento_sei and not id_doc_sei:
                id_doc_sei, pdf_bytes = cliente_sei.obter_pdf_recurso_processo(id_procedimento_sei)
                processo["id_documento_sei"] = id_doc_sei
            else:
                pdf_bytes = carregar_bytes_pdf(
                    identificador=id_doc_sei or processo.get("nome_arquivo_origem", ""),
                    fonte_pdf=fonte_pdf,
                    cliente_sei=cliente_sei,
                )
        else:
            pdf_bytes = carregar_bytes_pdf(
                identificador=processo["nome_arquivo_origem"],
                fonte_pdf=fonte_pdf,
                cliente_sei=cliente_sei,
            )

        if not pdf_bytes:
            raise ValueError(f"Arquivo de recurso vazio ou nulo para o processo {numero_sei}.")

        pdf_window_bytes = extrair_smart_window_pdf(pdf_bytes, max_paginas=max_paginas)
    except Exception as exc:
        logger.error(
            "Falha na obtenção/validação do PDF para o processo %s: %s",
            numero_sei,
            exc,
        )
        return {
            "processo_id": processo_id,
            "id_documento_sei": id_doc_sei,
            "numero_processo_sei": numero_sei,
            "calibracao_id": calibracao_id,
            "modelo_llm": modelo_llm,
            "versao_prompt": versao_prompt,
            "n1_classe_predita": "OUTROS",
            "n1_encaminhamento": "REVISAO_HUMANA",
            "n1_cot_justificativa": f"Falha de integridade do documento ou download no SEI: {exc}",
            "n1_lauda_evidencia": 1,
            "n1_frase_citada": "",
            "n1_tipo_citacao": "DOCUMENTO_ILEGIVEL",
            "sinais_epistemicos": {},
            "probabilidade_calibrada": 0.0,
            "probabilidade_final": 0.0,
            "conjunto_conformal": ["OUTROS"],
            "tamanho_conjunto_conformal": 1,
            "rota_execucao": "HITL_ERRO_ARQUIVO",
            "faixa_operacional": "VERMELHA",
            "estagio2_executado": False,
            "n2_classe_predita": None,
            "n2_cot_justificativa": None,
            "n2_delta_cot_dispersao": None,
            "n2_log_odds_atualizado": None,
            "status_fila": StatusProcessamento.ERRO_PROCESSAMENTO.value,
            "status_triagem": "ERRO_PDF_CORROMPIDO",
            "tokens_prompt_total": 0,
            "tokens_completion": 0,
            "tokens_cached": 0,
            "custo_real_usd": 0.0,
            "custo_sem_cache_usd": 0.0,
            "economia_cache_usd": 0.0,
            "tempo_execucao_s": round(time.time() - t_inicio, 3),
            "auditoria_sistema": {
                "erro_detalhado": str(exc),
                "status": "ERRO_PDF_CORROMPIDO",
            },
        }

    # 2. Inferência Primária N=1 (T=0.0)
    decisao_1, usage_1 = cliente_llm.classificar(
        pdf_bytes=pdf_window_bytes,
        prompt_sistema=prompt_sistema,
        modelo=modelo_llm,
        temperature=0.0,
        reasoning_effort=reasoning_effort,
    )
    if decisao_1 is None:
        raise ValueError(f"A LLM retornou decisão nula para o processo {numero_sei}")

    decisao_1 = aplicar_guardrail_epistemico(decisao_1)
    hipotese_1_str = str(
        decisao_1.hipotese_legal.value if hasattr(decisao_1.hipotese_legal, "value") else decisao_1.hipotese_legal
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
    if executar_n2 and rota_inicial not in [
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
            reasoning_effort=reasoning_effort,
        )
        decisao_2 = aplicar_guardrail_epistemico(decisao_2)
        estagio2_executado = True

        hip_1 = hipotese_1_str
        hip_2 = str(
            decisao_2.hipotese_legal.value if hasattr(decisao_2.hipotese_legal, "value") else decisao_2.hipotese_legal
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
        aplicar_cache_estatico=False,
    )

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
        "nome_arquivo_origem": nome_arquivo,
        "ouro_hipotese": processo.get("ouro_hipotese"),
        "ouro_encaminhamento": processo.get("ouro_encaminhamento"),
        "calibracao_id": calibracao_id,
        "modelo_llm": modelo_llm,
        "versao_prompt": versao_prompt,
        "id_documento_sei": id_doc_sei,
        "n1_classe_predita": hipotese_1_str,
        "n1_encaminhamento": str(
            decisao_1.encaminhamento.value if hasattr(decisao_1.encaminhamento, "value") else decisao_1.encaminhamento
        ),
        "n1_cot_justificativa": decisao_1.justificativa_analise_previa,
        "n1_lauda_evidencia": decisao_1.pagina_evidencia,
        "n1_frase_citada": decisao_1.frase_justificativa,
        "n1_tipo_citacao": str(
            decisao_1.tipo_citacao.value if hasattr(decisao_1.tipo_citacao, "value") else decisao_1.tipo_citacao
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
        "n2_cot_justificativa": (decisao_2.justificativa_analise_previa if decisao_2 else None),
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


def gerar_sumario_lote(resultados: list[dict[str, Any]]) -> dict[str, Any]:
    """Consolida as métricas operacionais e financeiras da execução do lote.

    :param resultados: Lista de dicionários de predição do lote.
    :returns: Sumário operacional e financeiro consolidado.
    """
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
        "detalhes": resultados,
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
