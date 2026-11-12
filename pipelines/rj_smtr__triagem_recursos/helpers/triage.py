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

import os
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any, Protocol

import polars as pl
from prefect.logging import get_logger

from .providers.base import BaseLLMClient

logger = get_logger("toolkit")

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
    StatusProcessamento,
)
from .cost_analyzer import calcular_custo_chamada
from .feature_extractor import extrair_sinais_epistemicos
from .guardrails import aplicar_guardrail_epistemico
from .meta_calibrator import predizer_probabilidade_calibrada
from .ocr_engine import extrair_ocr_pagina_especifica
from .router import (
    atualizar_probabilidade_bayesiana_log_odds,
    calcular_dispersao_cot,
    construir_prediction_set_aps,
)
from .storage import carregar_bytes_pdf
from .windowing import extrair_smart_window_pdf


_LLM_CLIENT_LOCK = threading.Lock()
_LLM_CLIENT_CACHE: dict[tuple[str, str, str], BaseLLMClient] = {}


def limpar_cache_cliente_llm() -> None:
    """Limpa e fecha instâncias em cache de clientes LLM (uso em testes/shutdown)."""
    with _LLM_CLIENT_LOCK:
        clientes = list(_LLM_CLIENT_CACHE.values())
        _LLM_CLIENT_CACHE.clear()
    for cliente in clientes:
        close_fn = getattr(cliente, "close", None)
        if callable(close_fn):
            close_fn()


def obter_cliente_llm(modelo: str | None = None) -> BaseLLMClient:
    """Return a thread-safe cached LLM client reusing persistent HTTP connections."""
    from ..constants import LLM_PROVIDER

    p = (LLM_PROVIDER or "openai").strip().lower()
    target_model = modelo or os.getenv("LLM_MODEL") or VERTEX_LLM_MODEL
    base_url = (os.getenv("BIFROST_BASE_URL") or "").strip() if p not in ("google", "vertex", "gemini") else "google"
    chave = (p, target_model, base_url)

    with _LLM_CLIENT_LOCK:
        cached = _LLM_CLIENT_CACHE.get(chave)
        if cached is not None:
            return cached

        if p in ("google", "vertex", "gemini"):
            from .providers.google_client import GoogleLLMClient

            cliente: BaseLLMClient = GoogleLLMClient(default_model=target_model)
        else:
            from .providers.openai_client import OpenAILLMClient

            cliente = OpenAILLMClient(default_model=target_model)

        _LLM_CLIENT_CACHE[chave] = cliente
        return cliente


def _montar_resultado_erro_arquivo(
    processo_id: str,
    id_doc_sei: Any,
    numero_sei: str,
    calibracao_id: Any,
    modelo_llm: str,
    versao_prompt: str,
    exc: Exception,
    t_inicio: float,
) -> dict[str, Any]:
    """Constrói payload canônico HITL_ERRO_ARQUIVO para falhas de obtenção/leitura de PDF."""
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


def preparar_documento_processo(
    processo: dict[str, Any],
    artefatos: dict[str, Any],
    fonte_pdf: str = "sei",
    cliente_sei: Any | None = None,
    max_paginas: int = MAX_PAGINAS_PDF,
    modelo_llm: str = VERTEX_LLM_MODEL,
) -> dict[str, Any]:
    """Executa o estágio 1: download em RAM e recorte Smart Window do PDF."""
    id_procedimento_sei = processo.get("id_procedimento_sei")
    id_doc_sei = processo.get("id_documento_sei")
    processo_id = str(processo.get("processo_id", ""))
    numero_sei = str(processo.get("numero_processo_sei", "SEI_SEM_NUMERO"))
    calibracao_id = artefatos.get("calibracao_id")
    versao_prompt = artefatos.get("versao_prompt", "v4_baseline")

    t_inicio = time.time()
    logger.info(
        "Iniciando triagem do processo SEI: %s (Fonte: %s)",
        numero_sei,
        fonte_pdf,
    )

    t0_download = time.perf_counter()
    try:
        if fonte_pdf == "sei" and cliente_sei is not None:
            if hasattr(cliente_sei, "obter_pdf_recurso_processo") and id_procedimento_sei:
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

        t_download = time.perf_counter() - t0_download
        t0_window = time.perf_counter()
        pdf_window_bytes = extrair_smart_window_pdf(pdf_bytes, max_paginas=max_paginas)
        t_window = time.perf_counter() - t0_window
    except Exception as exc:
        t_download = time.perf_counter() - t0_download
        logger.error(
            "Falha na obtenção/validação do PDF para o processo %s (%.2fs): %s",
            numero_sei,
            t_download,
            exc,
        )
        return {
            "ok": False,
            "processo": processo,
            "resultado_erro": _montar_resultado_erro_arquivo(
                processo_id=processo_id,
                id_doc_sei=id_doc_sei,
                numero_sei=numero_sei,
                calibracao_id=calibracao_id,
                modelo_llm=modelo_llm,
                versao_prompt=versao_prompt,
                exc=exc,
                t_inicio=t_inicio,
            ),
        }

    return {
        "ok": True,
        "processo": processo,
        "id_documento_sei": id_doc_sei,
        "pdf_bytes": pdf_bytes,
        "pdf_window_bytes": pdf_window_bytes,
        "t_download": t_download,
        "t_window": t_window,
        "t_inicio": t_inicio,
    }


def classificar_documento_preparado(
    documento_preparado: dict[str, Any],
    artefatos: dict[str, Any],
    limiar_alta: float = LIMIAR_ALTA,
    limiar_baixa: float = LIMIAR_BAIXA,
    limiar_dispersao_max: float = LIMIAR_DISPERSAO_COT_MAX,
    modelo_llm: str = VERTEX_LLM_MODEL,
    reasoning_effort: str = "medium",
    executar_n2: bool = True,
) -> dict[str, Any]:
    """Executa o estágio 2: inferência multimodal, OCR, calibração e roteamento N=2."""
    if not documento_preparado.get("ok"):
        return dict(documento_preparado["resultado_erro"])

    processo: dict[str, Any] = documento_preparado["processo"]
    id_doc_sei = documento_preparado.get("id_documento_sei")
    pdf_bytes: bytes = documento_preparado["pdf_bytes"]
    pdf_window_bytes: bytes = documento_preparado["pdf_window_bytes"]
    t_download = float(documento_preparado.get("t_download", 0.0))
    t_window = float(documento_preparado.get("t_window", 0.0))
    t_inicio = float(documento_preparado.get("t_inicio", time.time()))

    nome_arquivo = processo.get("nome_arquivo_origem") or f"{processo.get('numero_processo_sei', 'doc')}.pdf"
    processo_id = str(processo.get("processo_id", ""))
    numero_sei = str(processo.get("numero_processo_sei", "SEI_SEM_NUMERO"))

    calibrador = artefatos["calibrador"]
    conformal = artefatos["conformal"]
    prompt_sistema = artefatos["prompt_sistema"]
    calibracao_id = artefatos.get("calibracao_id")
    versao_prompt = artefatos.get("versao_prompt", "v4_baseline")
    stats_classes = getattr(calibrador, "estatisticas_classes", None)

    cliente_llm = obter_cliente_llm(modelo=modelo_llm)

    # 2. Inferência Primária N=1 (T=0.0)
    t0_gemini_n1 = time.perf_counter()
    decisao_1, usage_1 = cliente_llm.classificar(
        pdf_bytes=pdf_window_bytes,
        prompt_sistema=prompt_sistema,
        modelo=modelo_llm,
        temperature=0.0,
        reasoning_effort=reasoning_effort,
    )
    t_gemini_n1 = time.perf_counter() - t0_gemini_n1
    if decisao_1 is None:
        raise ValueError(f"A LLM retornou decisão nula para o processo {numero_sei}")

    decisao_1 = aplicar_guardrail_epistemico(decisao_1)
    hipotese_1_str = str(
        decisao_1.hipotese_legal.value if hasattr(decisao_1.hipotese_legal, "value") else decisao_1.hipotese_legal
    )

    # 3. Lazy OCR com tolerância de lauda adjacente
    t0_ocr = time.perf_counter()
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
    t_ocr = time.perf_counter() - t0_ocr

    # 4. Extração das 8 features ativas
    t0_signals = time.perf_counter()
    sinais_1 = extrair_sinais_epistemicos(
        decisao=decisao_1,
        texto_ocr_pagina=texto_ocr,
        tesseract_mean_conf=ocr_conf,
        estatisticas_classes=stats_classes,
    )
    t_signals = time.perf_counter() - t0_signals

    # 5. Meta-calibração tabular supervisionada
    t0_calib = time.perf_counter()
    p_cal_1 = predizer_probabilidade_calibrada(sinais_1, calibrador)
    set_conformal = construir_prediction_set_aps(
        decisao=decisao_1,
        p_cal=p_cal_1,
        estatisticas_classes=stats_classes,
        alpha=getattr(conformal, "alpha", 0.05),
    )
    t_calib = time.perf_counter() - t0_calib

    # 6. Avaliação do roteamento adaptativo
    if p_cal_1 >= limiar_alta and len(set_conformal) <= 1:
        rota_inicial = RotaExecucao.N1_DIRETO.value
    elif p_cal_1 < limiar_baixa or len(set_conformal) == 0:
        rota_inicial = RotaExecucao.HITL_BAIXA_CONFIANCA.value
    else:
        rota_inicial = "CANDIDATO_N2"

    estagio2_executado = False
    delta_cot = None
    log_odds_atualizado = None
    p_final = p_cal_1
    decisao_2 = None
    usage_2 = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0}
    t_gemini_n2 = 0.0

    # 7. Estágio N=2 Condicional (T=0.7)
    if rota_inicial == "CANDIDATO_N2":
        if executar_n2:
            logger.info(
                "Incerteza operacional em %s (p1=%.3f). Disparando N=2 (T=0.7)...",
                numero_sei,
                p_cal_1,
            )
            t0_gemini_n2 = time.perf_counter()
            decisao_2, usage_2 = cliente_llm.classificar(
                pdf_bytes=pdf_window_bytes,
                prompt_sistema=prompt_sistema,
                modelo=modelo_llm,
                temperature=0.7,
                reasoning_effort=reasoning_effort,
            )
            t_gemini_n2 = time.perf_counter() - t0_gemini_n2
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
                if hip_1 == hip_2 and (delta_cot or 0.0) <= limiar_dispersao_max:
                    rota_final = RotaExecucao.N2_CONSENSO.value
                else:
                    rota_final = RotaExecucao.HITL_DIVERGENCIA_DUPLA.value

            log_odds_atualizado = p_final
            if rota_final == RotaExecucao.HITL_DIVERGENCIA_DUPLA.value:
                set_conformal = {hip_1, hip_2}
        else:
            # Sem N=2, NUNCA registrar falso consenso de segundo estagio
            rota_final = RotaExecucao.HITL_BAIXA_CONFIANCA.value
    else:
        rota_final = rota_inicial

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
    tempos_etapas = {
        "download_sei_s": round(t_download, 3),
        "janela_extracao_s": round(t_window, 3),
        "gemini_n1_s": round(t_gemini_n1, 3),
        "ocr_s": round(t_ocr, 3),
        "sinais_s": round(t_signals, 3),
        "calibracao_s": round(t_calib, 3),
        "gemini_n2_s": round(t_gemini_n2, 3),
        "tempo_total_s": tempo_total,
    }

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
        "ETAPAS [%s]: SEI Download=%.2fs | Window=%.2fs | Gemini N=1=%.2fs | OCR=%.2fs | Sinais=%.2fs | Calibração=%.2fs | N=2=%.2fs | TOTAL=%.2fs",
        numero_sei,
        t_download,
        t_window,
        t_gemini_n1,
        t_ocr,
        t_signals,
        t_calib,
        t_gemini_n2,
        tempo_total,
    )
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
        "tempos_etapas": tempos_etapas,
    }


def triar_processo(
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
    """Executa estágio 1 (download) e estágio 2 (classificação) em sequência."""
    _ = is_local
    documento = preparar_documento_processo(
        processo=processo,
        artefatos=artefatos,
        fonte_pdf=fonte_pdf,
        cliente_sei=cliente_sei,
        max_paginas=max_paginas,
        modelo_llm=modelo_llm,
    )
    return classificar_documento_preparado(
        documento_preparado=documento,
        artefatos=artefatos,
        limiar_alta=limiar_alta,
        limiar_baixa=limiar_baixa,
        limiar_dispersao_max=limiar_dispersao_max,
        modelo_llm=modelo_llm,
        reasoning_effort=reasoning_effort,
        executar_n2=executar_n2,
    )


class FutureProtocol(Protocol):
    """Contrato mínimo de futuros compatível com Prefect e ThreadPoolExecutor."""

    def result(self, timeout: float | None = None) -> Any:
        """Obtém o resultado do futuro."""
        ...


def _resolver_futuro_seguro(futuro: Any, etapa: str) -> Any | None:
    """Resolve futuro Prefect ou padrão registrando falha sem interromper o lote."""
    try:
        return futuro.result(raise_on_failure=False)
    except TypeError:
        try:
            return futuro.result()
        except Exception as exc:
            logger.error("Falha irrecuperável na etapa %s: %s", etapa, exc)
            return None
    except Exception as exc:
        logger.error("Falha irrecuperável na etapa %s: %s", etapa, exc)
        return None


def executar_lote_em_janelas(
    processos: list[dict[str, Any]],
    submeter_download: Callable[[dict[str, Any]], FutureProtocol],
    submeter_classificacao: Callable[[dict[str, Any]], FutureProtocol],
    aguardar_concluidos: Callable[[list[Any]], Iterable[Any]],
    max_downloads_simultaneos: int = 4,
    aquecer_primeiro: bool = True,
) -> list[dict[str, Any]]:
    """Orquestra downloads SEI limitados em paralelo com inferências LLM sem ociosidade."""
    if not processos:
        return []

    pendentes: deque[dict[str, Any]] = deque(processos)
    resultados: list[dict[str, Any]] = []

    # Aquece o cache de prompt com o primeiro processo válido antes da rajada concorrente.
    if aquecer_primeiro and len(pendentes) > 1:
        while pendentes:
            proc_warmup = pendentes.popleft()
            doc_warmup = _resolver_futuro_seguro(submeter_download(proc_warmup), "warmup_download")
            if isinstance(doc_warmup, dict) and doc_warmup.get("ok"):
                res_warmup = _resolver_futuro_seguro(submeter_classificacao(doc_warmup), "warmup_classificacao")
                if isinstance(res_warmup, dict) and "processo_id" in res_warmup:
                    resultados.append(res_warmup)
                break
            if isinstance(doc_warmup, dict) and "resultado_erro" in doc_warmup:
                erro_warmup = doc_warmup["resultado_erro"]
                if isinstance(erro_warmup, dict) and "processo_id" in erro_warmup:
                    resultados.append(erro_warmup)

    limite_janela = max(1, max_downloads_simultaneos)
    downloads_ativos: list[Any] = []
    inferencias_ativas: list[Any] = []

    while pendentes and len(downloads_ativos) < limite_janela:
        downloads_ativos.append(submeter_download(pendentes.popleft()))

    while downloads_ativos:
        done_dl = next(iter(aguardar_concluidos(list(downloads_ativos))), None)
        if done_dl is None:
            break
        downloads_ativos.remove(done_dl)

        if pendentes:
            downloads_ativos.append(submeter_download(pendentes.popleft()))

        doc_prep = _resolver_futuro_seguro(done_dl, "download")
        if isinstance(doc_prep, dict) and doc_prep.get("ok"):
            inferencias_ativas.append(submeter_classificacao(doc_prep))
        elif isinstance(doc_prep, dict) and "resultado_erro" in doc_prep:
            erro_doc = doc_prep["resultado_erro"]
            if isinstance(erro_doc, dict) and "processo_id" in erro_doc:
                resultados.append(erro_doc)

    if inferencias_ativas:
        for done_inf in aguardar_concluidos(list(inferencias_ativas)):
            res_inf = _resolver_futuro_seguro(done_inf, "classificacao")
            if isinstance(res_inf, dict) and "processo_id" in res_inf:
                resultados.append(res_inf)

    return resultados


def gerar_sumario_lote(resultados: list[dict[str, Any]]) -> dict[str, Any]:
    """Consolida as metricas operacionais e financeiras da execucao do lote."""
    total = len(resultados)
    if total == 0:
        return {}

    df = pl.DataFrame(resultados)
    colunas = set(df.columns)

    qtd_verde = (
        int((df["faixa_operacional"] == FaixaOperacional.VERDE.value).sum()) if "faixa_operacional" in colunas else 0
    )
    qtd_amarela = (
        int((df["faixa_operacional"] == FaixaOperacional.AMARELA.value).sum()) if "faixa_operacional" in colunas else 0
    )
    qtd_vermelha = (
        int((df["faixa_operacional"] == FaixaOperacional.VERMELHA.value).sum()) if "faixa_operacional" in colunas else 0
    )

    estagio2_sum = int(df["estagio2_executado"].sum()) if "estagio2_executado" in colunas else 0
    total_chamadas = estagio2_sum + total
    mult_chamadas = round(total_chamadas / total, 2)

    custo_total = round(float(df["custo_estimado_usd"].sum()), 4) if "custo_estimado_usd" in colunas else 0.0
    custo_sem_cache = (
        round(float(df["custo_sem_cache_usd"].sum()), 4) if "custo_sem_cache_usd" in colunas else custo_total
    )
    economia_cache = round(float(df["economia_cache_usd"].sum()), 4) if "economia_cache_usd" in colunas else 0.0

    total_tokens_in = int(df["tokens_prompt_total"].sum()) if "tokens_prompt_total" in colunas else 0
    total_tokens_out = int(df["tokens_completion"].sum()) if "tokens_completion" in colunas else 0
    total_tokens_cache = int(df["tokens_cached"].sum()) if "tokens_cached" in colunas else 0
    pct_cache = round((total_tokens_cache / total_tokens_in * 100), 1) if total_tokens_in > 0 else 0.0

    from datetime import timezone

    sumario = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
        "SUMÁRIO FINANCEIRO / CACHE: Tokens In: %d (Cached: %d | %.1f%%) | Tokens Out: %d "
        "| Custo Real: $%.4f USD | Economia Cache: $%.4f USD",
        total_tokens_in,
        total_tokens_cache,
        pct_cache,
        total_tokens_out,
        custo_total,
        economia_cache,
    )
    return sumario
