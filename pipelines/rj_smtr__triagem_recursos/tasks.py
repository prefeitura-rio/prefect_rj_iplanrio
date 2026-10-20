"""Tarefas atômicas (Tasks) da pipeline SMTR no Prefect 3.

Suporta execução local e nuvem com instrumentação completa de logs.
"""

from datetime import datetime
from pathlib import Path
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
    LOCAL_ARTIFACTS_DIR,
    MAX_PAGINAS_PDF,
    PROMPTS_DIR,
    RotaExecucao,
    VERTEX_LLM_MODEL,
    resolver_banco_url,
)
from .utils.conformal import carregar_artefatos_conforme
from .utils.cost_analyzer import calcular_custo_chamada
from .utils.db_repository import (
    buscar_processos_para_triagem,
    persistir_lote_resultados_ia,
)
from .utils.feature_extractor import extrair_sinais_epistemicos
from .utils.guardrails import aplicar_guardrail_epistemico
from .utils.meta_calibrator import (
    carregar_calibrador,
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
    is_local: bool = True, arquivo_prompt: str = "v3_baseline.txt"
) -> Dict[str, Any]:
  logger = get_run_logger()
  caminho_calibrador = (
      LOCAL_ARTIFACTS_DIR / "meta_calibrator_vertex_gemini_3.7_flash.joblib"
  )
  caminho_conformal = (
      LOCAL_ARTIFACTS_DIR / "conformal_artifacts_vertex_gemini_3.7_flash.joblib"
  )
  caminho_prompt = PROMPTS_DIR / arquivo_prompt

  logger.info(
      "Carregando artefatos de calibração | Local=%s | Calibrador: %s",
      is_local,
      caminho_calibrador.name,
  )

  calibrador = carregar_calibrador(caminho_calibrador)
  conformal = carregar_artefatos_conforme(caminho_conformal)
  prompt_sistema = caminho_prompt.read_text(encoding="utf-8")

  return {
      "calibrador": calibrador,
      "conformal": conformal,
      "prompt_sistema": prompt_sistema,
  }


@task(name="buscar_processos_pendentes", retries=2, retry_delay_seconds=5)
def task_buscar_processos_pendentes(
    is_local: bool = True,
    modo: str = "INCREMENTAL",
    limite: int = 300,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
) -> List[Dict[str, Any]]:
  logger = get_run_logger()
  db_url = resolver_banco_url(is_local)
  logger.info(
      "Buscando processos na fila do banco | Modo: %s | Limite: %s",
      modo,
      limite,
  )

  processos = buscar_processos_para_triagem(
      db_url=db_url,
      modo=modo,
      limite=limite,
      data_inicio=data_inicio,
      data_fim=data_fim,
  )
  logger.info("%d processo(s) elegível(is) encontrado(s).", len(processos))
  return processos


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
    limiar_alta: float = LIMIAR_ALTA,
    limiar_baixa: float = LIMIAR_BAIXA,
    limiar_dispersao_max: float = LIMIAR_DISPERSAO_COT_MAX,
    max_paginas: int = MAX_PAGINAS_PDF,
    modelo_llm: str = VERTEX_LLM_MODEL,
) -> Dict[str, Any]:
  logger = get_run_logger()
  nome_arquivo = processo["nome_arquivo_origem"]
  processo_id = str(processo["processo_id"])
  numero_sei = str(processo["numero_processo_sei"])

  t_inicio = time.time()
  logger.info("Iniciando triagem do processo SEI: %s (%s)", numero_sei, nome_arquivo)

  calibrador = artefatos["calibrador"]
  conformal = artefatos["conformal"]
  prompt_sistema = artefatos["prompt_sistema"]
  stats_classes = getattr(calibrador, "estatisticas_classes", None)

  cliente_llm = OpenAILLMClient(default_model=modelo_llm)

  # 1. Carregamento e fatiamento do PDF
  logger.info("[1/7] Lendo bytes do PDF '%s' (is_local=%s)...", nome_arquivo, is_local)
  pdf_bytes = carregar_bytes_pdf(nome_arquivo, is_local=is_local)
  logger.info("PDF lido com sucesso (%d bytes). Aplicando janela de até %d páginas...", len(pdf_bytes), max_paginas)

  pdf_window_bytes = extrair_smart_window_pdf(pdf_bytes, max_paginas=max_paginas)
  logger.info("Janela gerada (%d bytes). Enviando para inferência N=1...", len(pdf_window_bytes))

  # 2. Chamada N=1 (T=0.0)
  logger.info("[2/7] Chamando LLM (N=1, T=0.0, modelo=%s)...", modelo_llm)
  decisao_1 = cliente_llm.classificar(
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
  logger.info(
      "N=1 concluído: Hipótese=%s | Lauda=%s | Encaminhamento=%s",
      hipotese_1_str,
      decisao_1.pagina_evidencia,
      decisao_1.encaminhamento,
  )

  # 3. Lazy OCR na lauda de prova
  texto_ocr, ocr_conf = "", 0.0
  if decisao_1.pagina_evidencia and decisao_1.pagina_evidencia > 0:
    logger.info("[3/7] Executando OCR na lauda %s...", decisao_1.pagina_evidencia)
    resultado_ocr = extrair_ocr_pagina_especifica(
        pdf_origem=pdf_bytes,
        num_pagina=decisao_1.pagina_evidencia,
        dpi=150,
        idioma="por",
    )
    if resultado_ocr and isinstance(resultado_ocr, tuple) and len(resultado_ocr) == 2:
      texto_ocr, ocr_conf = resultado_ocr
    else:
      logger.warning("Retorno inválido do OCR (%s). Assumindo fallback vazio.", type(resultado_ocr))
      texto_ocr, ocr_conf = "", 0.0
    logger.info("OCR concluído (confiança=%.2f, caracteres=%d)", ocr_conf, len(texto_ocr))
  else:
    logger.info("[3/7] Sem lauda de evidência apontada. Pulando OCR.")

  # 4. Extração das 8 features ativas
  logger.info("[4/7] Extraindo as 8 features epistêmicas...")
  sinais_1 = extrair_sinais_epistemicos(
      decisao=decisao_1,
      texto_ocr_pagina=texto_ocr,
      tesseract_mean_conf=ocr_conf,
      estatisticas_classes=stats_classes,
  )
  logger.info("Features extraídas: %s", sinais_1.to_dict())

  # 5. Meta-calibração tabular via LightGBM
  logger.info("[5/7] Calculando probabilidade calibrada via LightGBM...")
  p_cal_1 = predizer_probabilidade_calibrada(sinais_1, calibrador)
  set_conformal = construir_prediction_set_aps(
      decisao=decisao_1,
      p_cal=p_cal_1,
      estatisticas_classes=stats_classes,
      alpha=conformal.alpha,
  )
  logger.info("Resultado N=1: p_cal_1=%.4f | Conjunto Conformal=%s", p_cal_1, set_conformal)

  # 6. Avaliação do Chaveamento Adaptativo
  if p_cal_1 >= limiar_alta and len(set_conformal) <= 1:
    rota_inicial = RotaExecucao.N1_DIRETO.value
  elif p_cal_1 < limiar_baixa or len(set_conformal) == 0:
    rota_inicial = RotaExecucao.HITL_BAIXA_CONFIANCA.value
  else:
    rota_inicial = RotaExecucao.N2_CONSENSO.value

  logger.info("[6/7] Rota inicial determinada: %s", rota_inicial)

  total_chamadas = 1
  estagio2_executado = False
  delta_cot = None
  log_odds_atualizado = None
  p_final = p_cal_1
  rota_final = rota_inicial
  decisao_2 = None

  # 7. Estágio N=2 Condicional (T=0.7)
  if rota_inicial not in [
      RotaExecucao.N1_DIRETO.value,
      RotaExecucao.HITL_BAIXA_CONFIANCA.value,
  ]:
    logger.info(
        "[7/7] Incerteza operacional (p1=%.3f). Disparando N=2 (T=0.7)...",
        p_cal_1,
    )
    decisao_2 = cliente_llm.classificar(
        pdf_bytes=pdf_window_bytes,
        prompt_sistema=prompt_sistema,
        modelo=modelo_llm,
        temperature=0.7,
    )
    decisao_2 = aplicar_guardrail_epistemico(decisao_2)
    total_chamadas = 2
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
      logger.warning("Retorno inesperado da atualização bayesiana: %s", resultado_bayes)
      p_final = p_cal_1

    log_odds_atualizado = p_final
    if rota_final == RotaExecucao.HITL_DIVERGENCIA_DUPLA.value:
      set_conformal = {hip_1, hip_2}

    logger.info("N=2 concluído: Hip2=%s | DeltaCoT=%.3f | p_final=%.4f | Rota=%s", hip_2, delta_cot, p_final, rota_final)
  else:
    logger.info("[7/7] N=2 dispensado. Prosseguindo com probabilidade de N=1.")

  # 8. Faixa Operacional
  if p_final >= limiar_alta:
    faixa = FaixaOperacional.VERDE.value
  elif p_final >= 0.50:
    faixa = FaixaOperacional.AMARELA.value
  else:
    faixa = FaixaOperacional.VERMELHA.value

  tempo_total = round(time.time() - t_inicio, 2)
  info_custo = calcular_custo_chamada(
      modelo=modelo_llm,
      prompt_tokens=3500 * total_chamadas,
      completion_tokens=150 * total_chamadas,
      aplicar_cache_estatico=True,
  )

  logger.info(
      "Triagem de %s finalizada em %.2fs | Faixa: %s | p_final: %.4f",
      numero_sei,
      tempo_total,
      faixa,
      p_final,
  )

  return {
      "processo_id": processo_id,
      "numero_processo_sei": numero_sei,
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
      "custo_estimado_usd": info_custo["custo_total_usd"],
      "tempo_execucao_s": tempo_total,
  }


@task(name="persistir_lote_resultados", retries=2, retry_delay_seconds=5)
def task_persistir_lote_resultados(
    resultados: List[Dict[str, Any]], is_local: bool = True
) -> None:
  logger = get_run_logger()
  db_url = resolver_banco_url(is_local)
  logger.info(
      "Gravando %d resultado(s) no banco de dados...",
      len(resultados),
  )
  persistir_lote_resultados_ia(db_url, resultados)
  logger.info("Persistência concluída com sucesso.")


@task(name="gerar_sumario_lote")
def task_gerar_sumario_lote(resultados: List[Dict[str, Any]]) -> Dict[str, Any]:
  logger = get_run_logger()
  total = len(resultados)
  if total == 0:
    return {}

  df = pd.DataFrame(resultados)
  qtd_verde = int(
      (df["faixa_operacional"] == FaixaOperacional.VERDE.value).sum()
  )
  qtd_amarela = int(
      (df["faixa_operacional"] == FaixaOperacional.AMARELA.value).sum()
  )
  qtd_vermelha = int(
      (df["faixa_operacional"] == FaixaOperacional.VERMELHA.value).sum()
  )

  total_chamadas = int(df["estagio2_executado"].sum()) + total
  mult_chamadas = round(total_chamadas / total, 2)
  custo_total = round(float(df["custo_estimado_usd"].sum()), 4)

  sumario = {
      "timestamp": datetime.now().isoformat(),
      "total": total,
      "faixa_verde": qtd_verde,
      "faixa_amarela": qtd_amarela,
      "faixa_vermelha": qtd_vermelha,
      "multiplicador_chamadas": mult_chamadas,
      "custo_usd": custo_total,
  }

  logger.info("=" * 70)
  logger.info("SUMÁRIO DA RODADA:")
  logger.info(
      "Total: %d | Verde: %d | Amarela: %d | Vermelha: %d",
      total,
      qtd_verde,
      qtd_amarela,
      qtd_vermelha,
  )
  logger.info(
      "Multiplicador: %.2fx | Custo Total: $%.4f USD",
      mult_chamadas,
      custo_total,
  )
  logger.info("=" * 70)
  return sumario