"""
Módulo de Roteamento de Inferência Sequencial Adaptativa e Controle de Incerteza.

Orquestra o pipeline em cascata com garantias formais de cobertura e risco:
- Estágio 1: Inferência primária determinística (N=1, T=0.0) e Lazy OCR na lauda citada.
- Estágio 2: Extração dos 12 sinais epistêmicos (Mahalanobis, k-NN Density, Masked Grounding).
- Estágio 3: Meta-calibração supervisionada GBDT (p_cal) e construção do Prediction Set APS / CCCP.
- Estágio 4: Bifurcação adaptativa:
    * Rota 1 (N1_DIRETO): p_cal >= 0.90, C(X) unitário e grounding verificado.
    * Rota 2 (HITL_BAIXA_CONFIANCA): p_cal < 0.70 ou anomalia estrutural.
    * Rota 3 (N2_CONSENSO / AUDITORIA): Incerteza moderada (0.70 <= p_cal < 0.90) com cálculo de
      Dispersão Semântica da CoT (Delta_CoT) para detecção de fragilidade argumentativa.
"""

from collections import Counter
from dataclasses import dataclass, field
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union, Tuple

import numpy as np

from .conformal import ArtefatosConforme
from .feature_extractor import (
    EstatisticasSemanticasClasses,
    VetorSinaisEpistemicos,
    extrair_sinais_epistemicos,
    gerar_embedding_texto,
)
from .guardrails import aplicar_guardrail_epistemico
from .meta_calibrator import ModeloCalibrador, predizer_probabilidade_calibrada
from .ocr_engine import extrair_ocr_pagina_especifica
from .schemas import EsquemaDecisao, HipoteseLegal
from .providers.base import BaseLLMClient

TODAS_HIPOTESES_LEGAIS = [
    "citran",
    "roubo_furto_clonagem",
    "erro_captura_imagem",
    "veiculo_especial",
    "nao_indicacao_condutor",
    "prescricao",
    "hipotese_esgotada",
]


@dataclass
class DecisaoProcessualFinal:
    """Estrutura consolidada contendo a decisão final e os metadados de auditoria."""
    decisao: EsquemaDecisao
    rota: str
    probabilidade_calibrada: float
    prediction_set_conformal: Set[str]
    total_chamadas: int
    entropia_semantica: float
    dispersao_cot: float
    sinais_epistemicos: VetorSinaisEpistemicos
    amostras_brutas: List[EsquemaDecisao] = field(default_factory=list)
    metadados_execucao: Dict[str, Any] = field(default_factory=dict)


def _normalizar_para_string(valor: Any) -> str:
    """Extrai a string canônica a partir de Enums ou strings brutas."""
    if valor is None:
        return ""
    if hasattr(valor, "value"):
        return str(valor.value).strip().lower()
    return str(valor).strip().lower()


def calcular_dispersao_cot(cot_1: str, cot_2: str) -> float:
    """Calcula a dispersão semântica angular Delta_CoT = 1 - cos(e1, e2) entre duas justificativas."""
    if not cot_1 or not cot_2:
        return 1.0

    e1 = gerar_embedding_texto(cot_1)
    e2 = gerar_embedding_texto(cot_2)

    norm_1 = float(np.linalg.norm(e1))
    norm_2 = float(np.linalg.norm(e2))

    if norm_1 == 0 or norm_2 == 0:
        return 1.0

    cos_sim = float(np.dot(e1, e2) / (norm_1 * norm_2))
    cos_sim = max(-1.0, min(1.0, cos_sim))

    return round(float(1.0 - cos_sim), 4)


def construir_prediction_set_aps(
    decisao: EsquemaDecisao,
    p_cal: float,
    estatisticas_classes: Optional[Union[EstatisticasSemanticasClasses, Dict[str, Any]]],
    alpha: float = 0.05,
    temperatura_softmax: float = 0.12,
    razao_minima_competitiva: float = 0.20,
) -> Set[str]:
    """
    Constrói o Prediction Set C(X) incluindo apenas classes que apresentem
    plausibilidade relativa mínima frente à classe predita (LAC Ratio).
    """
    classe_predita = _normalizar_para_string(decisao.hipotese_legal)

    # 1. Se p_cal >= 0.90, a predição tem suporte estatístico suficiente para ser unitária
    p_top = float(max(0.01, min(1.0, p_cal)))
    if p_top >= 0.90:
        return {classe_predita}

    # 2. Extração dos centróides
    centroides: Dict[str, np.ndarray] = {}
    if isinstance(estatisticas_classes, EstatisticasSemanticasClasses):
        centroides = estatisticas_classes.centroides
    elif isinstance(estatisticas_classes, dict):
        centroides = estatisticas_classes.get("centroides", estatisticas_classes)

    emb_cot = gerar_embedding_texto(decisao.justificativa_analise_previa)
    norm_e = float(np.linalg.norm(emb_cot))
    similaridades: Dict[str, float] = {}

    for hip in TODAS_HIPOTESES_LEGAIS:
        c_vec = centroides.get(hip)
        if c_vec is not None and norm_e > 0:
            norm_c = float(np.linalg.norm(c_vec))
            cos_sim = float(np.dot(emb_cot, c_vec)) / (norm_c * norm_e) if norm_c > 0 else 0.0
            similaridades[hip] = max(0.0, min(1.0, cos_sim))
        else:
            similaridades[hip] = 0.0

    # 3. Distribuição de massa residual entre classes competidoras
    massa_residual = max(0.0, 1.0 - p_top)
    classes_outras = [h for h in TODAS_HIPOTESES_LEGAIS if h != classe_predita]

    prediction_set: Set[str] = {classe_predita}

    if classes_outras and massa_residual > 0.05:
        exp_sims = np.array([np.exp(similaridades.get(h, 0.0) / temperatura_softmax) for h in classes_outras])
        soma_exp = float(np.sum(exp_sims))
        pesos_softmax = (exp_sims / soma_exp) if soma_exp > 0 else np.ones(len(classes_outras)) / len(classes_outras)

        for h, peso in zip(classes_outras, pesos_softmax):
            p_k = massa_residual * peso
            if p_k >= 0.10 and (p_k / p_top) >= razao_minima_competitiva:
                prediction_set.add(h)

    return prediction_set


def calcular_entropia_discreta(classes: List[str]) -> float:
    """Calcula a Entropia de Shannon no espaço discreto de predições amostradas."""
    if not classes:
        return 0.0

    total = len(classes)
    contagem = Counter(classes)
    entropia = 0.0

    for qtd in contagem.values():
        p = qtd / total
        if p > 0:
            entropia -= p * math.log2(p)

    return round(float(entropia), 4)

def atualizar_probabilidade_bayesiana_log_odds(
    p_1: float,
    hip_1: str,
    hip_2: str,
    delta_cot: float,
    beta_consenso: float = 1.35,
    beta_divergencia: float = 2.20,
) -> Tuple[float, str]:
    """Atualiza a probabilidade a priori p_1 com a evidência de N=2 no espaço logit."""
    p_clipped = max(1e-4, min(1.0 - 1e-4, p_1))
    logit_prior = math.log(p_clipped / (1.0 - p_clipped))

    if hip_1 == hip_2:
        # Consenso: ganho contínuo de log-odds (sempre positivo, sem fragilidade semântica)
        fator_qualidade = max(0.0, 1.0 - (delta_cot / 0.50))
        delta_l = beta_consenso * (0.50 + 0.50 * fator_qualidade)
        rota = "N2_CONSENSO"
    else:
        # Divergência de classe: penalidade severa
        delta_l = -beta_divergencia * (1.0 + delta_cot)
        rota = "HITL_DIVERGENCIA_DUPLA"

    logit_post = logit_prior + delta_l
    p_post = 1.0 / (1.0 + math.exp(-logit_post))

    return round(float(p_post), 4), rota


class RoteadorInferenciaAdaptativa:
    """Roteador adaptativo com validação conformal, meta-calibração GBDT e auditoria multi-estágio."""
    def __init__(
        self,
        modelo_calibrador: ModeloCalibrador,
        artefatos_conforme: ArtefatosConforme,
        limiar_alta_confianca: float = 0.85,
        limiar_baixa_confianca: float = 0.50,
        limiar_dispersao_cot_max: float = 0.50,
    ) -> None:
        self.calibrador = modelo_calibrador
        self.conformal = artefatos_conforme
        self.limiar_alta = limiar_alta_confianca
        self.limiar_baixa = limiar_baixa_confianca
        self.limiar_dispersao_max = limiar_dispersao_cot_max

    def avaliar_criterio_parada(
        self,
        p_cal: float,
        set_conformal: Set[str],
    ) -> str:
        """Determina a rota de execução inicial em N=1 com base na certeza estatística e conformal."""
        # Caso 1: Incerteza severa ou conjunto conformal vazio -> Revisão Humana Imediata
        if p_cal < self.limiar_baixa or len(set_conformal) == 0:
            return "HITL_BAIXA_CONFIANCA"

        # Caso 2: Alta certeza estatística e cobertura estritamente unitária -> Aprovação Direta
        if p_cal >= self.limiar_alta and len(set_conformal) <= 1:
            return "N1_DIRETO"

        # Caso 3: Incerteza moderada ou ambiguidade entre hipóteses -> Dispara Estágio N=2
        return "N2_CONSENSO"

    def processar_processo(
        self,
        pdf_bytes: bytes,
        pdf_path_original: Path,
        cliente_llm: BaseLLMClient,
        prompt_sistema: str,
        modelo_especifico: Optional[str] = None,
    ) -> DecisaoProcessualFinal:
        """Executa a esteira adaptativa de inferência com roteamento conformal e controle de dispersão."""
        amostras: List[EsquemaDecisao] = []

        # =========================================================================
        # ESTÁGIO 1: Inferência Primária Determinística (N=1, T=0.0)
        # =========================================================================
        decisao_1 = cliente_llm.classificar(pdf_bytes, prompt_sistema, modelo=modelo_especifico)
        decisao_1 = aplicar_guardrail_epistemico(decisao_1)
        amostras.append(decisao_1)

        # Lazy OCR restrito à página apontada
        if decisao_1.pagina_evidencia > 0:
            texto_ocr, ocr_conf = extrair_ocr_pagina_especifica(
                pdf_path=pdf_path_original,
                num_pagina=decisao_1.pagina_evidencia,
            )
        else:
            texto_ocr, ocr_conf = "", 0.0

        # Extração de Sinais Epistêmicos Expandidos (12 Variáveis)
        stats = getattr(self.calibrador, "estatisticas_classes", None)
        sinais_1 = extrair_sinais_epistemicos(
            decisao=decisao_1,
            texto_ocr_pagina=texto_ocr,
            tesseract_mean_conf=ocr_conf,
            estatisticas_classes=stats,
        )

        # Meta-Calibração Supervisionada (Probabilidade de Acerto)
        p_cal = predizer_probabilidade_calibrada(sinais_1, self.calibrador)

        # Construção do Prediction Set Conformal APS
        alpha_val = getattr(self.conformal, "alpha", 0.05)
        set_conformal = construir_prediction_set_aps(
            decisao=decisao_1,
            p_cal=p_cal,
            estatisticas_classes=stats,
            alpha=alpha_val,
        )

        # Avaliação da Rota
        decisao_rota = self.avaliar_criterio_parada(
            p_cal=p_cal,
            set_conformal=set_conformal,
        )

        # Rota 1: Alta Confiança (N=1 Direto)
        if decisao_rota == "N1_DIRETO":
            return DecisaoProcessualFinal(
                decisao=decisao_1,
                rota="N1_DIRETO",
                probabilidade_calibrada=p_cal,
                prediction_set_conformal=set_conformal,
                total_chamadas=1,
                entropia_semantica=0.0,
                dispersao_cot=0.0,
                sinais_epistemicos=sinais_1,
                amostras_brutas=amostras,
                metadados_execucao={"status_triagem": "AUTOMATIZADO_ALTA_CONFIANCA"}
            )

        # Rota 2: Incerteza Crítica (HITL em N=1)
        if decisao_rota.startswith("HITL_"):
            return DecisaoProcessualFinal(
                decisao=decisao_1,
                rota=decisao_rota,
                probabilidade_calibrada=p_cal,
                prediction_set_conformal=set_conformal,
                total_chamadas=1,
                entropia_semantica=0.0,
                dispersao_cot=0.0,
                sinais_epistemicos=sinais_1,
                amostras_brutas=amostras,
                metadados_execucao={"status_triagem": "AUDITORIA_INSEGURANCA_EXTREMA"}
            )

        # =========================================================================
        # ESTÁGIO 2: Chamada de Validação / Auditoria (N=2)
        # =========================================================================
        decisao_2 = cliente_llm.classificar(pdf_bytes, prompt_sistema, modelo=modelo_especifico)
        decisao_2 = aplicar_guardrail_epistemico(decisao_2)
        amostras.append(decisao_2)

        hip_1 = _normalizar_para_string(decisao_1.hipotese_legal)
        hip_2 = _normalizar_para_string(decisao_2.hipotese_legal)

        # Cálculo da Dispersão Semântica do Raciocínio (Delta_CoT)
        delta_cot = calcular_dispersao_cot(
            decisao_1.justificativa_analise_previa,
            decisao_2.justificativa_analise_previa
        )

        # Atualização Bayesiana de Log-Odds (sem fragilidade semântica)
        p_post, rota_final = atualizar_probabilidade_bayesiana_log_odds(
            p_1=p_cal,
            hip_1=hip_1,
            hip_2=hip_2,
            delta_cot=delta_cot,
        )

        if rota_final == "N2_CONSENSO":
            return DecisaoProcessualFinal(
                decisao=decisao_1,
                rota="N2_CONSENSO",
                probabilidade_calibrada=p_post,
                prediction_set_conformal={hip_1},
                total_chamadas=2,
                entropia_semantica=0.0,
                dispersao_cot=delta_cot,
                sinais_epistemicos=sinais_1,
                amostras_brutas=amostras,
                metadados_execucao={"status_triagem": "VALIDADO_POR_CONSENSO_N2"}
            )
        else:
            # Divergência de Hipótese (N1 != N2)
            entropia = calcular_entropia_discreta([hip_1, hip_2])
            return DecisaoProcessualFinal(
                decisao=decisao_1,
                rota="HITL_DIVERGENCIA_DUPLA",
                probabilidade_calibrada=p_post,
                prediction_set_conformal={hip_1, hip_2},
                total_chamadas=2,
                entropia_semantica=entropia,
                dispersao_cot=delta_cot,
                sinais_epistemicos=sinais_1,
                amostras_brutas=amostras,
                metadados_execucao={
                    "status_triagem": "AUDITORIA_DIVERGENCIA_DUPLA",
                    "hipotese_opcao_1": hip_1,
                    "hipotese_opcao_2": hip_2,
                }
            )