"""
Módulo de Guardrails Determinísticos e Calibração de Confiança Proxy.

Implementa a validação factual de citações (fuzzy string matching), salvaguardas
epistêmicas contra alegações genéricas/hipotéticas e a computação matemática do score
de confiança heurístico sem dependência de logprobs brutas da API.
"""

from difflib import SequenceMatcher
from typing import Union
from .schemas import Encaminhamento, EsquemaDecisao, HipoteseLegal, TipoCitacao

def verificar_grounding_fuzzy(
    texto_pagina: str, 
    frase_citada: str, 
    limiar_similaridade: float = 0.80
) -> bool:
    """Verifica se a citação literal retornada pelo modelo existe no texto lido via OCR.

    Args:
        texto_pagina: Texto extraído da lauda física via OCR.
        frase_citada: String ipsis litteris reportada pelo modelo.
        limiar_similaridade: Razão mínima de similaridade difusa (padrão: 0.80).

    Returns:
        bool: True se a citação for confirmada no documento; False caso contrário.
    """
    if not frase_citada or len(frase_citada.strip()) < 4:
        return False

    if not texto_pagina or len(texto_pagina.strip()) < 10:
        return False

    frase_norm = " ".join(frase_citada.lower().split())
    texto_norm = " ".join(texto_pagina.lower().split())

    if frase_norm in texto_norm:
        return True

    tamanho_janela = len(frase_norm)
    passo = max(1, tamanho_janela // 4)

    for i in range(0, len(texto_norm) - tamanho_janela + 1, passo):
        trecho_candidato = texto_norm[i:i + tamanho_janela]
        similaridade = SequenceMatcher(None, frase_norm, trecho_candidato).ratio()
        if similaridade >= limiar_similaridade:
            return True

    return False


def aplicar_guardrail_epistemico(decisao: EsquemaDecisao) -> EsquemaDecisao:
    """Força reclassificação para default residual se a citação for hipotética ou sem suporte fático.

    Garante integridade de tipos com Enums do Pydantic e blindagem contra campos NoneType.
    """
    if decisao is None:
        return decisao

    classes_positivas = {
        "roubo_furto_clonagem",
        "erro_captura_imagem",
        "veiculo_especial",
        "nao_indicacao_condutor",
        "citran",
        "prescricao"
    }

    # Sanitização defensiva de atributos de texto contra NoneType
    if getattr(decisao, "justificativa_analise_previa", None) is None:
        decisao.justificativa_analise_previa = ""
    if getattr(decisao, "frase_justificativa", None) is None:
        decisao.frase_justificativa = ""

    # Sanitização do sub-esquema de auditoria de sistema
    aud = getattr(decisao, "auditoria_sistema", None)
    if aud is not None:
        if getattr(aud, "prefixo_auto", None) is None:
            aud.prefixo_auto = "NAO_IDENTIFICADO"
        if getattr(aud, "fase_descricao", None) is None:
            aud.fase_descricao = "NAO_IDENTIFICADO"
        if getattr(aud, "data_fato_infracao", None) is None:
            aud.data_fato_infracao = "NAO_IDENTIFICADO"
        if getattr(aud, "data_expedicao_penalidade", None) is None:
            aud.data_expedicao_penalidade = "NAO_IDENTIFICADO"

    hipotese_raw = getattr(decisao, "hipotese_legal", "")
    hipotese_str = (
        hipotese_raw.value 
        if hasattr(hipotese_raw, "value") 
        else str(hipotese_raw or "")
    ).strip().lower()

    tipo_cit_raw = getattr(decisao, "tipo_citacao", "")
    tipo_cit_str = (
        tipo_cit_raw.value 
        if hasattr(tipo_cit_raw, "value") 
        else str(tipo_cit_raw or "")
    ).strip().upper()

    # 1. Normalização de página em hipótese esgotada
    if hipotese_str == "hipotese_esgotada":
        decisao.pagina_evidencia = 0

    # 2. Bloqueio Epistêmico: hipótese material positiva com citação puramente hipotética ou inexistente
    if hipotese_str in classes_positivas:
        if tipo_cit_str in ["ALEGACAO_HIPOTETICA", "NENHUMA_MENCAO", ""]:
            decisao.hipotese_legal = HipoteseLegal.HIPOTESE_ESGOTADA
            decisao.encaminhamento = Encaminhamento.PUBLICAR_INTEMPESTIVO
            decisao.pagina_evidencia = 0
            decisao.frase_justificativa = ""
            decisao.justificativa_analise_previa += (
                " [Bloqueio Epistêmico: Hipótese material desprovida de declaração fática afirmativa]."
            )

    return decisao


def calcular_confianca_proxy(
    grounding_valido: bool,
    tipo_citacao: Union[TipoCitacao, str],
    is_manuscrito: bool,
    hipotese_legal: Union[HipoteseLegal, str],
    justificativa_analise: str = "",
    possui_espelho_valido: bool = True
) -> float:
    """Calcula o score numérico de certeza [0.0 a 1.0] cruzando modalidade, grounding e integridade.

    Args:
        grounding_valido: Se a citação foi confirmada via OCR/texto.
        tipo_citacao: Modalidade identificada do trecho citado.
        is_manuscrito: Indicador de escrita manual (caligrafia).
        hipotese_legal: Classe final identificada.
        justificativa_analise: Texto do raciocínio analítico prévio.
        possui_espelho_valido: Se os dados de auditoria sistêmica são consistentes.

    Returns:
        float: Confiança heurística calibrada.
    """
    hip_str = (
        hipotese_legal.value 
        if hasattr(hipotese_legal, "value") 
        else str(hipotese_legal)
    ).lower()

    tipo_str = (
        tipo_citacao.value 
        if hasattr(tipo_citacao, "value") 
        else str(tipo_citacao)
    ).upper()

    # =========================================================================
    # 1. Hipótese Esgotada (Fase 3 - Default)
    # =========================================================================
    if hip_str == "hipotese_esgotada":
        if tipo_str in ["FATO_DECLARADO", "DADO_ESTRUTURADO"]:
            return 0.10  # Incoerência interna (recurso com fato material marcado como esgotado)
        
        if is_manuscrito:
            return 0.75  # Cartas manuscritas sem enquadramento
        
        if tipo_str == "ALEGACAO_HIPOTETICA":
            return 0.95  # Teses condicionais e prazos em dias descartados com alta certeza
        
        if len(justificativa_analise.strip()) >= 50:
            return 0.95  # CoT estruturado em 2 declarações completo
        
        return 0.70

    # =========================================================================
    # 2. Bloqueios e Incoerências em Hipóteses Positivas
    # =========================================================================
    if tipo_str in ["ALEGACAO_HIPOTETICA", "NENHUMA_MENCAO"]:
        return 0.00

    # =========================================================================
    # 3. Dados Estruturados (CITRAN / Multa NIC PJ / Prescrição de Sistema)
    # =========================================================================
    if tipo_str == "DADO_ESTRUTURADO":
        if not possui_espelho_valido:
            return 0.40
        # Dado estruturado com espelho válido não depende exclusivamente de OCR de texto corrido
        return 0.98 if grounding_valido else 0.88

    # =========================================================================
    # 4. Fatos Declarados em Documentos Manuscritos vs Tipografados
    # =========================================================================
    if is_manuscrito:
        # Manuscrito lido pela VLM tem alta confiabilidade visual mesmo com falha no OCR tradicional
        return 0.90 if grounding_valido else 0.82

    # Fato declarado em petição tipografada depende de grounding textual no OCR
    return 0.98 if grounding_valido else 0.20