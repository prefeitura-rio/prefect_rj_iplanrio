"""
NFST ↔ Fatura Cross-Page Merger

Pós-processamento que vincula NFSTs a Faturas de telecomunicações do mesmo ciclo
de faturamento, usando numero_conta + mes_referencia como chave de junção.

Contexto:
    Documentos de telecomunicações frequentemente chegam em PDFs com múltiplas
    páginas: uma página de Fatura/Demonstrativo (com "TOTAL A PAGAR") e uma ou
    mais páginas de NFST (com "TOTAL NOTA FISCAL [operadora]"). O modelo de
    extração vê cada página individualmente e retorna valor_total = null na NFST
    quando "TOTAL A PAGAR" não está presente na mesma página. Este módulo resolve
    esse null vinculando a NFST à Fatura correspondente (mesmo numero_conta).

Uso:
    extracted_nfs = merge_nfst_with_fatura(extracted_nfs)
"""

from __future__ import annotations

import math
import re

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)
# TODO(Trick): logger da iplanrio não exibe logs de nível INFO no Prefect
# (bug em investigação). Workaround temporário: usamos logger.warning()
# nos lugares que logicamente seriam logger.info() abaixo. Reverter para
# logger.info() quando o bug for corrigido.


# ---------------------------------------------------------------------------
# Helpers de normalização
# ---------------------------------------------------------------------------


def normalize_value(val: object) -> float:
    """
    Normaliza valor monetário para float.

    Trata o formato de moeda brasileiro, onde pontos são separadores de
    milhar e vírgulas são separadores decimais. Só trata . ou , como
    separador decimal se seguido de 1-2 dígitos no final.

    :param val: Valor monetário como string, int ou float.
    :returns: Valor float normalizado, ou 0.0 se não for possível parsear.
    """
    if val is None or (isinstance(val, float) and math.isnan(val)) or val in ("-", ""):
        return 0.0

    if isinstance(val, (int, float)):
        return float(val)

    val_str = str(val).replace("R$", "").replace(" ", "").strip()

    decimal_pattern = r"[.,](\d{1,2})$"
    match = re.search(decimal_pattern, val_str)

    if match:
        decimal_pos = match.start()
        integer_part = val_str[:decimal_pos]
        decimal_part = match.group(1)
        integer_part = integer_part.replace(".", "").replace(",", "")
        normalized = f"{integer_part}.{decimal_part}"
    else:
        normalized = val_str.replace(".", "").replace(",", "")

    try:
        return float(normalized)
    except Exception:
        return 0.0


def normalize_numero_conta(conta: str | None) -> str | None:
    """
    Normaliza número de conta removendo espaços, hífens e zeros à esquerda
    não significativos, para facilitar comparação.

    Exemplos:
        "0421736803"  → "421736803"
        "042173-6803" → "4217368 03" → ... → "421736803"
        None          → None
    """
    if not conta:
        return None
    # Remove tudo que não é dígito
    digits = re.sub(r"\D", "", conta)
    # Remove zeros à esquerda
    return digits.lstrip("0") or digits  # mantém "0" se só zeros


def normalize_mes_referencia(mes: str | None) -> str | None:
    """
    Normaliza mês de referência para o formato MM/YYYY.

    Aceita: "05/2024", "5/2024", "2024-05", "05-2024".
    Retorna None se não reconhecido.
    """
    if not mes:
        return None
    s = mes.strip()

    # Formato MM/YYYY ou M/YYYY
    m = re.match(r"^(\d{1,2})/(\d{4})$", s)
    if m:
        return f"{int(m.group(1)):02d}/{m.group(2)}"

    # Formato YYYY-MM ou YYYY/MM
    m = re.match(r"^(\d{4})[-/](\d{1,2})$", s)
    if m:
        return f"{int(m.group(2)):02d}/{m.group(1)}"

    # Formato MM-YYYY
    m = re.match(r"^(\d{1,2})-(\d{4})$", s)
    if m:
        return f"{int(m.group(1)):02d}/{m.group(2)}"

    return None


def mes_from_data_servico(data_servico: str | None) -> str | None:
    """
    Extrai o mês/ano de data_servico para usar como fallback quando
    mes_referencia não está disponível na NFST.

    Aceita: "25/04/2024 a 24/05/2024", "01/2024", "01/04/2024".
    Retorna: "MM/YYYY" do primeiro mês encontrado, ou None.
    """
    if not data_servico:
        return None
    # Procura padrão DD/MM/YYYY
    m = re.search(r"\d{2}/(\d{2}/\d{4})", data_servico)
    if m:
        return normalize_mes_referencia(m.group(1))
    # Procura padrão MM/YYYY
    m = re.search(r"(\d{1,2}/\d{4})", data_servico)
    if m:
        return normalize_mes_referencia(m.group(1))
    return None


# ---------------------------------------------------------------------------
# Lógica de match
# ---------------------------------------------------------------------------


def is_nfst(doc: dict) -> bool:
    tipo = (doc.get("tipo_documento") or "").strip().upper()
    return tipo == "NFST"


def is_fatura_telecom(doc: dict) -> bool:
    tipo = (doc.get("tipo_documento") or "").strip().lower()
    return tipo == "fatura" and bool(doc.get("numero_conta"))


def needs_merge(doc: dict) -> bool:
    """NFST cujo valor_total ainda não foi preenchido."""
    return is_nfst(doc) and doc.get("valor_total") is None


def find_fatura_for_nfst(
    nfst: dict,
    faturas: list[dict],
) -> tuple[dict | None, str | None]:
    """
    Encontra a Fatura de telecomunicações que corresponde a esta NFST.

    Estratégia de match (em ordem de prioridade):
    1. numero_conta exato (normalizado) + mes_referencia exato → match definitivo
    2. numero_conta exato + mes_referencia derivado de data_servico da NFST
    3. numero_conta exato sem desambiguação por mês (quando há apenas 1 candidata)

    Retorna:
        (fatura_dict, descricao_match) ou (None, None) se não encontrada.
    """
    nfst_conta = normalize_numero_conta(nfst.get("numero_conta"))
    if not nfst_conta:
        logger.warning(f"NFST pág.{nfst.get('pagina')} sem numero_conta — merge impossível")
        return None, None

    # Candidatas com mesmo numero_conta
    candidatas = [f for f in faturas if normalize_numero_conta(f.get("numero_conta")) == nfst_conta]

    if not candidatas:
        logger.warning(
            f"NFST pág.{nfst.get('pagina')} conta={nfst_conta}: nenhuma Fatura com mesmo numero_conta encontrada"
        )
        return None, None

    # Tenta desambiguar por mes_referencia
    nfst_mes = normalize_mes_referencia(nfst.get("mes_referencia"))
    if nfst_mes is None:
        # Fallback: tenta extrair mês de data_servico
        nfst_mes = mes_from_data_servico(nfst.get("data_servico"))

    if nfst_mes:
        por_mes = [f for f in candidatas if normalize_mes_referencia(f.get("mes_referencia")) == nfst_mes]
        if len(por_mes) == 1:
            return por_mes[0], f"numero_conta={nfst_conta} + mes_referencia={nfst_mes}"
        if len(por_mes) > 1:
            logger.warning(
                f"NFST pág.{nfst.get('pagina')} conta={nfst_conta} mês={nfst_mes}: "
                f"{len(por_mes)} Faturas candidatas — usando a primeira"
            )
            return por_mes[0], f"numero_conta={nfst_conta} + mes_referencia={nfst_mes} (ambíguo)"

    # Sem mês ou sem match por mês: aceita se há apenas 1 candidata
    if len(candidatas) == 1:
        logger.warning(
            f"NFST pág.{nfst.get('pagina')} conta={nfst_conta}: "
            "mes_referencia indisponível — usando única Fatura candidata"
        )
        return candidatas[0], f"numero_conta={nfst_conta} (sem mes_referencia)"

    logger.warning(
        f"NFST pág.{nfst.get('pagina')} conta={nfst_conta}: "
        f"{len(candidatas)} Faturas candidatas sem desambiguação por mês — merge ignorado"
    )
    return None, None


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------


def merge_nfst_with_fatura(extracted_nfs: list[dict]) -> list[dict]:
    """
    Pós-processamento: vincula NFSTs a Faturas de telecomunicações do mesmo ciclo.

    Para cada NFST com valor_total = null:
      1. Busca Faturas com mesmo numero_conta (normalizado)
      2. Desambigua pelo mes_referencia (ou data_servico como fallback)
      3. Se encontrada: preenche valor_total da NFST com o da Fatura
         e adiciona campo de proveniência
      4. Se não encontrada: mantém valor_total = null e loga aviso

    :param extracted_nfs: Lista de dicts extraídos pelo NFExtractor para um PDF.
    :returns: Lista modificada in-place (mesmos objetos, sem cópias extras).
    """
    if not extracted_nfs:
        return extracted_nfs

    nfsts_a_mergear = [doc for doc in extracted_nfs if needs_merge(doc)]
    if not nfsts_a_mergear:
        return extracted_nfs

    faturas = [doc for doc in extracted_nfs if is_fatura_telecom(doc)]

    if not faturas:
        logger.warning(
            f"Há {len(nfsts_a_mergear)} NFST(s) com valor_total=null "
            "mas nenhuma Fatura de telecom foi extraída neste PDF — "
            "valor_total permanece null (revisão manual necessária)"
        )
        return extracted_nfs

    merged_count = 0
    for nfst in nfsts_a_mergear:
        fatura, descricao = find_fatura_for_nfst(nfst, faturas)

        if fatura is None:
            logger.warning(
                f"NFST pág.{nfst.get('pagina')} numero_nf={nfst.get('numero_nf')!r}: "
                "sem Fatura correspondente — valor_total permanece null"
            )
            continue

        fatura_valor = normalize_value(fatura.get("valor_total"))
        nfst_conta = nfst.get("numero_conta") or fatura.get("numero_conta")

        logger.warning(
            f"Merge NFST pág.{nfst.get('pagina')} + Fatura pág.{fatura.get('pagina')} "
            f"via {descricao} → valor_total = R$ {fatura_valor:.2f}"
        )

        nfst["valor_total"] = fatura_valor
        nfst["origem_valor_total"] = f"merge_fatura:{nfst_conta}"

        # Propaga mes_referencia da Fatura para a NFST se ainda estiver null
        if not nfst.get("mes_referencia") and fatura.get("mes_referencia"):
            nfst["mes_referencia"] = fatura["mes_referencia"]

        merged_count += 1

    if merged_count:
        logger.warning(f"NFST merge concluído: {merged_count}/{len(nfsts_a_mergear)} NFST(s) preenchidas")
    else:
        logger.warning(f"NFST merge: nenhuma das {len(nfsts_a_mergear)} NFST(s) pôde ser vinculada")

    return extracted_nfs
