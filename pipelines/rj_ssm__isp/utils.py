"""Utilitários gerais do pipeline rj_ssm__isp."""

import hashlib
import json
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

import pandas as pd
from prefect_rj_iplanrio.logging import get_logger

from constants import (
    SP_TZ,
    CSV_COLUMNS,
    DATE_FIELDS,
    MESES_ABREV,
    MUNICIPIO_RIO_DE_JANEIRO,
    PERIODO_DIA_POR_FFAIXA,
)

logger = get_logger(__name__)


def _ultimo_trimestre(ref: datetime) -> tuple[str, str]:
    """Retorna (data_inicio, data_fim) do último trimestre completo.

    Trimestres: Jan–Mar, Abr–Jun, Jul–Set, Out–Dez.

    :param ref: Data de referência.
    :returns: Tupla ``(data_inicio, data_fim)`` no formato ``YYYY-MM-DD``.
    """
    mes = ref.month
    ano = ref.year

    trimestre_atual = (mes - 1) // 3
    trimestre_anterior = trimestre_atual - 1
    if trimestre_anterior < 0:
        trimestre_anterior = 3
        ano -= 1

    mes_inicio = trimestre_anterior * 3 + 1
    mes_fim = mes_inicio + 2

    if mes_fim == 12:
        ultimo_dia = datetime(ano, 12, 31)
    else:
        ultimo_dia = datetime(ano, mes_fim + 1, 1) - timedelta(days=1)

    return datetime(ano, mes_inicio, 1).strftime("%Y-%m-%d"), ultimo_dia.strftime("%Y-%m-%d")


def resolve_dates(
    fase: Literal["parcial", "consolidados", "errata"],
    data_inicio: Optional[str],
    data_fim: Optional[str],
) -> dict[str, str]:
    """Resolve ``data_inicio`` e ``data_fim`` conforme a fase, quando não fornecidos.

    Retorna um dict em vez de tupla para compatibilidade com ``@task`` do Prefect,
    que não suporta unpacking direto de tuplas retornadas por tasks.

    - ``"parcial"`` (Fase 1): janela do dia anterior (D-1 a D-1).
    - ``"consolidados"`` (Fase 2): janela dos últimos 30 dias (D-30 até D-1).
    - ``"errata"`` (Fase 3): janela do último trimestre completo.

    :param fase: Fase de disponibilidade dos dados.
    :param data_inicio: Data de início explícita, ou ``None`` para usar o default da fase.
    :param data_fim: Data de fim explícita, ou ``None`` para usar o default da fase.
    :returns: Dict com chaves ``"data_inicio"`` e ``"data_fim"`` no formato ``YYYY-MM-DD``.
    :raises ValueError: Se ``fase`` for inválida.
    """
    now = datetime.now(tz=SP_TZ)

    if fase == "parcial":
        if data_inicio is None:
            data_inicio = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        if data_fim is None:
            data_fim = data_inicio
    elif fase == "consolidados":
        if data_inicio is None:
            data_inicio = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        if data_fim is None:
            data_fim = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    elif fase == "errata":
        inicio_tri, fim_tri = _ultimo_trimestre(now)
        if data_inicio is None:
            data_inicio = inicio_tri
        if data_fim is None:
            data_fim = fim_tri
    else:
        raise ValueError(f"Fase inválida: {fase!r}. Use 'parcial', 'consolidados' ou 'errata'.")

    return {"data_inicio": data_inicio, "data_fim": data_fim}


def strip_accents(text: str) -> str:
    """Remove acentos de uma string, preservando os demais caracteres.

    :param text: Texto de entrada, possivelmente acentuado.
    :returns: Texto sem marcas de acentuação.
    """
    return "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    )


def resolve_crime_codes(
    titles: list[str], delito_do_domain: dict[int, str]
) -> list[int]:
    """Resolve nomes de "Título do DO" para os códigos numéricos do domínio da camada.

    A comparação ignora acentuação e caixa, pois o domínio da API e a lista de
    títulos configurada podem divergir nesses detalhes.

    :param titles: Nomes de "Título do DO" a resolver.
    :param delito_do_domain: Domínio do campo ``delito_do`` (código -> nome),
        obtido via :meth:`IspGeoClient.get_field_domains`.
    :returns: Códigos numéricos correspondentes aos títulos encontrados.
    """
    lookup = {
        strip_accents(name).lower(): code for code, name in delito_do_domain.items()
    }
    codes: list[int] = []
    for title in titles:
        key = strip_accents(title).lower()
        if key not in lookup:
            logger.warning("Título do DO não encontrado no domínio: %r", title)
            continue
        codes.append(lookup[key])
    return codes


def build_where(
    data_inicio: str,
    data_fim: str,
    crime_codes: Optional[list[int]],
    municipio_cod: int = MUNICIPIO_RIO_DE_JANEIRO,
) -> str:
    """Monta a cláusula ``WHERE`` da consulta à camada de microdados.

    ``crime_codes=None`` omite o filtro de ``delito_do`` inteiramente (traz
    TODOS os registros do período/município, mesmo que o código de
    ``delito_do`` não esteja cadastrado no domínio da camada — o domínio fica
    desatualizado em relação aos dados reais, ver uso em ``task.py``).

    :param data_inicio: Data de início (``YYYY-MM-DD``), inclusive.
    :param data_fim: Data de fim (``YYYY-MM-DD``), inclusive.
    :param crime_codes: Códigos de ``delito_do`` a incluir, ou ``None`` para
        não filtrar por tipo de delito.
    :param municipio_cod: Código IBGE do município do fato.
    :returns: Cláusula ``WHERE`` no dialeto SQL da API ArcGIS.
    """
    clausulas = []
    if crime_codes is not None:
        codes_sql = ",".join(str(c) for c in crime_codes)
        clausulas.append(f"delito_do IN ({codes_sql})")
    clausulas.append(f"fmun_cod = {municipio_cod}")
    clausulas.append(f"datf >= timestamp '{data_inicio} 00:00:00'")
    clausulas.append(f"datf <= timestamp '{data_fim} 23:59:59'")
    return " AND ".join(clausulas)


def format_date(epoch_ms: Optional[int]) -> Optional[str]:
    """Converte um timestamp epoch em milissegundos para ``YYYY-MM-DD``.

    :param epoch_ms: Timestamp em milissegundos desde a época UNIX, ou ``None``.
    :returns: Data formatada em UTC, ou ``None`` se ``epoch_ms`` for ``None``.
    """
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d"
    )


def build_wkt_point(
    longitude: Optional[float], latitude: Optional[float]
) -> Optional[str]:
    """Monta um ponto WKT a partir de coordenadas geográficas.

    Formato ``'POINT(long lat)'``, aceito pelo BigQuery para colunas
    ``GEOGRAPHY`` (via ``ST_GEOGFROMTEXT`` ou carga direta com schema
    ``GEOGRAPHY``).

    :param longitude: Longitude do ponto, ou ``None``.
    :param latitude: Latitude do ponto, ou ``None``.
    :returns: String WKT do ponto, ou ``None`` se alguma coordenada faltar.
    """
    if longitude is None or latitude is None:
        return None
    return f"POINT({longitude} {latitude})"


def enrich_row(row: dict, ffaixa_code: Optional[int]) -> dict:
    """Deriva colunas auxiliares a partir de ``datf``, ``horf``, ``fdiasem``, ``ffaixa``.

    Deriva ano/mês do fato, faixa horária, dia da semana e ponto WKT,
    mantendo as colunas originais intactas.

    :param row: Linha decodificada (mutada in-place e também retornada).
    :param ffaixa_code: Código bruto do campo ``ffaixa`` (antes de decodificado).
    :returns: A própria ``row``, com as colunas derivadas adicionadas.
    """
    datf = row.get("datf")
    if datf:
        dt = datetime.strptime(datf, "%Y-%m-%d")
        row["ano_fato"] = dt.year
        row["id_mes_fato"] = dt.month
        row["mes_fato"] = MESES_ABREV[dt.month]
    else:
        row["ano_fato"] = row["id_mes_fato"] = row["mes_fato"] = None

    horf = row.get("horf")
    row["hora_faixa"] = int(horf.split(":")[0]) if horf else None

    fdiasem = row.get("fdiasem")
    if fdiasem and " - " in str(fdiasem):
        id_dia, nome_dia = fdiasem.split(" - ", 1)
        row["id_dia_semana_fato"] = int(id_dia.strip())
        row["dia_semana_fato"] = nome_dia.strip()
    else:
        row["id_dia_semana_fato"] = row["dia_semana_fato"] = None

    row["periodo_dia"] = PERIODO_DIA_POR_FFAIXA.get(ffaixa_code)

    wkt = build_wkt_point(row.get("point_x"), row.get("point_y"))
    row["wkt"] = wkt
    row["geography"] = wkt

    return row


def decode_row(attrs: dict, domains: dict[str, dict[int, str]]) -> dict:
    """Decodifica uma feature bruta da API, aplicando datas e domínios.

    :param attrs: Dict de atributos brutos (``feature["attributes"]``).
    :param domains: Domínios de campos codificados, por :meth:`IspGeoClient.get_field_domains`.
    :returns: Linha decodificada e enriquecida com colunas derivadas.
    """
    row = dict(attrs)
    ffaixa_code = attrs.get("ffaixa")
    # Renomeia o campo "fase" da API para "fase_divulgacao" para liberar
    # a chave "fase" para o label de fase do pipeline.
    if "fase" in row:
        row["fase_divulgacao"] = row.pop("fase")
    for field in DATE_FIELDS:
        row[field] = format_date(attrs.get(field))
    for field, mapping in domains.items():
        if field in row and row[field] in mapping:
            row[field] = mapping[row[field]]
    return enrich_row(row, ffaixa_code)


def _add_id_hash(data: list[dict]) -> list[dict]:
    """Adiciona id_hash a cada row com base nos dados brutos da API.

    Hasheado antes da criação do DataFrame para evitar inconsistências de dtype
    inference do pandas (ex: coluna inferida como int64 num run e float64 em outro
    quando None aparece). json.dumps com sort_keys garante representação canônica
    e determinística independente de tipo ou ordem de chaves.

    Usa os 32 chars do MD5 (128 bits) — suficiente para o volume dessa API.
    """
    for row in data:
        row["id_hash"] = hashlib.md5(
            json.dumps(row, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
    return data


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Monta o DataFrame final, na ordem e com os nomes de coluna do BigQuery.

    :param rows: Linhas decodificadas por :func:`decode_row`.
    :returns: DataFrame com uma coluna por entrada de ``CSV_COLUMNS``,
        na ordem definida ali.
    """
    data = {
        label: [row.get(field) for row in rows] for field, label in CSV_COLUMNS
    }
    return pd.DataFrame(data=data, dtype=object)

