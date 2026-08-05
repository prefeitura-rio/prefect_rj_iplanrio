"""Utilidades para processamento de dados de precipitação do AlertaRio via SFTP."""

from defusedxml import ElementTree as ET
from pathlib import Path
from string import Template
from typing import Any, Dict

import pandas as pd
import pendulum
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def parse_float(value: str | None) -> float | None:
    """Converte string para float, tratando 'None' como None.

    Valores literais "None" (string) são convertidos para None (null).
    Outros valores são convertidos para float ou retornam None se inválidos.

    :param value: Valor a ser convertido.
    :returns: Float ou None.
    """
    if value is None or value == "None":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_xml_to_records(
    xml_content: str,
    source_file: str,
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
    """Parse XML AlertaRio em dois conjuntos de registros.

    Extrai dados de estações do XML estruturado AlertaRio, separando
    registros pluviométricos de meteorológicos. Cada estação pode ter
    ambos os tipos de dados ou apenas dados de chuva.

    :param xml_content: Conteúdo XML como string.
    :param source_file: Nome do arquivo origem (para rastreamento).
    :returns: Tupla (lista_registros_pluviometricos, lista_registros_meteorologicos).
    :raises ET.ParseError: Se o XML não for válido.
    """
    logger.info("Fazendo parsing do XML: %s", source_file)

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error("Erro ao fazer parse do XML: %s", e)
        raise

    pluviometric_records = []
    meteorological_records = []

    for estacao in root.findall("estacao"):
        estacao_id = estacao.get("id")
        estacao_nome = estacao.get("nome")
        estacao_type = estacao.get("type")

        localizacao = estacao.find("localizacao")
        bacia = localizacao.get("bacia") if localizacao is not None else None
        latitude = localizacao.get("latitude") if localizacao is not None else None
        longitude = localizacao.get("longitude") if localizacao is not None else None

        if latitude is not None:
            try:
                latitude = float(latitude)
            except (ValueError, TypeError):
                latitude = None

        if longitude is not None:
            try:
                longitude = float(longitude)
            except (ValueError, TypeError):
                longitude = None

        chuvas = estacao.find("chuvas")
        if chuvas is not None:
            hora_medicao = chuvas.get("hora")

            record_pluv = {
                "id": estacao_id,
                "nome": estacao_nome,
                "bacia": bacia,
                "latitude": latitude,
                "longitude": longitude,
                "m05": parse_float(chuvas.get("m05")),
                "m10": parse_float(chuvas.get("m10")),
                "m15": parse_float(chuvas.get("m15")),
                "h01": parse_float(chuvas.get("h01")),
                "h04": parse_float(chuvas.get("h04")),
                "h24": parse_float(chuvas.get("h24")),
                "h96": parse_float(chuvas.get("h96")),
                "mes": parse_float(chuvas.get("mes")),
                "hora": hora_medicao,
            }
            pluviometric_records.append(record_pluv)

        met = estacao.find("met")
        if met is not None and estacao_type == "met":
            hora_medicao = chuvas.get("hora") if chuvas is not None else None

            record_met = {
                "id": estacao_id,
                "nome": estacao_nome,
                "bacia": bacia,
                "latitude": latitude,
                "longitude": longitude,
                "temperatura": parse_float(met.get("temperatura")),
                "umidade": parse_float(met.get("umidade")),
                "sensacao": parse_float(met.get("sensacao")),
                "pressao": parse_float(met.get("pressao")),
                "velvento": parse_float(met.get("velvento")),
                "dirvento": parse_float(met.get("dirvento")),
                "hora": hora_medicao,
            }
            meteorological_records.append(record_met)

    logger.info(
        "Parse concluído: %d registros pluviométricos, %d meteorológicos",
        len(pluviometric_records),
        len(meteorological_records),
    )

    return pluviometric_records, meteorological_records


def transform_pluviometric_dataframe(dfr: pd.DataFrame) -> pd.DataFrame:
    """Transforma DataFrame pluviométrico bruto para formato padrão BigQuery.

    Realiza renomeação de colunas, parse de datas, remoção de duplicatas
    e limpeza de valores ausentes.

    :param dfr: DataFrame com dados brutos de precipitação.
    :returns: DataFrame transformado e limpo.
    """
    if dfr.empty:
        logger.warning("DataFrame pluviométrico vazio, retornando sem transformações")
        return dfr

    logger.info("Transformando dados pluviométricos: %d registros", len(dfr))

    rename_cols = {
        "id": "id_estacao",
        "nome": "nome_estacao",
        "bacia": "bacia",
        "latitude": "latitude",
        "longitude": "longitude",
        "m05": "acumulado_chuva_5min",
        "m10": "acumulado_chuva_10min",
        "m15": "acumulado_chuva_15min",
        "h01": "acumulado_chuva_1h",
        "h04": "acumulado_chuva_4h",
        "h24": "acumulado_chuva_24h",
        "h96": "acumulado_chuva_96h",
        "mes": "acumulado_chuva_mes",
        "hora": "data_medicao",
    }

    dfr = dfr.rename(columns=rename_cols)

    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"], format="%Y-%m-%dT%H:%M:%S")

    dfr = dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    keep_cols = [col for col in rename_cols.values() if col in dfr.columns]
    dfr = dfr[keep_cols]

    logger.info("Dados pluviométricos transformados: %d registros", len(dfr))

    return dfr


def transform_meteorological_dataframe(dfr: pd.DataFrame) -> pd.DataFrame:
    """Transforma DataFrame meteorológico bruto para formato padrão BigQuery.

    Realiza renomeação de colunas, parse de datas, remoção de duplicatas
    e limpeza de valores ausentes.

    :param dfr: DataFrame com dados brutos meteorológicos.
    :returns: DataFrame transformado e limpo.
    """
    if dfr.empty:
        logger.warning("DataFrame meteorológico vazio, retornando sem transformações")
        return dfr

    logger.info("Transformando dados meteorológicos: %d registros", len(dfr))

    rename_cols = {
        "id": "id_estacao",
        "nome": "nome_estacao",
        "bacia": "bacia",
        "latitude": "latitude",
        "longitude": "longitude",
        "temperatura": "temperatura",
        "umidade": "umidade_ar",
        "sensacao": "sensacao_termica",
        "pressao": "pressao_atmosferica",
        "velvento": "velocidade_vento",
        "dirvento": "direcao_vento",
        "hora": "data_medicao",
    }

    dfr = dfr.rename(columns=rename_cols)

    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"], format="%Y-%m-%dT%H:%M:%S")

    dfr = dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    keep_cols = [col for col in rename_cols.values() if col in dfr.columns]
    dfr = dfr[keep_cols]

    logger.info("Dados meteorológicos transformados: %d registros", len(dfr))

    return dfr


def save_dataframe_to_parquet_partitions(
    dfr: pd.DataFrame,
    data_type: str,
    partition_column: str = "data_medicao",
) -> Path:
    """Salva DataFrame em partições Parquet com estrutura ano/mes/data.

    Cria estrutura de diretórios particionados e salva cada partição
    como arquivo Parquet comprimido. O timestamp do arquivo é adicionado
    ao nome para rastreamento.

    :param dfr: DataFrame a ser salvo.
    :param data_type: Tipo de dado ("pluviometric" ou "meteorological").
    :param partition_column: Coluna usada para particionamento.
    :returns: Caminho do diretório raiz das partições.
    """
    if dfr.empty:
        logger.warning(
            "DataFrame %s vazio, criando diretório vazio apenas", data_type
        )
        base_path = Path(f"/tmp/precipitacao_alertario/{data_type}")
        base_path.mkdir(parents=True, exist_ok=True)
        return base_path

    base_path = Path(f"/tmp/precipitacao_alertario/{data_type}")
    base_path.mkdir(parents=True, exist_ok=True)

    dfr[partition_column] = pd.to_datetime(dfr[partition_column])
    dfr["ano_particao"] = dfr[partition_column].dt.strftime("%Y")
    dfr["mes_particao"] = dfr[partition_column].dt.strftime("%m")
    dfr["data_particao"] = dfr[partition_column].dt.strftime("%Y-%m-%d")

    grouped = dfr.groupby(["ano_particao", "mes_particao", "data_particao"], dropna=False)

    timestamp_suffix = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M%S")

    for (ano, mes, data), group_data in grouped:
        partition_path = base_path / f"ano={ano}" / f"mes={mes}" / f"data={data}"
        partition_path.mkdir(parents=True, exist_ok=True)

        group_data_clean = group_data.drop(
            columns=["ano_particao", "mes_particao", "data_particao", partition_column]
        )

        filename = f"data_{timestamp_suffix}.parquet"
        filepath = partition_path / filename

        group_data_clean.to_parquet(
            filepath,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        logger.info("Partição salva: %s", filepath)

    return base_path
