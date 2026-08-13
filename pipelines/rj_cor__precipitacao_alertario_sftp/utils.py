"""Utilidades para processamento de dados de precipitação do AlertaRio via SFTP."""

from pathlib import Path
from typing import Any

from defusedxml import ElementTree as ET
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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
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
        root = ET.parse(xml_content)
    except ET.ParseError as e:
        logger.error("Erro ao fazer parse do XML: %s", e)
        raise

    pluviometric_records = []
    meteorological_records = []

    for estacao in root.findall("estacao"):
        estacao_id = estacao.get("id")
        estacao_type = estacao.get("type")

        chuvas = estacao.find("chuvas")
        if chuvas is not None:
            hora_medicao = chuvas.get("hora").replace("T", " ")

            record_pluv = {
                "id_estacao": estacao_id,
                "data_medicao": hora_medicao,
                "acumulado_chuva_5min": parse_float(chuvas.get("m05")),
                "acumulado_chuva_10min": parse_float(chuvas.get("m10")),
                "acumulado_chuva_15min": parse_float(chuvas.get("m15")),
                "acumulado_chuva_30min": parse_float(chuvas.get("m30")),
                "acumulado_chuva_1hora": parse_float(chuvas.get("h01")),
                "acumulado_chuva_2hora": parse_float(chuvas.get("h02")),
                "acumulado_chuva_3hora": parse_float(chuvas.get("h03")),
                "acumulado_chuva_4horas": parse_float(chuvas.get("h04")),
                "acumulado_chuva_6horas": parse_float(chuvas.get("h06")),
                "acumulado_chuva_12horas": parse_float(chuvas.get("h12")),
                "acumulado_chuva_24horas": parse_float(chuvas.get("h24")),
                "acumulado_chuva_96horas": parse_float(chuvas.get("h96")),
                "acumulado_chuva_mes": parse_float(chuvas.get("mes")),
            }
            pluviometric_records.append(record_pluv)

        met = estacao.find("met")
        if met is not None and estacao_type == "met":
            hora_medicao = met.get("hora").replace("T", " ") if met is not None else None

            record_met = {
                "id_estacao": estacao_id,
                "temperatura": parse_float(met.get("temperatura")),
                "umidade_ar": parse_float(met.get("umidade")),
                "sensacao_termica": parse_float(met.get("sensacao")),
                "pressao_atmosferica": parse_float(met.get("pressao")),
                "temperatura_orvalho": parse_float(met.get("pontoOrvalho")),
                "velocidade_vento": parse_float(met.get("velvento")),
                "direcao_vento": parse_float(met.get("dirvento")),
                "data_medicao": hora_medicao,
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

    keep_cols = [
        "id_estacao",
        "data_medicao",
        "acumulado_chuva_5min",
        "acumulado_chuva_10min",
        "acumulado_chuva_15min",
        "acumulado_chuva_30min",
        "acumulado_chuva_1hora",
        "acumulado_chuva_2hora",
        "acumulado_chuva_3hora",
        "acumulado_chuva_4horas",
        "acumulado_chuva_6horas",
        "acumulado_chuva_12horas",
        "acumulado_chuva_24horas",
        "acumulado_chuva_96horas",
        "acumulado_chuva_mes",
    ]

    dfr = dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

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

    keep_cols = [
        "id_estacao",
        "temperatura",
        "umidade_ar",
        "sensacao_termica",
        "pressao_atmosferica",
        "temperatura_orvalho",
        "velocidade_vento",
        "direcao_vento",
        "data_medicao",
    ]

    dfr = dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

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
            columns=["ano_particao", "mes_particao", "data_particao"]
        )

        filename = f"data_{timestamp_suffix}.csv"
        filepath = partition_path / filename
        logger.info("Salvando partição: %s com %d registros", filepath, len(group_data_clean))
        group_data_clean.to_csv(filepath, index=False, sep=",")

    return base_path
