"""Utilidades para processamento de dados de precipitação do AlertaRio.

Este módulo fornece funções auxiliares para download, parse e transformação
de arquivos XML de precipitação do serviço AlertaRio do Rio de Janeiro.
As funções consolidam múltiplos XMLs em DataFrames particionados e os salvam
em estrutura ano/mês/data no disco local.
"""

import os
from pathlib import Path
from typing import Any

import dotenv
import pandas as pd
from defusedxml import ElementTree as ET
from google.cloud import storage
from google.oauth2 import service_account
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def download_xml_files_from_gcs(
    bucket_name: str,
    file_names: list[str],
) -> list[str]:
    """Baixa múltiplos arquivos XML do GCS e retorna seus conteúdos.

    Conecta ao bucket Google Cloud Storage e baixa os arquivos XML
    especificados, retornando uma lista com os conteúdos de cada arquivo.
    Se nenhum arquivo for fornecido, retorna uma lista vazia.

    :param bucket_name: Nome do bucket GCS.
    :param file_names: Lista de nomes de arquivos XML a baixar.
    :returns: Lista com conteúdos XML como strings.
    :raises Exception: Se houver erro ao baixar qualquer arquivo.
    """
    if not file_names:
        logger.info("Lista de arquivos vazia, retornando lista vazia")
        return []

    logger.info(
        "Baixando %d arquivo(s) do bucket %s",
        len(file_names),
        bucket_name,
    )

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    xml_contents: list[str] = []

    for file_name in file_names:
        try:
            blob = bucket.blob(file_name)
            content = blob.download_as_text(encoding="utf-8")
            xml_contents.append(content)
            logger.info("Arquivo baixado: %s", file_name)
        except Exception as e:
            logger.error("Erro ao baixar arquivo %s: %s", file_name, str(e))
            raise

    logger.info(
        "Download concluído: %d de %d arquivo(s)",
        len(xml_contents),
        len(file_names),
    )
    return xml_contents



def parse_float(value: str | None) -> float | None:
    """Converte string para float, tratando a string 'None' como None Python.

    A string literal ``"None"`` é convertida para ``None``.
    Outros valores são convertidos para float ou retornam None se inválidos.
    Útil para lidar com dados XML que usam a string 'None' para valores ausentes.

    :param value: Valor a ser convertido (string ou None).
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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extrai registros de chuva e meteorológicos de um XML AlertaRio.

    Realiza parsing de um XML estruturado em estações, cada uma contendo
    dados pluviométricos (``<chuvas>``) e/ou meteorológicos (``<met>``).
    Os registros são separados em duas listas distintas.

    :param xml_content: Conteúdo XML como string.
    :returns: Tupla (lista_registros_pluviometricos, lista_registros_meteorologicos).
    :raises ET.ParseError: Se o XML não for válido ou malformado.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error("Erro ao fazer parse do XML: %s", e)
        raise

    pluviometric_records = []
    meteorological_records = []

    for estacao in root.findall("estacao"):
        estacao_id = estacao.get("id")
        estacao_type = estacao.get("type")

        # Processar dados pluviométricos
        chuvas = estacao.find("chuvas")
        if chuvas is not None:
            hora_chuva = chuvas.get("hora")
            if hora_chuva:
                hora_medicao = hora_chuva.replace("T", " ")
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

        # Processar dados meteorológicos
        met = estacao.find("met")
        if met is not None and estacao_type == "met":
            chuvas = estacao.find("chuvas")
            hora_medicao_met = None
            if chuvas is not None:
                hora_chuva = chuvas.get("hora")
                if hora_chuva:
                    hora_medicao_met = hora_chuva.replace("T", " ")

            if hora_medicao_met:
                record_met = {
                    "id_estacao": estacao_id,
                    "temperatura": parse_float(met.get("temperatura")),
                    "umidade_ar": parse_float(met.get("umidade")),
                    "sensacao_termica": parse_float(met.get("sensacao")),
                    "pressao_atmosferica": parse_float(met.get("pressao")),
                    "temperatura_orvalho": parse_float(met.get("pontoOrvalho")),
                    "velocidade_vento": parse_float(met.get("velvento")),
                    "direcao_vento": parse_float(met.get("dirvento")),
                    "data_medicao": hora_medicao_met,
                }
                meteorological_records.append(record_met)

    logger.info(
        "Parse concluído: %d registros pluviométricos, %d meteorológicos",
        len(pluviometric_records),
        len(meteorological_records),
    )

    return pluviometric_records, meteorological_records


def transform_pluviometric_dataframe(dfr: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza dados pluviométricos para ingestão no BigQuery.

    Remove duplicatas por estação e data de medição, seleciona as colunas
    esperadas pelo schema BigQuery e registra o resultado.

    :param dfr: DataFrame com dados brutos de precipitação.
    :returns: DataFrame transformado e pronto para ingestão.
    """
    if dfr.empty:
        logger.info("DataFrame pluviométrico vazio, retornando sem transformações")
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
    """Limpa e padroniza dados meteorológicos para ingestão no BigQuery.

    Remove duplicatas por estação e data de medição, seleciona as colunas
    esperadas pelo schema BigQuery e registra o resultado.

    :param dfr: DataFrame com dados brutos meteorológicos.
    :returns: DataFrame transformado e pronto para ingestão.
    """
    if dfr.empty:
        logger.info("DataFrame meteorológico vazio, retornando sem transformações")
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


def save_dataframe_to_csv_partitions(
    dfr: pd.DataFrame,
    data_type: str,
    partition_column: str = "data_medicao",
) -> Path:
    """Salva DataFrame em partições CSV com estrutura ano/mês/dia.

    Particiona o DataFrame por data e salva cada partição como arquivo CSV.
    A estrutura de diretórios segue o padrão ``/tmp/{data_type}/ano_particao={ano}/mes_particao={mes}/data_particao={data}/``.
    Cada arquivo é nomeado com o timestamp do primeiro registro da partição
    para rastreabilidade.

    :param dfr: DataFrame a ser salvo.
    :param data_type: Tipo de dado (``pluviometric`` ou ``meteorological``).
    :param partition_column: Coluna usada para particionamento (padrão: ``data_medicao``).
    :returns: Caminho do diretório raiz onde as partições foram salvas.
    """
    if dfr.empty:
        logger.info(
            "DataFrame %s vazio, criando diretório vazio apenas", data_type
        )
        base_path = Path(f"/tmp/{data_type}")
        base_path.mkdir(parents=True, exist_ok=True)
        return base_path

    dfr[partition_column] = pd.to_datetime(dfr[partition_column])
    dfr["ano_particao"] = dfr[partition_column].dt.strftime("%Y")
    dfr["mes_particao"] = dfr[partition_column].dt.strftime("%m")
    dfr["data_particao"] = dfr[partition_column].dt.strftime("%Y-%m-%d")

    grouped = dfr.groupby(
        ["ano_particao", "mes_particao", "data_particao"], dropna=False
    )

    for (ano, mes, data), group_data in grouped:
        partition_path = Path(
            f"/tmp/{data_type}/ano_particao={ano}/mes_particao={mes}/data_particao={data}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        group_data_clean = group_data.drop(
            columns=["ano_particao", "mes_particao", "data_particao"]
        )

        # Timestamp do primeiro registro da partição para nome do arquivo
        group_data_clean[partition_column] = pd.to_datetime(
            group_data_clean[partition_column]
        )
        timestamp_suffix = group_data_clean[partition_column].iloc[0].strftime(
            "%Y%m%d%H%M%S"
        )

        filename = f"data_{timestamp_suffix}.csv"
        filepath = partition_path / filename
        logger.info("Salvando partição: %s com %d registros", filepath, len(group_data_clean))
        group_data_clean.to_csv(filepath, index=False, sep=",")

    return Path(f"/tmp/{data_type}")


def save_pluviometric_and_meteorological_dataframes(
    dfr_pluviometric: pd.DataFrame,
    dfr_meteorological: pd.DataFrame,
    partition_column: str = "data_medicao",
) -> tuple[Path, Path]:
    """Salva DataFrames pluviométrico e meteorológico em partições separadas.

    Orchestração conveniente que salva ambos os tipos de dados em operações
    paralelas, retornando os caminhos dos diretórios raiz das partições.

    :param dfr_pluviometric: DataFrame com dados pluviométricos transformados.
    :param dfr_meteorological: DataFrame com dados meteorológicos transformados.
    :param partition_column: Coluna usada para particionamento (padrão: ``data_medicao``).
    :returns: Tupla (Path para pluviométricos, Path para meteorológicos).
    """
    logger.info("Salvando DataFrames pluviométrico e meteorológico")

    pluviometric_path = save_dataframe_to_csv_partitions(
        dfr=dfr_pluviometric,
        data_type="pluviometric",
        partition_column=partition_column,
    )

    meteorological_path = save_dataframe_to_csv_partitions(
        dfr=dfr_meteorological,
        data_type="meteorological",
        partition_column=partition_column,
    )

    return pluviometric_path, meteorological_path


def process_multiple_xml_files(
    xml_contents: list[str],
) -> tuple[list[str], list[str]]:
    """Processa múltiplos XMLs e salva um arquivo de partição por timestamp.

    Pipeline que itera sobre cada arquivo XML, extrai registros pluviométricos
    e meteorológicos, aplica transformações de limpeza e salva em partições
    ano/mês/dia, criando um arquivo separado para cada timestamp de entrada.

    A função segue estes passos:
    1. Para cada XML: executa parse, transforma e salva em partição individual
    2. Coleta os caminhos de todos os arquivos gerados
    3. Retorna listas de caminhos para dados pluviométricos e meteorológicos

    :param xml_contents: Lista com conteúdos XML como strings.
    :returns: Tupla (lista de caminhos pluviométricos, lista de caminhos meteorológicos) como strings.
    :raises Exception: Se houver erro no parse de qualquer XML.
    """


    logger.info("Iniciando processamento de %d arquivo(s) XML", len(xml_contents))

    # Processa cada XML individualmente
    for xml_index, xml_content in enumerate(xml_contents, start=1):
        try:
            pluviometric_records, meteorological_records = parse_xml_to_records(
                xml_content=xml_content,
            )

            logger.info(
                "XML %d/%d processado: %d pluviométricos, %d meteorológicos",
                xml_index,
                len(xml_contents),
                len(pluviometric_records),
                len(meteorological_records),
            )

            # Criar DataFrames individuais para este XML
            dfr_pluviometric = pd.DataFrame(pluviometric_records)
            dfr_meteorological = pd.DataFrame(meteorological_records)

            # Transformar (limpar e padronizar)
            dfr_pluviometric = transform_pluviometric_dataframe(dfr_pluviometric)
            dfr_meteorological = transform_meteorological_dataframe(dfr_meteorological)

            # Salvar em partições individuais por timestamp
            pluviometric_path, meteorological_path = (
                save_pluviometric_and_meteorological_dataframes(
                    dfr_pluviometric=dfr_pluviometric,
                    dfr_meteorological=dfr_meteorological,
                    partition_column="data_medicao",
                )
            )

        except Exception as e:
            logger.error("Erro ao processar XML %d: %s", xml_index, str(e))
            raise

    logger.info(
        "Processamento completo de %d arquivo(s): %d partições pluviométricas, %d meteorológicas",
        len(xml_contents),
        len(pluviometric_path),
        len(meteorological_path),
    )

    return pluviometric_path, meteorological_path
