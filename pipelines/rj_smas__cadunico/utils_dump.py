# ruff: noqa

import asyncio
import shutil
from datetime import datetime
from os import system
from pathlib import Path
from typing import List
from uuid import uuid4
from zipfile import ZipFile
import json

import pandas as pd
from utils.types import Cloud
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from pipelines.rj_smas__cadunico.helpers import (
    get_gcs_client,
    list_blobs_with_prefix,
    read_staging_dates,
    upload_folder,
    write_text_parquet,
)
from pipelines.rj_smas__cadunico.utils_logging import (
    FileProcessingLogger,
    log_partition_comparison,
    log_ingestion_summary,
)
from prefect.logging import get_logger

logger = get_logger("toolkit")


def parse_partition_from_filename(blob_name: str) -> str:
    if "_" in blob_name:
        name_parts = blob_name.split("_")[-1].split(".")
    else:
        name_parts = blob_name.split(".")
    partition_info = None

    for name_part in name_parts:
        if name_part.startswith("A") and len(name_part) == 7:
            partition_info = name_part.replace("A", "")
            parsed_date = datetime.strptime(partition_info, "%y%m%d").strftime("%Y-%m-%d")
            return str(parsed_date)
        elif len(name_part) == 8 and name_part.isdigit():
            parsed_date = datetime.strptime(name_part, "%Y%m%d").strftime("%Y-%m-%d")
            return str(parsed_date)
        elif len(name_part) == 6 and name_part.isdigit():
            parsed_date = datetime.strptime(name_part + "01", "%Y%m%d").strftime("%Y-%m-%d")
            return str(parsed_date)
    raise ValueError(f"No partition info found in blob name: {blob_name}")


def parse_txt_first_line(filepath):
    with open(filepath) as f:  # noqa
        first_line = f.readline()
    txt_layout_version = first_line[69:74].strip().replace(".", "")
    dta_extracao_dados_hdr = first_line[82:90].strip()
    txt_date = datetime.strptime(dta_extracao_dados_hdr, "%d%m%Y").strftime("%Y-%m-%d")
    return txt_layout_version, txt_date


def ingest_file_sync(blob_name: str, bucket_name: str, dataset_id: str, table_id: str, cloud: Cloud) -> None:
    """
    Processa um arquivo ZIP: baixa, extrai, divide em chunks, converte para CSV e faz upload.

    Args:
        blob_name (str): Nome do blob para ingerir.
        bucket_name (str): Nome do bucket GCS.
        dataset_id (str): ID do dataset de destino.
        table_id (str): ID da tabela de destino.
        cloud (Cloud): O bucket e o projeto da tabela de destino.
    """
    # Inicializar logger estruturado para este arquivo
    partition = parse_partition_from_filename(blob_name)
    file_id = partition
    file_short_name = blob_name.split("/")[-1]

    # Criar diretórios temporários
    output_directory_path: Path = Path("/tmp") / f"ingestion_{uuid4()}"
    output_directory_path.mkdir(parents=True, exist_ok=True)
    temp_directory: Path = Path("/tmp") / str(uuid4())
    temp_directory.mkdir(parents=True, exist_ok=True)

    # Inicializar logger especializado
    file_logger = FileProcessingLogger(file_id, file_short_name)

    try:
        # ETAPA 1: Download
        file_logger.start_step("Download do arquivo", 1, 8)
        gcs_client = get_gcs_client()
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        fname = str(temp_directory / blob_name.rpartition("/")[-1])
        blob.download_to_filename(fname, client=gcs_client)

        file_size_mb = Path(fname).stat().st_size / (1024 * 1024)
        file_logger.start_processing(file_size_mb)
        file_logger.complete_step(True, {"tamanho_mb": file_size_mb})

        # ETAPA 2: Extração
        file_logger.start_step("Extração do ZIP", 2, 8)
        unzip_output_directory = temp_directory / "output"
        unzip_output_directory.mkdir(parents=True, exist_ok=True)
        with ZipFile(fname, "r") as zip_file:
            zip_file.extractall(unzip_output_directory)

        extracted_files = list(unzip_output_directory.glob("*"))
        file_logger.complete_step(True, {"arquivos_extraidos": len(extracted_files)})

        # ETAPA 3: Análise de arquivos TXT
        file_logger.start_step("Análise e divisão de arquivos TXT", 3, 8)
        txt_files = list(unzip_output_directory.glob("*.txt")) + list(unzip_output_directory.glob("*.TXT"))

        # Processar arquivos TXT e coletar informações
        txt_files_info = []
        txt_layout_version = None
        txt_date = None
        txt_files_after_split: List[Path] = []
        total_size_gb = 0
        large_files_count = 0

        for txt_file in txt_files:
            txt_layout_version, txt_date = parse_txt_first_line(filepath=txt_file)
            txt_file_size = txt_file.stat().st_size
            txt_file_size_gb = txt_file_size / (1024**3)
            total_size_gb += txt_file_size_gb

            # Coletar info para log consolidado
            txt_files_info.append(
                {
                    "name": txt_file.name,
                    "size_gb": txt_file_size_gb,
                    "layout_version": txt_layout_version,
                    "date": txt_date,
                }
            )

            if txt_file_size > 1e9:
                large_files_count += 1
                system(f"split -b 1G {txt_file} {txt_file}.PART_")
                txt_file.unlink()
                parts = list(unzip_output_directory.glob("*.PART_*"))
                txt_files_after_split.extend(parts)
            else:
                txt_files_after_split.append(txt_file)

        # Log consolidado da análise
        file_logger.log_file_analysis(len(txt_files), txt_files_info)
        txt_files = txt_files_after_split
        file_logger.complete_step(
            True,
            {
                "arquivos_finais": len(txt_files),
                "tamanho_total_gb": total_size_gb,
                "arquivos_grandes": large_files_count,
            },
        )

        # ETAPA 4: Conversão TXT para CSV
        file_logger.start_step("Conversão TXT para CSV", 4, 8)
        csv_files: List[Path] = []
        for txt_file in txt_files:
            csv_file = Path(str(txt_file) + ".csv")
            txt_file.rename(csv_file)
            csv_files.append(csv_file)
        file_logger.complete_step(True, {"csv_files": len(csv_files)})

        # ETAPA 5: Colunas de partição
        file_logger.start_step("Colunas de partição", 5, 8)
        partition_warning = partition != txt_date
        year, month, _ = partition.split("-")
        constants = {
            "versao_layout_particao": str(txt_layout_version),
            "ano_particao": str(int(year)),
            "mes_particao": str(int(month)),
            "data_particao": partition,
        }
        file_logger.complete_step(
            True,
            {"partição": partition, "data_consistente": not partition_warning, "layout_version": txt_layout_version},
        )

        # ETAPA 6: Conversão para Parquet, com as colunas de partição
        file_logger.start_step("Conversão para Parquet", 6, 8)
        total_csv_size = 0
        for number, csv_file in enumerate(csv_files):
            total_csv_size += csv_file.stat().st_size / (1024 * 1024)
            write_text_parquet(csv_file, output_directory_path / f"{number:04d}.parquet", constants)
            csv_file.unlink()
        file_logger.complete_step(True, {"csv_files_mb": total_csv_size})

        # ETAPA 7: Sem criação de tabela: a tabela Delta nasce no primeiro upload
        file_logger.start_step("Tabela Delta", 7, 8)
        file_logger.complete_step(True)

        # ETAPA 8: Upload para GCS e BigQuery, com a data da partição como captura
        file_logger.start_step("Upload para BigQuery/GCS", 8, 8)
        upload_folder(
            output_directory_path,
            dataset_id,
            table_id,
            cloud=cloud,
            dump_mode="append",
            source_format="parquet",
            capture=datetime.fromisoformat(partition),
        )
        file_logger.complete_step(True)

        # Finalização bem-sucedida
        file_logger.complete_processing(
            True,
            {
                "partição": partition,
                "dataset": f"{dataset_id}.{table_id}",
                "csv_files": len(csv_files),
                "size_total_mb": total_csv_size,
            },
        )

    except Exception as e:
        file_logger.complete_processing(False, {"erro": str(e)[:100]})
        raise

    finally:
        # Limpar diretórios temporários
        shutil.rmtree(temp_directory, ignore_errors=True)
        shutil.rmtree(output_directory_path, ignore_errors=True)


def get_existing_partitions(prefix: str, bucket_name: str, dataset_id: str, table_id: str) -> List[str]:
    """
    Lista as partições já processadas na área de staging.

    Args:
        prefix (str): Prefixo do caminho para listar partições.
        bucket_name (str): Nome do bucket GCS.

    Returns:
        List[str]: Lista de partições no formato `YYYY-MM-DD`.
    """
    # Log consolidado da verificação de staging
    logger.info(f"🔍 VERIFICANDO STAGING: {bucket_name}/{prefix}")
    logger.info(
        f"   🔗 Console: https://console.cloud.google.com/storage/browser/{bucket_name}/staging/{dataset_id}/{table_id}",
    )

    staging_partitions = read_staging_dates(bucket_name=bucket_name, prefix=prefix)

    logger.info(f"   📊 Resultado: {len(staging_partitions)} partições únicas")
    return staging_partitions


def get_files_to_ingest(prefix: str, partitions: List[str], bucket_name: str) -> List[str]:
    """
    Identifica arquivos ZIP novos na área raw que ainda não foram processados.

    Args:
        prefix (str): Prefixo do caminho na área raw.
        partitions (List[str]): Lista de partições já processadas.
        bucket_name (str): Nome do bucket GCS.

    Returns:
        List[str]: Lista de nomes de blobs para ingerir.
    """
    # Log consolidado da verificação de raw
    logger.info(f"🔍 VERIFICANDO RAW: {bucket_name}/{prefix}")
    logger.info(f"   🔗 Console: https://console.cloud.google.com/storage/browser/{bucket_name}/{prefix}")

    raw_blobs = list_blobs_with_prefix(bucket_name=bucket_name, prefix=prefix)
    raw_blobs = [blob for blob in raw_blobs if blob.name and blob.name.lower().endswith(".zip")]

    # Extract partition information from blobs
    raw_partitions = []
    raw_partitions_blobs = []
    parsing_errors = []

    for blob in raw_blobs:
        if not blob.name:
            continue
        try:
            raw_partitions.append(parse_partition_from_filename(blob.name))
            raw_partitions_blobs.append(blob)
        except Exception as e:
            parsing_errors.append(f"{blob.name}: {str(e)[:50]}")

    # Filter files that are not in the staging area
    files_to_ingest = []
    for blob, partition in zip(raw_partitions_blobs, raw_partitions, strict=False):
        if partition not in partitions:
            files_to_ingest.append(blob.name)

    # Log consolidado dos resultados
    logger.info(
        f"   📊 Resultado: {len(raw_blobs)} ZIPs → {len(raw_partitions)} partições válidas → {len(files_to_ingest)} para ingerir",
    )

    if parsing_errors:
        logger.warning(f"   ⚠️  Erros de parsing ({len(parsing_errors)}): {parsing_errors[:3]}")

    # Usar função de comparação consolidada se há arquivos para ingerir
    if files_to_ingest:
        staging_partitions = partitions
        log_partition_comparison(staging_partitions, raw_partitions, files_to_ingest)

    return files_to_ingest


def need_to_ingest(files_to_ingest: list) -> bool:
    """Verifica se existem arquivos para ingerir com log consolidado"""
    need_ingest = len(files_to_ingest) > 0
    status = "✅ SIM" if need_ingest else "❌ NÃO"
    logger.info(f"{status} - Necessário ingerir: {len(files_to_ingest)} arquivo(s)")
    return need_ingest


def ingest_files(
    files_to_ingest: List[str],
    bucket_name: str,
    dataset_id: str,
    table_id: str,
    cloud: Cloud,
    max_concurrent: int = 3,
) -> None:
    """
    Processa múltiplos arquivos ZIP de forma assíncrona com controle de concorrência.

    Args:
        files_to_ingest (List[str]): Lista de nomes de blobs para ingerir.
        bucket_name (str): Nome do bucket GCS.
        dataset_id (str): ID do dataset de destino.
        table_id (str): ID da tabela de destino.
        cloud (Cloud): O bucket e o projeto da tabela de destino.
        max_concurrent (int): Número máximo de downloads/processamentos simultâneos.
    """
    if not files_to_ingest:
        logger.info("❌ Nenhum arquivo para ingerir")
        return

    processing_results = []

    async def _run_async():
        logger.info(
            f"🚀 INICIANDO INGESTÃO ASSÍNCRONA: {len(files_to_ingest)} arquivo(s) | Concorrência: {max_concurrent}",
        )

        semaphore = asyncio.Semaphore(max_concurrent)

        async def _run_with_semaphore(blob_name):
            file_start_time = datetime.now()
            success = True
            error_msg = None

            async with semaphore:
                try:
                    await run_sync_in_worker_thread(
                        ingest_file_sync,
                        blob_name,
                        bucket_name,
                        dataset_id,
                        table_id,
                        cloud,
                    )
                except Exception as e:
                    success = False
                    error_msg = str(e)

            # Coletar métricas do processamento
            duration = (datetime.now() - file_start_time).total_seconds()
            processing_results.append(
                {
                    "file_name": blob_name.split("/")[-1],
                    "success": success,
                    "duration_seconds": duration,
                    "error": error_msg,
                    "partition": parse_partition_from_filename(blob_name) if success else None,
                    "size_mb": 0,  # Placeholder - seria ideal capturar do processo
                }
            )

            return success

        tasks = []
        for blob_name in files_to_ingest:
            task = _run_with_semaphore(blob_name)
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(_run_async())

    # Log consolidado final
    log_ingestion_summary(files_to_ingest, max_concurrent, processing_results)
