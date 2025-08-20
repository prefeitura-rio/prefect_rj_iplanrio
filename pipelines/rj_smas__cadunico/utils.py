# -*- coding: utf-8 -*-
# ruff: noqa: DTZ007,PLR2004,E501,PLR0915
import asyncio
import shutil
from datetime import datetime
from os import system
from pathlib import Path
from typing import List
from uuid import uuid4
from zipfile import ZipFile

import basedosdados as bd
import pandas as pd
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs
from iplanrio.pipelines_utils.gcs import get_gcs_client
from iplanrio.pipelines_utils.logging import log


def parse_partition_from_filename(blob_name: str) -> str:
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

    raise ValueError(f"No partition info found in blob name: {blob_name}")


def parse_txt_first_line(filepath):
    with open(filepath) as f:  # noqa
        first_line = f.readline()
    txt_layout_version = first_line[69:74].strip().replace(".", "")
    dta_extracao_dados_hdr = first_line[82:90].strip()
    txt_date = datetime.strptime(dta_extracao_dados_hdr, "%d%m%Y").strftime("%Y-%m-%d")
    return txt_layout_version, txt_date


def create_table_if_not_exists(
    dataset_id: str,
    table_id: str,
    biglake_table: bool = True,
) -> bool:
    """
    Create table using BD+ .

    Args:
        data_path (str | Path): The path to the data.
        dataset_id (str): The dataset ID.
        table_id (str): The table ID.
        biglake_table (bool): Whether to create a BigLake table.
    """

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    table_exists = tb.table_exists(mode="staging")

    if not table_exists:
        mock_data_path = Path("/tmp/mock_data/")
        partition_data_path_file = Path(
            "versao_layout_particao=XXXX/ano_particao=1970/mes_particao=1/data_particao=1970-01-01/delete_this_data.csv"
        )
        mock_data_path_partition_file = mock_data_path / partition_data_path_file
        mock_data_path_partition_file.parent.mkdir(parents=True, exist_ok=True)

        # create mock data csv
        data = {"text": ["delete_this_data"]}
        pd.DataFrame(data).to_csv(mock_data_path_partition_file, index=False)

        # create table
        tb.create(
            path=mock_data_path,
            csv_delimiter="Æ",
            csv_skip_leading_rows=0,
            csv_allow_jagged_rows=False,
            if_storage_data_exists="replace",
            biglake_table=biglake_table,
        )
        log(f"SUCESSFULLY CREATED TABLE: {dataset_id}.{table_id}")
        # delete data from storage
        st.delete_file(
            filename=str(partition_data_path_file),
            mode="staging",
        )
        log(
            f"SUCESSFULLY DELETED DATA FROM STORAGE: staging/{dataset_id}/{table_id}/{str(partition_data_path_file)}"  # noqa
        )
    else:
        log(f"TABLE ALREADY EXISTS: {dataset_id}.{table_id}")

    return table_exists


def append_data_to_storage(
    data_path: str | Path,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    biglake_table: bool = True,
) -> str:
    """
    Upload to GCS.

    Args:
        data_path (str | Path): The path to the data.
        dataset_id (str): The dataset ID.
        table_id (str): The table ID.
        dump_mode (str): The dump mode.
        biglake_table (bool): Whether to create a BigLake table.
    """
    create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    return dataset_id


async def ingest_file(blob_name: str, bucket_name: str, dataset_id: str, table_id: str) -> None:
    """
    Processa um arquivo ZIP: baixa, extrai, divide em chunks, converte para CSV e faz upload.

    Args:
        blob_name (str): Nome do blob para ingerir.
        bucket_name (str): Nome do bucket GCS.
        dataset_id (str): ID do dataset de destino.
        table_id (str): ID da tabela de destino.
    """
    # Criar ID único para tracking deste arquivo
    partition = parse_partition_from_filename(blob_name)
    file_id = partition

    file_short_name = blob_name.split("/")[-1]

    log(f"[{file_id}] INICIANDO processamento de {file_short_name}")

    # Criar diretório temporário único para este arquivo
    output_directory_path: Path = Path("/tmp") / f"ingestion_{uuid4()}"
    output_directory_path.mkdir(parents=True, exist_ok=True)

    # Create temporary directory for file
    temp_directory: Path = Path("/tmp") / str(uuid4())
    temp_directory.mkdir(parents=True, exist_ok=True)

    log(f"[{file_id}] ETAPA 1/6: Iniciando download de {file_short_name}")
    # Download blob to temporary directory
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    fname = str(temp_directory / blob_name.rpartition("/")[-1])
    blob.download_to_filename(fname, client=gcs_client)

    file_size_mb = Path(fname).stat().st_size / (1024 * 1024)
    log(f"[{file_id}] ✓ Download concluído - {file_short_name} ({file_size_mb:.1f} MB)")

    # Unzip file
    log(f"[{file_id}] ETAPA 2/6: Extraindo arquivos do ZIP")
    unzip_output_directory = temp_directory / "output"
    unzip_output_directory.mkdir(parents=True, exist_ok=True)
    with ZipFile(fname, "r") as zip_file:
        zip_file.extractall(unzip_output_directory)

    extracted_files = list(unzip_output_directory.glob("*"))
    log(f"[{file_id}] ✓ Extração concluída - {len(extracted_files)} arquivo(s) extraído(s)")

    # List TXT files (non-case sensitive)
    txt_files = list(unzip_output_directory.glob("*.txt")) + list(unzip_output_directory.glob("*.TXT"))
    log(f"[{file_id}] Encontrados {len(txt_files)} arquivo(s) TXT para processamento")

    # Split TXT files into chunks of 1GB
    log(f"[{file_id}] ETAPA 3/6: Analisando e dividindo arquivos TXT")
    txt_layout_version = None
    txt_date = None
    txt_files_after_split: List[Path] = []
    total_size_gb = 0

    for i, txt_file in enumerate(txt_files, 1):
        txt_layout_version, txt_date = parse_txt_first_line(filepath=txt_file)
        txt_file_size = txt_file.stat().st_size
        txt_file_size_gb = txt_file_size / (1024**3)
        total_size_gb += txt_file_size_gb

        log(f"[{file_id}] Arquivo TXT {i}/{len(txt_files)}: {txt_file.name} ({txt_file_size_gb:.2f} GB)")
        log(f"[{file_id}] Layout version: {txt_layout_version}, Data: {txt_date}")

        if txt_file_size > 1e9:
            log(f"[{file_id}] Dividindo arquivo grande em chunks de 1GB...")
            system(f"split -b 1G {txt_file} {txt_file}.PART_")
            txt_file.unlink()
            parts = list(unzip_output_directory.glob("*.PART_*"))
            txt_files_after_split.extend(parts)
            log(f"[{file_id}] ✓ Arquivo dividido em {len(parts)} partes")
        else:
            log(f"[{file_id}] Arquivo menor que 1GB, mantendo inteiro")
            txt_files_after_split.append(txt_file)

    txt_files = txt_files_after_split
    log(
        f"[{file_id}] ✓ Processamento TXT concluído - {len(txt_files)} arquivo(s) final(is) ({total_size_gb:.2f} GB total)"
    )

    # Modify extension to CSV
    log(f"[{file_id}] ETAPA 4/6: Convertendo TXT para CSV")
    csv_files: List[Path] = []
    for txt_file in txt_files:
        csv_file = Path(str(txt_file) + ".csv")
        txt_file.rename(csv_file)
        csv_files.append(csv_file)
    log(f"[{file_id}] ✓ Conversão concluída - {len(csv_files)} arquivo(s) CSV criado(s)")

    # Create partition directories
    log(f"[{file_id}] ETAPA 5/6: Criando estrutura de partições")
    if partition == txt_date:
        log(f"[{file_id}] ✓ Partição validada: {partition} (consistente com data no TXT)")
    else:
        log(
            f"[{file_id}] ⚠ ATENÇÃO: Partição {partition} difere da data no TXT {txt_date}",
            "warning",
        )

    year, month, _ = partition.split("-")
    partition_directory = (
        output_directory_path
        / f"versao_layout_particao={txt_layout_version}"
        / f"ano_particao={int(year)}"
        / f"mes_particao={int(month)}"
        / f"data_particao={partition}"
    )
    partition_directory.mkdir(parents=True, exist_ok=True)

    log(f"[{file_id}] Estrutura criada: {partition_directory.relative_to(output_directory_path)}")

    # Move CSV files to partition directory
    log(f"[{file_id}] ETAPA 6/6: Movendo arquivos para estrutura final")
    total_csv_size = 0
    for i, csv_file in enumerate(csv_files, 1):
        csv_size_mb = csv_file.stat().st_size / (1024 * 1024)
        total_csv_size += csv_size_mb
        csv_file.rename(partition_directory / csv_file.name)
        log(f"[{file_id}] Movido arquivo {i}/{len(csv_files)}: {csv_file.name} ({csv_size_mb:.1f} MB)")

    log(f"[{file_id}] ✅ PROCESSAMENTO CONCLUÍDO - {file_short_name}")
    log(f"[{file_id}] 📊 Resumo: {len(csv_files)} arquivo(s) CSV, {total_csv_size:.1f} MB total, partição {partition}")

    # ETAPA 7: Upload para BigQuery/GCS
    log(f"[{file_id}] ETAPA 7/8: Criando tabela se necessário")
    await asyncio.to_thread(
        create_table_if_not_exists,
        dataset_id=dataset_id,
        table_id=table_id,
    )

    log(f"[{file_id}] ETAPA 8/8: Fazendo upload dos dados")
    await asyncio.to_thread(
        append_data_to_storage,
        data_path=output_directory_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
    )

    # Limpar diretórios temporários
    log(f"[{file_id}] 🧹 Limpando arquivos temporários...")

    shutil.rmtree(temp_directory, ignore_errors=True)
    shutil.rmtree(output_directory_path, ignore_errors=True)

    log(f"[{file_id}] ✅ UPLOAD E LIMPEZA CONCLUÍDOS - {file_short_name}")
    log(f"[{file_id}] 🎯 Partição {partition} disponível em {dataset_id}.{table_id}")


async def ingest_file_with_semaphore(
    semaphore: asyncio.Semaphore,
    blob_name: str,
    bucket_name: str,
    dataset_id: str,
    table_id: str,
) -> None:
    """
    Wrapper assíncrono para ingest_file com controle de semáforo.
    """
    async with semaphore:
        await ingest_file(blob_name, bucket_name, dataset_id, table_id)
