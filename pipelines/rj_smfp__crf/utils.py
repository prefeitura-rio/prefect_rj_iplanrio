"""Utility functions for GCS ZIP file handling and extraction."""

import io
import zipfile
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import pandas as pd
from google.cloud.storage import Client, Bucket
from iplanrio.pipelines_utils.pandas import to_partitions
from tomlkit import date

def list_zip_files_in_gcs_folder(
    bucket: Bucket, folder_prefix: str
) -> list[str]:
    """List all ZIP files in a GCS folder.

    :param bucket: Google Cloud Storage bucket object.
    :param folder_prefix: Folder path prefix in the bucket (e.g., 'data/crf/').
    :returns: List of blob names (full paths) for ZIP files found.
    """
    zip_blobs = []
    blobs = bucket.list_blobs(prefix=folder_prefix)
    for blob in blobs:
        # if blob.name.startswith(folder_prefix) and blob.name.endswith(".zip"):
            if blob.name == "PERIODOS_EVENTOS/BX-22518948-EVE-03012021.zip":
                zip_blobs.append(blob.name)
    return zip_blobs


def download_and_extract_zip_from_gcs(
    bucket: Bucket, blob_name: str, extract_path: str
) -> str:
    """Download a ZIP file from GCS and extract its contents locally.

    Downloads the ZIP blob from Google Cloud Storage to memory, extracts all
    files to the specified directory, and returns the extraction directory path.

    :param bucket: Google Cloud Storage bucket object.
    :param blob_name: Full blob name/path in the bucket (e.g., 'data/crf/file.zip').
    :param extract_path: Local directory path where files will be extracted.
    :returns: Path to the extraction directory.
    :raises FileNotFoundError: If the blob does not exist in the bucket.
    :raises zipfile.BadZipFile: If the downloaded file is not a valid ZIP.
    """
    blob = bucket.blob(blob_name)

    if not blob.exists():
        raise FileNotFoundError(
            f"Blob '{blob_name}' not found in bucket '{bucket.name}'"
        )

    # Download to memory
    zip_content = blob.download_as_bytes()

    # Extract to local directory
    extract_dir = Path(extract_path)
    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            zip_file.extractall(extract_dir)
    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(f"File '{blob_name}' is not a valid ZIP file") from e

    return str(extract_dir)


def list_extracted_files(extract_path: str) -> list[str]:
    """List all files extracted to a directory.

    :param extract_path: Path to the directory containing extracted files.
    :returns: List of relative file paths (relative to extract_path).
    """
    extract_dir = Path(extract_path)

    if not extract_dir.exists():
        return []

    files = []
    for file in extract_dir.rglob("*"):
        if file.is_file():
            relative_path = file.relative_to(extract_dir)
            files.append(str(relative_path))

    return files


def get_gcs_bucket(project_id: str, bucket_name: str) -> Bucket:
    """Get a GCS bucket object.

    :param project_id: Google Cloud project ID.
    :param bucket_name: Name of the bucket.
    :returns: Google Cloud Storage Bucket object.
    """
    client = Client(project=project_id)
    bucket = client.bucket(bucket_name)
    return bucket


@dataclass(frozen=True)
class FwfTableConfig:
    """Configuration for reading a fixed-width format (FWF) table.

    Stores column specifications and names for a specific FWF data file
    format used in CRF (Cadastro de Recursos Financeiros) processing.
    """

    table_id: str
    """Table identifier (e.g., 'periodos', 'eventos', 'eventos_mei')."""

    colspecs: list[tuple[int, int]]
    """Column position specifications as (start_col, end_col) tuples."""

    names: list[str]
    """Column names in the same order as colspecs."""

    file_pattern: str = "*.txt"
    """Glob pattern to find the file (default: '*.txt')."""

    encoding: str = "utf-8"
    """File encoding (default: 'utf-8')."""


# FWF table configurations for CRF data files
# Padrão de nome: 00-PER-AAAAMMDD.txt (períodos), 00-PERMEI-AAAAMMDD.txt (períodos MEI),
# 00-EVE-AAAAMMDD.txt (eventos), 00-EVEMEI-AAAAMMDD.txt (eventos MEI)

FWF_PERIODOS_CONFIG = FwfTableConfig(
    table_id="periodos",
    colspecs=[
        (0, 8),    # CNPJ
        (8, 16),   # Data início
        (16, 24),  # Data fim
        (24, 25),  # Identificador cancelamento
        (25, 34),  # Número opção
    ],
    names=[
        "cnpj",
        "data_inicio",
        "data_fim",
        "identificador_cancelamento",
        "numero_opcao",
    ],
    file_pattern="00-PER-*.txt",
)

FWF_PERIODOS_MEI_CONFIG = FwfTableConfig(
    table_id="periodos_mei",
    colspecs=[
        (0, 8),    # CNPJ
        (8, 16),   # Data início
        (16, 24),  # Data fim
        (24, 25),  # Identificador cancelamento
        (25, 34),  # Número opção
    ],
    names=[
        "cnpj",
        "data_inicio",
        "data_fim",
        "identificador_cancelamento",
        "numero_opcao",
    ],
    file_pattern="00-PERMEI-*.txt",
)

FWF_EVENTOS_CONFIG = FwfTableConfig(
    table_id="eventos",
    colspecs=[
        (0, 8),      # CNPJ
        (8, 9),      # Natureza do evento
        (9, 12),     # Código do evento
        (12, 20),    # Data do fato motivador
        (20, 28),    # Data efeito
        (28, 78),    # Número do processo judicial
        (78, 103),   # Número do processo administrativo
        (103, 353),  # Observações
        (353, 360),  # Código UA
        (360, 362),  # Código UF
        (362, 366),  # Código Município
        (366, 374),  # Data de ocorrência
        (374, 380),  # Hora de ocorrência
        (380, 389),  # Número da Opção
    ],
    names=[
        "cnpj",
        "natureza_evento",
        "codigo_evento",
        "data_fato_motivador",
        "data_efeito",
        "numero_processo_judicial",
        "numero_processo_administrativo",
        "observacoes",
        "codigo_ua",
        "codigo_uf",
        "codigo_municipio",
        "data_ocorrencia",
        "hora_ocorrencia",
        "numero_opcao",
    ],
    file_pattern="00-EVE-*.txt",
)

FWF_EVENTOS_MEI_CONFIG = FwfTableConfig(
    table_id="eventos_mei",
    colspecs=[
        (0, 8),      # CNPJ
        (8, 9),      # Natureza do evento
        (9, 12),     # Código do evento
        (12, 20),    # Data do fato motivador
        (20, 28),    # Data efeito
        (28, 78),    # Número do processo judicial
        (78, 103),   # Número do processo administrativo
        (103, 353),  # Observações
        (353, 360),  # Código UA
        (360, 362),  # Código UF
        (362, 366),  # Código Município
        (366, 374),  # Data de ocorrência
        (374, 380),  # Hora de ocorrência
        (380, 389),  # Número da Opção
    ],
    names=[
        "cnpj",
        "natureza_evento",
        "codigo_evento",
        "data_fato_motivador",
        "data_efeito",
        "numero_processo_judicial",
        "numero_processo_administrativo",
        "observacoes",
        "codigo_ua",
        "codigo_uf",
        "codigo_municipio",
        "data_ocorrencia",
        "hora_ocorrencia",
        "numero_opcao",
    ],
    file_pattern="00-EVEMEI-*.txt",
)


def read_extracted_fwf_file(
    extract_path: str,
    table_id: str,
    extract_base_path
) -> pd.DataFrame:
    """Lê um arquivo de formato de largura fixa (FWF) de um diretório descompactado.

    Busca um arquivo correspondente ao padrão de arquivo do `table_id` no
    diretório de descompactação e o lê usando as especificações de coluna
    pré-definidas para a tabela. Adiciona automaticamente as colunas
    `arquivo_origem` (nome completo do arquivo) e `data_referencia` (data extraída
    do nome do arquivo no formato AAAAMMDD).

    :param extract_path: Caminho do diretório contendo os arquivos descompactados.
    :param table_id: Identificador da tabela. Valores válidos: 'periodos',
        'periodos_mei', 'eventos', 'eventos_mei'.
    :returns: DataFrame contendo os dados do arquivo FWF parseado com colunas
        adicionais `arquivo_origem` e `data_referencia`.
    :raises FileNotFoundError: Se nenhum arquivo for encontrado no diretório.
    :raises ValueError: Se múltiplos arquivos forem encontrados ou table_id inválido.
    :raises pd.errors.ParserError: Se houver erro ao fazer parsing do arquivo.
    """
    # Mapa de configurações por table_id
    config_map = {
        "periodos": FWF_PERIODOS_CONFIG,
        "periodos_mei": FWF_PERIODOS_MEI_CONFIG,
        "eventos": FWF_EVENTOS_CONFIG,
        "eventos_mei": FWF_EVENTOS_MEI_CONFIG,
    }

    if table_id not in config_map:
        raise ValueError(
            f"table_id '{table_id}' inválido. "
            f"Válidos: {', '.join(config_map.keys())}"
        )

    config = config_map[table_id]
    extract_dir = Path(extract_path)

    if not extract_dir.exists():
        raise FileNotFoundError(
            f"Diretório de descompactação não encontrado: {extract_path}"
        )

    # Busca arquivos correspondentes ao padrão
    matching_files = list(extract_dir.glob(config.file_pattern))

    if not matching_files:
        raise FileNotFoundError(
            f"Nenhum arquivo com padrão '{config.file_pattern}' encontrado em "
            f"{extract_path} para a tabela '{table_id}'"
        )

    if len(matching_files) > 1:
        raise ValueError(
            f"Múltiplos arquivos com padrão '{config.file_pattern}' encontrados "
            f"em {extract_path} para a tabela '{table_id}'. "
            f"Arquivos encontrados: {[f.name for f in matching_files]}"
        )

    file_path = matching_files[0]

    try:
        df = pd.read_fwf(
            file_path,
            colspecs=config.colspecs,
            names=config.names,
            dtype=str,
            encoding=config.encoding,
        )

        # Adicionar coluna com nome completo do arquivo
        arquivo_nome = file_path.name
        df["arquivo_origem"] = arquivo_nome

        # Extrair data de referência do nome do arquivo (formato: 00-XXX-AAAAMMDD.txt)
        # Extrai apenas os 8 dígitos da data (AAAAMMDD)
        nome_sem_ext = arquivo_nome.replace(".txt", "").replace(".TXT", "")
        partes = nome_sem_ext.split("-")
        if len(partes) >= 3:
            data_referencia = partes[-1]
            df["data_referencia"] = data_referencia

            df['ano'] = df["data_referencia"].apply(lambda x: str(x)[0:4])[0]
            df['mes'] = df["data_referencia"].apply(lambda x: str(x)[4:6])[0]
            df['dia'] = df["data_referencia"].apply(lambda x: str(x)[6:8])[0]


            to_partitions(
            data=df,
            savepath=f"{extract_base_path}/{table_id}",
            data_type="parquet",
            partition_columns=["ano", "mes", "dia"],
        )
        return df

    except Exception as e:
        raise pd.errors.ParserError(
            f"Erro ao fazer parsing do arquivo FWF '{file_path}' para "
            f"a tabela '{table_id}': {e}"
        ) from e


def cleanup_extracted_directory(extract_path: str) -> None:
    """Remove todos os arquivos de um diretório descompactado.

    Deleta recursivamente todos os arquivos no diretório, útil para limpeza
    após o processamento de um lote de arquivos FWF.

    :param extract_path: Caminho do diretório a ser limpo.
    :raises FileNotFoundError: Se o diretório não existir.
    :raises Exception: Se houver erro ao remover arquivos.
    """
    extract_dir = Path(extract_path)

    if not extract_dir.exists():
        raise FileNotFoundError(
            f"Diretório não encontrado: {extract_path}"
        )

    try:
        for file_path in extract_dir.rglob("*"):
            if file_path.is_file():
                file_path.unlink()
    except Exception as e:
        raise Exception(
            f"Erro ao limpar diretório {extract_path}: {e}"
        ) from e
