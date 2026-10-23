"""Utilitários do domínio CRF — arquivos FWF compactados em ZIP."""

import io
import re
import zipfile
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud.storage import Bucket
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import to_partitions

from pipelines.rj_smfp__crf.utils.shared import (
    EXTRACT_BASE_PATH,
    cleanup_extracted_directory,
    get_gcs_bucket,
)

# ---------------------------------------------------------------------------
# Configuração de layout FWF
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FwfTableConfig:
    """Configuração de leitura de uma tabela em formato de largura fixa (FWF).

    :param colspecs: Posições das colunas como tuplas ``(início, fim)``.
    :param names: Nomes das colunas, na mesma ordem de ``colspecs``.
    :param file_pattern: Padrão glob do arquivo dentro do ZIP.
    :param encoding: Codificação do arquivo.
    """

    colspecs: list[tuple[int, int]]
    names: list[str]
    file_pattern: str = "*.txt"
    encoding: str = "utf-8"


FWF_PERIODOS_CONFIG = FwfTableConfig(
    colspecs=[
        (0, 8),   # CNPJ
        (8, 16),  # Data início
        (16, 24), # Data fim
        (24, 25), # Identificador cancelamento
        (25, 34), # Número opção
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

FWF_PERIODOS_MEI_CONFIG = replace(FWF_PERIODOS_CONFIG, file_pattern="00-PERMEI-*.txt")

FWF_EVENTOS_CONFIG = FwfTableConfig(
    colspecs=[
        (0, 8),     # CNPJ
        (8, 9),     # Natureza do evento
        (9, 12),    # Código do evento
        (12, 20),   # Data do fato motivador
        (20, 28),   # Data efeito
        (28, 78),   # Número do processo judicial
        (78, 103),  # Número do processo administrativo
        (103, 353), # Observações
        (353, 360), # Código UA
        (360, 362), # Código UF
        (362, 366), # Código Município
        (366, 374), # Data de ocorrência
        (374, 380), # Hora de ocorrência
        (380, 389), # Número da Opção
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

FWF_EVENTOS_MEI_CONFIG = replace(FWF_EVENTOS_CONFIG, file_pattern="00-EVEMEI-*.txt")

FWF_CONFIGS: dict[str, FwfTableConfig] = {
    "periodos_simples": FWF_PERIODOS_CONFIG,
    "periodos_mei":     FWF_PERIODOS_MEI_CONFIG,
    "eventos_simples":  FWF_EVENTOS_CONFIG,
    "eventos_mei":      FWF_EVENTOS_MEI_CONFIG,
}


# ---------------------------------------------------------------------------
# Dataclass de configuração de execução
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CrfConfig:
    """Configuração de execução do pipeline CRF.

    :param project_id: Google Cloud project ID.
    :param bucket_name: Nome do bucket GCS contendo os ZIPs.
    :param folder_prefix: Prefixo da pasta no bucket.
    :param table_id: Identificador da tabela CRF a processar.
    :param data_inicio: Data de início do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com data igual ou anterior são ignorados.
    :param data_fim: Data de fim do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com data posterior são ignorados.
    :param extract_base_path: Diretório base local para extração e saída parquet.
    """

    project_id: str
    bucket_name: str
    folder_prefix: str
    table_id: str
    data_inicio: date | str | None = None
    data_fim: date | str | None = None
    extract_base_path: str = EXTRACT_BASE_PATH


# ---------------------------------------------------------------------------
# Funções
# ---------------------------------------------------------------------------

def extract_date_from_blob_name(blob_name: str) -> date | None:
    """Extrai a data do nome de um arquivo ZIP CRF.

    Procura pelo padrão ``DDMMYYYY`` no nome do arquivo.
    Exemplo: ``BX-22518948-EVE-03012021.zip`` → ``date(2021, 1, 3)``.

    :param blob_name: Nome do blob no GCS (pode incluir prefixo de pasta).
    :returns: Data extraída como ``datetime.date`` ou ``None`` se o padrão não
        for encontrado ou a data for inválida.
    """
    filename = blob_name.split("/")[-1]
    match = re.search(r"-(\d{8})\.zip$", filename, re.IGNORECASE)
    if match:
        date_str = match.group(1)
        try:
            day = int(date_str[0:2])
            month = int(date_str[2:4])
            year = int(date_str[4:8])
            return date(year, month, day)
        except (ValueError, TypeError):
            log(f"CRF: data inválida no nome do arquivo '{blob_name}': {date_str!r}", level="warning")
    return None


def list_zip_files_in_gcs_folder(
    bucket: Bucket,
    folder_prefix: str,
    data_inicio: date | str | None = None,
    data_fim: date | str | None = None,
) -> list[str]:
    """Lista arquivos ZIP no GCS filtrando por intervalo de datas.

    Lista todos os blobs ZIP com prefixo ``BX-`` na pasta especificada e retorna
    apenas aqueles cuja data extraída do nome do arquivo está dentro do
    intervalo ``(data_inicio, data_fim]``.

    :param bucket: Google Cloud Storage bucket object.
    :param folder_prefix: Prefixo do caminho da pasta no bucket.
    :param data_inicio: Início do intervalo como ``datetime.date`` ou ``YYYY-MM-DD``.
        Arquivos com data igual ou anterior são ignorados.
    :param data_fim: Fim do intervalo como ``datetime.date`` ou ``YYYY-MM-DD``.
        Arquivos com data posterior são ignorados.
    :returns: Lista de nomes de blob filtrados.
    """

    def parse_date(value: date | str | None, param_name: str) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            raise ValueError(
                f"Formato de data inválido para '{param_name}': '{value}'. Use o formato YYYY-MM-DD."
            )

    data_inicio = parse_date(data_inicio, "data_inicio")
    data_fim = parse_date(data_fim, "data_fim")

    blobs = bucket.list_blobs(prefix=folder_prefix)
    zip_blobs = [
        blob.name
        for blob in blobs
        if blob.name.split("/")[-1].startswith("BX-") and blob.name.endswith(".zip")
    ]

    if data_inicio is None and data_fim is None:
        log(f"CRF: {len(zip_blobs)} ZIP(s) encontrados em '{folder_prefix}' (sem filtro de data)")
        return zip_blobs

    filtered = []
    sem_data = 0
    for blob_name in zip_blobs:
        file_date = extract_date_from_blob_name(blob_name)
        if file_date is None:
            sem_data += 1
            continue
        if data_inicio is not None and file_date <= data_inicio:
            continue
        if data_fim is not None and file_date > data_fim:
            continue
        filtered.append(blob_name)

    if sem_data:
        log(f"CRF: {sem_data} arquivo(s) ignorados — data não legível no nome", level="warning")
    log(f"CRF: {len(filtered)} de {len(zip_blobs)} ZIP(s) no intervalo ({data_inicio}, {data_fim}]")
    return filtered


def download_and_extract_zip_from_gcs(bucket: Bucket, blob_name: str, extract_path: str) -> str:
    """Baixa um ZIP do GCS para memória e extrai o conteúdo localmente.

    :param bucket: Google Cloud Storage bucket object.
    :param blob_name: Caminho completo do blob no bucket.
    :param extract_path: Diretório local de extração.
    :returns: Caminho do diretório de extração.
    :raises FileNotFoundError: Se o blob não existir no bucket.
    :raises zipfile.BadZipFile: Se o arquivo baixado não for um ZIP válido.
    """
    blob = bucket.blob(blob_name)

    if not blob.exists():
        raise FileNotFoundError(f"Blob '{blob_name}' not found in bucket '{bucket.name}'")

    zip_content = blob.download_as_bytes()

    extract_dir = Path(extract_path)
    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            zip_file.extractall(extract_dir)
    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(f"File '{blob_name}' is not a valid ZIP file") from e

    return str(extract_dir)


def list_extracted_files(extract_path: str) -> list[str]:
    """Lista todos os arquivos dentro de um diretório de extração.

    :param extract_path: Caminho do diretório.
    :returns: Lista de caminhos relativos dos arquivos.
    """
    extract_dir = Path(extract_path)

    if not extract_dir.exists():
        return []

    return [
        str(file.relative_to(extract_dir))
        for file in extract_dir.rglob("*")
        if file.is_file()
    ]


def read_extracted_fwf_file(
    extract_path: str,
    table_id: str,
    extract_base_path: str,
) -> tuple[pd.DataFrame, str]:
    """Lê um arquivo FWF de um diretório descompactado e salva como parquet particionado.

    Busca o arquivo correspondente ao padrão do ``table_id``, lê com as
    especificações de coluna de ``FWF_CONFIGS`` e adiciona as colunas
    ``arquivo_origem``, ``data_referencia``, ``ano_particao``, ``mes_particao``
    e ``data_particao``.

    :param extract_path: Caminho do diretório contendo os arquivos descompactados.
    :param table_id: Identificador da tabela. Valores válidos: ``'periodos_simples'``,
        ``'periodos_mei'``, ``'eventos_simples'``, ``'eventos_mei'``.
    :param extract_base_path: Diretório base de saída para os parquets.
    :returns: Tupla ``(DataFrame, caminho_parquet)``.
    :raises FileNotFoundError: Se nenhum arquivo for encontrado no diretório.
    :raises ValueError: Se múltiplos arquivos forem encontrados ou ``table_id`` for inválido.
    :raises pd.errors.ParserError: Se houver erro ao fazer parsing do arquivo.
    """
    if table_id not in FWF_CONFIGS:
        raise ValueError(f"table_id '{table_id}' inválido. Válidos: {', '.join(FWF_CONFIGS)}")

    config = FWF_CONFIGS[table_id]
    extract_dir = Path(extract_path)

    if not extract_dir.exists():
        raise FileNotFoundError(f"Diretório de descompactação não encontrado: {extract_path}")

    matching_files = list(extract_dir.glob(config.file_pattern))

    if not matching_files:
        raise FileNotFoundError(
            f"Nenhum arquivo com padrão '{config.file_pattern}' encontrado em {extract_path}"
        )

    if len(matching_files) > 1:
        raise ValueError(
            f"Múltiplos arquivos com padrão '{config.file_pattern}' encontrados em {extract_path}: "
            f"{[f.name for f in matching_files]}"
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
    except Exception as e:
        raise pd.errors.ParserError(
            f"Erro ao fazer parsing do arquivo FWF '{file_path}': {e}"
        ) from e

    arquivo_nome = file_path.name
    df["arquivo_origem"] = arquivo_nome

    nome_sem_ext = arquivo_nome.replace(".txt", "").replace(".TXT", "")
    partes = nome_sem_ext.split("-")
    if len(partes) < 3:
        raise ValueError(
            f"Nome do arquivo '{arquivo_nome}' não segue o padrão esperado "
            "'XX-XXX-AAAAMMDD.txt' — não foi possível extrair a data de referência."
        )

    data_referencia = partes[-1]
    df["data_referencia"] = data_referencia
    df["ano_particao"] = df["data_referencia"].str[0:4]
    df["mes_particao"] = df["data_referencia"].str[4:6]
    df["data_particao"] = pd.to_datetime(df["data_referencia"], format="%Y%m%d").dt.strftime("%Y-%m-%d")

    data_path = f"{extract_base_path}/{table_id}"
    to_partitions(
        data=df,
        savepath=data_path,
        data_type="parquet",
        partition_columns=["ano_particao", "mes_particao", "data_particao"],
    )

    return df, data_path


def process_crf_zip_files(config: CrfConfig) -> str | None:
    """Processa sequencialmente todos os arquivos ZIP CRF do GCS.

    Realiza o ciclo completo para cada arquivo ZIP dentro do intervalo de datas:
    1. Listar ZIPs no GCS filtrados pelo intervalo ``(data_inicio, data_fim]``.
    2. Baixar e descompactar o ZIP localmente.
    3. Ler o arquivo FWF e salvar como parquet particionado.
    4. Limpar o diretório descompactado em ``try/finally``.

    :param config: Instância de ``CrfConfig`` com todos os parâmetros de execução.
    :returns: Caminho local do diretório parquet ou ``None`` se não houver arquivos.
    """
    bucket = get_gcs_bucket(config.project_id, config.bucket_name)

    zip_files = list_zip_files_in_gcs_folder(
        bucket=bucket,
        folder_prefix=config.folder_prefix,
        data_inicio=config.data_inicio,
        data_fim=config.data_fim,
    )

    if not zip_files:
        log(f"CRF: nenhum ZIP encontrado para '{config.table_id}' no intervalo especificado", level="warning")
        return None

    log(f"CRF: processando {len(zip_files)} ZIP(s) para '{config.table_id}'")

    total = 0
    data_path = None

    for blob_name in zip_files:
        zip_filename = blob_name.split("/")[-1].replace(".zip", "")
        extract_path = f"{config.extract_base_path}/{zip_filename}"

        try:
            extracted_dir = download_and_extract_zip_from_gcs(bucket, blob_name, extract_path)
            df, data_path = read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id=config.table_id,
                extract_base_path=config.extract_base_path,
            )
            total += len(df)
            log(f"CRF: {zip_filename} → {len(df)} linhas")
        finally:
            cleanup_extracted_directory(extract_path=extract_path)

    log(f"CRF: '{config.table_id}' concluído — {total} linhas no total")
    return data_path
