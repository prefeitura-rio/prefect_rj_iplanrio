# -*- coding: utf-8 -*-
"""Tasks do fluxo rj_iplanrio__sisbicho_images."""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
from basedosdados import Base
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_iplanrio__sisbicho_images.utils.tasks import (
    MAGIC_NUMBERS,
    detect_and_decode,
)


def _infer_identifier_field(schema: Iterable[bigquery.SchemaField]) -> str:
    """Identifica o campo que será usado como chave do animal."""

    preferred = (
        "animal_id",
        "id_animal",
        "id",
        "id_animal_prontuario",
        "codigo_animal",
        "codigo",
        "identificador",
    )
    available = {field.name.lower(): field.name for field in schema}

    for candidate in preferred:
        if candidate.lower() in available:
            return available[candidate.lower()]

    for field in schema:
        name = field.name.lower()
        if name not in {"qrcode_dados", "foto_dados"}:
            return field.name

    raise ValueError("Não foi possível identificar uma coluna para chave do animal.")


def _strip_data_uri_prefix(value: str) -> str:
    if "," in value and value.lower().startswith("data:"):
        return value.split(",", 1)[1]
    return value


def _coerce_to_base64_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("ascii")
        except UnicodeDecodeError as exc:
            msg = (
                "Valor de imagem em bytes não pôde ser decodificado como ASCII/Base64. "
                "Registro não atende ao formato esperado."
            )
            log(f"[ERRO CRÍTICO] {msg} Detalhes: {exc}")
            raise ValueError(msg) from exc
    return str(value)


def _looks_like_base64(value: str) -> bool:
    value = value.strip()
    if not value:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")
    return set(value) <= allowed and len(value) % 4 == 0


def _bytes_to_text(decoded_bytes: bytes) -> str | None:
    for encoding in ("utf-8", "latin-1"):
        try:
            return decoded_bytes.decode(encoding).strip()
        except UnicodeDecodeError:
            continue
    return None


def _contains_magic_number(decoded_bytes: bytes) -> bool:
    return any(decoded_bytes.startswith(magic) for magic in MAGIC_NUMBERS)


def _sanitize_identifier(identifier: object) -> str:
    text = str(identifier).strip()
    if not text:
        return "sem_identificador"
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in text)


def _extract_qrcode_payload(value: str) -> str | None:
    if value is None:
        return None

    candidate = _coerce_to_base64_text(value).strip()
    if not candidate:
        return None

    original = candidate
    for _ in range(2):
        cleaned = _strip_data_uri_prefix(candidate)
        if not _looks_like_base64(cleaned):
            break
        try:
            decoded = base64.b64decode(cleaned, validate=True)
        except ValueError:
            break

        if _contains_magic_number(decoded):
            return original

        decoded_text = _bytes_to_text(decoded)
        if not decoded_text:
            break
        candidate = decoded_text

    candidate = candidate.strip()
    if not candidate:
        return None

    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        return candidate

    if isinstance(parsed, dict):
        for key in ("payload", "qr_payload", "dados", "data"):
            if parsed.get(key):
                value = parsed[key]
                return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        return json.dumps(parsed, ensure_ascii=False)

    if isinstance(parsed, list):
        return json.dumps(parsed, ensure_ascii=False)

    return candidate


def _infer_extension(image_bytes: bytes) -> str:
    for magic, extension in MAGIC_NUMBERS.items():
        if image_bytes.startswith(magic):
            if extension == "jpeg":
                return "jpg"
            return extension
    return "bin"


def _extension_to_content_type(extension: str) -> str:
    mapping = {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "gif": "image/gif",
    }
    return mapping.get(extension.lower(), "application/octet-stream")


def _get_total_count(
    client: bigquery.Client,
    source_table: str,
    target_table: str,
    identifier_field: str,
) -> int:
    """Retorna o número total de registros com mídia disponível que ainda não foram processados."""

    # Verifica se a tabela de destino existe
    try:
        client.get_table(target_table)
        table_exists = True
    except NotFound:
        log(f"Tabela {target_table} não existe. Primeira execução: processando todos os registros.")
        table_exists = False

    if table_exists:
        # Query incremental com LEFT JOIN
        count_query = f"""
            SELECT COUNT(*) as total
            FROM `{source_table}` AS src
            LEFT JOIN `{target_table}` AS tgt
                ON src.{identifier_field} = tgt.id_animal
            WHERE (src.qrcode_dados IS NOT NULL OR src.foto_dados IS NOT NULL)
              AND tgt.id_animal IS NULL
        """
    else:
        # Query completa (primeira carga)
        count_query = f"""
            SELECT COUNT(*) as total
            FROM `{source_table}` AS src
            WHERE src.qrcode_dados IS NOT NULL OR src.foto_dados IS NOT NULL
        """

    result = client.query(count_query).result()
    return next(result).total


@task
def fetch_sisbicho_media_task(
    billing_project_id: str,
    credential_bucket: str,
    source_dataset_id: str,
    source_table_id: str,
    target_dataset_id: str,
    target_table_id: str,
    batch_size: int = 10,
    max_records: int | None = None,
) -> tuple[bigquery.Client, str, str, str, int]:
    """
    Prepara query para processar dados do SISBICHO em lotes.

    Retorna informações necessárias para processar em batches:
    - Cliente BigQuery configurado
    - Nome da tabela fonte (source)
    - Nome da tabela destino (target)
    - Nome do campo identificador
    - Total de registros a processar
    """
    credentials = Base(bucket_name=credential_bucket)._load_credentials(mode="prod")
    client = bigquery.Client(credentials=credentials, project=billing_project_id)

    source_table = f"{billing_project_id}.{source_dataset_id}.{source_table_id}"
    target_table = f"{billing_project_id}.{target_dataset_id}.{target_table_id}"

    table = client.get_table(source_table)
    identifier_field = _infer_identifier_field(table.schema)

    total_count = _get_total_count(client, source_table, target_table, identifier_field)

    if max_records:
        total_count = min(total_count, max_records)

    log(f"Total de registros a processar: {total_count}")
    log(f"Tamanho do lote: {batch_size}")

    return client, source_table, target_table, identifier_field, total_count


def fetch_batch(
    client: bigquery.Client,
    source_table: str,
    target_table: str,
    identifier_field: str,
    offset: int,
    batch_size: int,
) -> pd.DataFrame:
    """Busca um lote específico de dados do BigQuery, excluindo animais já processados."""

    # Verifica se a tabela de destino existe
    try:
        client.get_table(target_table)
        table_exists = True
    except NotFound:
        table_exists = False

    if table_exists:
        # Query incremental com LEFT JOIN
        query = f"""
            WITH animal_cpf AS (
                SELECT
                    p.id_animal,
                    pet_master.cpf
                FROM `rj-crm-registry.rmi_dados_mestres.pet` AS pet_master,
                UNNEST(pet_master.pet) AS p
            )
            SELECT
                src.{identifier_field} AS animal_identifier,
                animal_cpf.cpf AS cpf_proprietario,
                src.qrcode_dados,
                src.foto_dados
            FROM `{source_table}` AS src
            LEFT JOIN animal_cpf
                ON animal_cpf.id_animal = src.{identifier_field}
            LEFT JOIN `{target_table}` AS tgt
                ON src.{identifier_field} = tgt.id_animal
            WHERE (src.qrcode_dados IS NOT NULL OR src.foto_dados IS NOT NULL)
              AND tgt.id_animal IS NULL
            ORDER BY src.{identifier_field}
            LIMIT {batch_size}
            OFFSET {offset}
        """.strip()
    else:
        # Query completa (primeira carga)
        query = f"""
            WITH animal_cpf AS (
                SELECT
                    p.id_animal,
                    pet_master.cpf
                FROM `rj-crm-registry.rmi_dados_mestres.pet` AS pet_master,
                UNNEST(pet_master.pet) AS p
            )
            SELECT
                src.{identifier_field} AS animal_identifier,
                animal_cpf.cpf AS cpf_proprietario,
                src.qrcode_dados,
                src.foto_dados
            FROM `{source_table}` AS src
            LEFT JOIN animal_cpf
                ON animal_cpf.id_animal = src.{identifier_field}
            WHERE src.qrcode_dados IS NOT NULL OR src.foto_dados IS NOT NULL
            ORDER BY src.{identifier_field}
            LIMIT {batch_size}
            OFFSET {offset}
        """.strip()

    log(f"Buscando lote: offset={offset}, limit={batch_size}")

    job_config = bigquery.QueryJobConfig(
        use_query_cache=True,
        use_legacy_sql=False,
    )

    query_job = client.query(query, job_config=job_config)
    dataframe = query_job.result().to_dataframe()

    log(f"Lote carregado: {len(dataframe)} registros")
    return dataframe


@task
def extract_qrcode_payload_task(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Cria coluna com o payload do QRCode a partir da coluna codificada."""

    if dataframe.empty:
        return dataframe.assign(qrcode_payload=pd.Series(dtype="string"))

    df = dataframe.copy()
    df["qrcode_payload"] = df["qrcode_dados"].apply(_extract_qrcode_payload)

    return df


@task
def upload_pet_images_task(
    dataframe: pd.DataFrame,
    storage_bucket: str,
    storage_prefix: str,
    billing_project_id: str,
) -> pd.DataFrame:
    """Faz o upload das imagens dos pets para o GCS e retorna a URL final."""

    if dataframe.empty:
        return dataframe.assign(foto_url=pd.Series(dtype="string"), foto_blob_path=pd.Series(dtype="string"))

    storage_client = storage.Client(project=billing_project_id)
    bucket = storage_client.bucket(storage_bucket)

    foto_urls: list[str | None] = []
    blob_paths: list[str | None] = []

    for _, row in dataframe.iterrows():
        raw_value = row.get("foto_dados")
        identifier = row.get("animal_identifier")
        safe_identifier = _sanitize_identifier(identifier)

        if raw_value is None or str(raw_value).strip() == "":
            foto_urls.append(None)
            blob_paths.append(None)
            continue

        raw_text = _coerce_to_base64_text(raw_value)
        cleaned = _strip_data_uri_prefix(raw_text)

        raw_preview = raw_text.strip().replace("\n", " ")
        cleaned_preview = cleaned.strip().replace("\n", " ")
        log(
            f"[DEBUG] Preparando decode (task) animal={identifier} tipo={type(raw_value).__name__} "
            f"len_original={len(raw_text)} "
            f"len_limpo={len(cleaned)} preview_original={raw_preview[:60]} preview_limpo={cleaned_preview[:60]}"
        )

        try:
            image_bytes = detect_and_decode(cleaned)
        except ValueError as exc:
            log(f"[ERRO CRÍTICO] Falha ao decodificar Base64 do animal {identifier}: {exc}")
            raise ValueError(
                f"Decode de Base64 falhou para animal {identifier}. "
                f"Batch abortado para evitar transferência de dados incompletos."
            ) from exc

        extension = _infer_extension(image_bytes)
        content_type = _extension_to_content_type(extension)
        digest = hashlib.sha1(image_bytes).hexdigest()
        blob_name = f"{storage_prefix}/animal_id={safe_identifier}/{digest}.{extension}"
        blob = bucket.blob(blob_name)

        try:
            exists = blob.exists(storage_client)
        except NotFound:
            exists = False

        if not exists:
            log(f"Upload da imagem do animal {identifier} para gs://{storage_bucket}/{blob_name}")
            blob.upload_from_string(image_bytes, content_type=content_type)
            blob.metadata = {"sha1": digest}
            blob.patch()
        else:
            log(f"Imagem do animal {identifier} já existe em gs://{storage_bucket}/{blob_name}")

        public_url = f"https://storage.googleapis.com/{storage_bucket}/{blob_name}"
        foto_urls.append(public_url)
        blob_paths.append(blob_name)

    df = dataframe.copy()
    df["foto_url"] = foto_urls
    df["foto_blob_path"] = blob_paths
    return df


@task
def build_output_dataframe_task(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Prepara o DataFrame final a ser persistido no BigQuery."""

    if dataframe.empty:
        return dataframe.assign(
            foto_url=pd.Series(dtype="string"),
            foto_blob_path=pd.Series(dtype="string"),
            qrcode_payload=pd.Series(dtype="string"),
            ingestao_data=pd.Series(dtype="datetime64[ns]"),
        )

    df = dataframe.copy()
    df["ingestao_data"] = datetime.now(timezone.utc)

    selected_columns = [
        "animal_identifier",
        "qrcode_payload",
        "foto_url",
        "foto_blob_path",
        "ingestao_data",
    ]
    return df[selected_columns]


def process_single_batch(
    client: bigquery.Client,
    source_table: str,
    target_table: str,
    identifier_field: str,
    offset: int,
    batch_size: int,
    storage_bucket: str,
    storage_prefix: str,
    billing_project_id: str,
) -> pd.DataFrame:
    """
    Processa um único lote: busca dados, extrai QR code, faz upload de imagens.

    Retorna o DataFrame processado pronto para gravar no BigQuery.
    """
    # Fetch batch
    batch_df = fetch_batch(client, source_table, target_table, identifier_field, offset, batch_size)

    if batch_df.empty:
        log(f"Lote vazio no offset {offset}. Pulando.")
        return pd.DataFrame()

    # Extract QR code payload
    batch_df = _extract_qrcode_payload_batch(batch_df)

    # Upload images
    batch_df = _upload_batch_images(
        batch_df,
        storage_bucket,
        storage_prefix,
        billing_project_id,
    )

    # Build output
    output_df = _build_batch_output(batch_df)

    return output_df


def _extract_qrcode_payload_batch(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Versão sem @task para processar QR codes em lote."""
    if dataframe.empty:
        return dataframe.assign(qrcode_payload=pd.Series(dtype="string"))

    df = dataframe.copy()
    df["qrcode_payload"] = df["qrcode_dados"].apply(_extract_qrcode_payload)
    return df


def _upload_batch_images(
    dataframe: pd.DataFrame,
    storage_bucket: str,
    storage_prefix: str,
    billing_project_id: str,
) -> pd.DataFrame:
    """Versão sem @task para upload de imagens em lote."""
    if dataframe.empty:
        return dataframe.assign(foto_url=pd.Series(dtype="string"), foto_blob_path=pd.Series(dtype="string"))

    storage_client = storage.Client(project=billing_project_id)
    bucket = storage_client.bucket(storage_bucket)

    foto_urls: list[str | None] = []
    blob_paths: list[str | None] = []

    for _, row in dataframe.iterrows():
        raw_value = row.get("foto_dados")
        identifier = row.get("animal_identifier")
        safe_identifier = _sanitize_identifier(identifier)

        if raw_value is None or str(raw_value).strip() == "":
            foto_urls.append(None)
            blob_paths.append(None)
            continue

        raw_text = _coerce_to_base64_text(raw_value)
        cleaned = _strip_data_uri_prefix(raw_text)

        raw_preview = raw_text.strip().replace("\n", " ")
        cleaned_preview = cleaned.strip().replace("\n", " ")
        log(
            f"[DEBUG] Preparando decode (batch) animal={identifier} tipo={type(raw_value).__name__} "
            f"len_original={len(raw_text)} "
            f"len_limpo={len(cleaned)} preview_original={raw_preview[:60]} preview_limpo={cleaned_preview[:60]}"
        )

        try:
            image_bytes = detect_and_decode(cleaned)
        except ValueError as exc:
            log(f"[ERRO CRÍTICO] Falha ao decodificar Base64 do animal {identifier}: {exc}")
            raise ValueError(
                f"Decode de Base64 falhou para animal {identifier}. "
                f"Batch abortado para evitar transferência de dados incompletos."
            ) from exc

        extension = _infer_extension(image_bytes)
        content_type = _extension_to_content_type(extension)
        digest = hashlib.sha1(image_bytes).hexdigest()
        blob_name = f"{storage_prefix}/animal_id={safe_identifier}/{digest}.{extension}"
        blob = bucket.blob(blob_name)

        try:
            exists = blob.exists(storage_client)
        except NotFound:
            exists = False

        if not exists:
            log(f"Upload da imagem do animal {identifier} para gs://{storage_bucket}/{blob_name}")
            blob.upload_from_string(image_bytes, content_type=content_type)
            blob.metadata = {"sha1": digest}
            blob.patch()
        else:
            log(f"Imagem do animal {identifier} já existe em gs://{storage_bucket}/{blob_name}")

        public_url = f"https://storage.googleapis.com/{storage_bucket}/{blob_name}"
        foto_urls.append(public_url)
        blob_paths.append(blob_name)

    df = dataframe.copy()
    df["foto_url"] = foto_urls
    df["foto_blob_path"] = blob_paths
    return df


def _build_batch_output(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Versão sem @task para construir output do lote."""
    if dataframe.empty:
        return dataframe.assign(
            id_animal=pd.Series(dtype="string"),
            cpf_proprietario=pd.Series(dtype="string"),
            foto_url=pd.Series(dtype="string"),
            qrcode_payload=pd.Series(dtype="string"),
            ingestao_data=pd.Series(dtype="datetime64[ns]"),
        )

    df = dataframe.copy()
    df["ingestao_data"] = datetime.now(timezone.utc)

    # Renomear animal_identifier para id_animal
    df = df.rename(columns={"animal_identifier": "id_animal"})

    selected_columns = [
        "id_animal",
        "cpf_proprietario",
        "qrcode_payload",
        "foto_url",
        "ingestao_data",
    ]
    return df[selected_columns]
