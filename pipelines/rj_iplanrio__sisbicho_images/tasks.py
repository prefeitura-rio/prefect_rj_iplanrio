# -*- coding: utf-8 -*-
"""Tasks do fluxo rj_iplanrio__sisbicho_images."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Iterable

import cv2
import numpy as np
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


def _decode_qrcode_bytes(image_bytes: bytes) -> str | None:
    """Lê o conteúdo textual presente em um QR Code representado como imagem."""

    array = np.frombuffer(image_bytes, dtype=np.uint8)
    image = cv2.imdecode(array, cv2.IMREAD_GRAYSCALE)
    if image is None:
        log("[ERRO] Não foi possível decodificar bytes do QR Code em imagem.")
        return None

    detector = cv2.QRCodeDetector()
    payload, _, _ = detector.detectAndDecode(image)
    if not payload:
        return None

    payload = payload.strip()
    return payload or None


def _normalize_qrcode_payload(text: str) -> str | None:
    """Converte o texto lido do QR Code em JSON com chaves intactas."""

    if text is None:
        return None

    cleaned = text.strip()
    if not cleaned:
        return None

    # Primeiro, tenta parsear como JSON nativo.
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, (dict, list)):
        return json.dumps(parsed, ensure_ascii=False)
    if isinstance(parsed, str):
        cleaned = parsed.strip()

    lines = [line.strip() for line in cleaned.replace("\r", "\n").split("\n") if line.strip()]
    payload_dict: dict[str, str] = {}
    current_key: str | None = None

    for line in lines:
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if key:
                payload_dict[key] = value
                current_key = key
                continue

        if current_key:
            extra = line.strip()
            if extra:
                existing = payload_dict.get(current_key, "")
                payload_dict[current_key] = f"{existing} {extra}".strip() if existing else extra
        else:
            payload_dict.setdefault("observacao", "")
            payload_dict["observacao"] = (
                f"{payload_dict['observacao']} {line}".strip() if payload_dict["observacao"] else line
            )

    if payload_dict:
        return json.dumps(payload_dict, ensure_ascii=False)

    return cleaned


def _extract_qrcode_payload(value: str) -> str | None:
    if value is None:
        return None

    candidate = _coerce_to_base64_text(value).strip()
    if not candidate:
        return None

    cleaned = _strip_data_uri_prefix(candidate)
    if not _looks_like_base64(cleaned):
        log("[ERRO] Valor de QR Code não está em Base64 válido.")
        return None

    try:
        image_bytes = detect_and_decode(cleaned)
    except ValueError as exc:
        log(f"[ERRO] Falha ao decodificar imagem Base64 do QR Code: {exc}")
        return None

    payload = _decode_qrcode_bytes(image_bytes)
    if payload:
        normalized_payload = _normalize_qrcode_payload(payload)
        if normalized_payload:
            return normalized_payload

    decoded_text = _bytes_to_text(image_bytes)
    if decoded_text:
        normalized_payload = _normalize_qrcode_payload(decoded_text)
        if normalized_payload:
            return normalized_payload

    log("[ERRO] QR Code não pôde ser interpretado.")
    return None


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
) -> tuple[int, bool]:
    """Retorna o número total de registros com mídia disponível que ainda não foram processados.

    Returns:
        tuple: (total_count, table_is_empty)
            - total_count: número de registros a processar
            - table_is_empty: True se a tabela existe mas está vazia
    """

    # Verifica se a tabela de destino existe
    table_is_empty = False
    try:
        target_table_ref = client.get_table(target_table)
        table_exists = True
        log(f"[DEBUG COUNT] Tabela target EXISTE: {target_table}")
        log(f"[DEBUG COUNT] Tabela tem {target_table_ref.num_rows} linhas")
    except NotFound:
        log(f"[DEBUG COUNT] Tabela {target_table} NÃO EXISTE. Primeira execução: processando todos os registros.")
        table_exists = False

    if table_exists:
        # Query incremental com LEFT JOIN
        count_query = f"""
            SELECT COUNT(*) as total
            FROM `{source_table}` AS src
            LEFT JOIN `{target_table}` AS tgt
                ON CAST(src.{identifier_field} AS STRING) = tgt.id_animal
            WHERE (src.qrcode_dados IS NOT NULL OR src.foto_dados IS NOT NULL)
              AND tgt.id_animal IS NULL
        """

        log(f"[DEBUG COUNT] Query incremental:")
        log(f"[DEBUG COUNT] {count_query}")

        try:
            result = client.query(count_query).result()
            total = next(result).total
            log(f"[DEBUG COUNT] Resultado: {total} registros NOVOS a processar (LEFT JOIN filtrou registros existentes)")
            return total, table_is_empty
        except Exception as exc:
            error_msg = str(exc).lower()
            # Detecta tabela vazia (Hive partition sem arquivos)
            if "cannot query hive partitioned data" in error_msg and "without any associated files" in error_msg:
                log(f"[INFO] Tabela {target_table} existe mas está vazia. Será deletada e recriada.")
                table_exists = False
                table_is_empty = True
            else:
                # Outro tipo de erro - propaga
                log(f"[ERRO] Falha ao executar query incremental: {exc}")
                raise

    # Query completa (primeira carga ou tabela vazia)
    count_query = f"""
        SELECT COUNT(*) as total
        FROM `{source_table}` AS src
        WHERE (src.qrcode_dados IS NOT NULL OR src.foto_dados IS NOT NULL)
    """

    log(f"[DEBUG COUNT] Query completa (sem filtro incremental):")
    log(f"[DEBUG COUNT] {count_query}")

    result = client.query(count_query).result()
    total = next(result).total
    log(f"[DEBUG COUNT] Resultado: {total} registros TOTAIS (primeira carga ou tabela vazia)")
    return total, table_is_empty


@task
def fetch_sisbicho_media_task(
    billing_project_id: str,
    credential_bucket: str,
    source_dataset_id: str,
    source_table_id: str,
    target_dataset_id: str,
    target_table_id: str,
    batch_size: int = 1000,
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

    total_count, table_is_empty = _get_total_count(client, source_table, target_table, identifier_field)

    # Se a tabela está vazia (corrompida), deleta para o basedosdados recriar do zero
    if table_is_empty:
        log(f"[RECOVERY] Deletando tabela vazia {target_table}...")
        try:
            client.delete_table(target_table, not_found_ok=True)
            log(f"[RECOVERY] Tabela {target_table} deletada com sucesso. Será recriada na primeira gravação.")
        except Exception as exc:
            log(f"[ERRO] Falha ao deletar tabela vazia: {exc}")
            raise

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

    # Extrai dataset e project da source_table para montar as outras tabelas
    parts = source_table.split(".")
    project_dataset = ".".join(parts[:-1])

    # Verifica se a tabela de destino existe
    try:
        target_table_ref = client.get_table(target_table)
        table_exists = True
        log(f"[DEBUG FETCH] Tabela target EXISTE: {target_table} com {target_table_ref.num_rows} linhas")
    except NotFound:
        log(f"[DEBUG FETCH] Tabela target NÃO EXISTE: {target_table}")
        table_exists = False

    if table_exists:
        # Query incremental com LEFT JOIN
        query = f"""
            WITH animal_unico AS (
                SELECT
                    {identifier_field},
                    qrcode_dados,
                    foto_dados,
                    ROW_NUMBER() OVER (PARTITION BY {identifier_field} ORDER BY datalake_loaded_at DESC) as rn
                FROM `{source_table}`
                WHERE qrcode_dados IS NOT NULL OR foto_dados IS NOT NULL
            )
            SELECT
                CAST(a.{identifier_field} AS STRING) AS animal_identifier,
                prop.cpf_numero AS cpf,
                a.qrcode_dados,
                a.foto_dados
            FROM animal_unico a
            LEFT JOIN `{target_table}` AS tgt
                ON CAST(a.{identifier_field} AS STRING) = tgt.id_animal
            LEFT JOIN `{project_dataset}.animal_proprietario` AS ap
                ON a.{identifier_field} = ap.id_animal
                AND ap.fim_datahora IS NULL
            LEFT JOIN `{project_dataset}.proprietario` AS prop
                ON ap.id_proprietario = prop.id_proprietario
            WHERE a.rn = 1
              AND tgt.id_animal IS NULL
            ORDER BY a.{identifier_field}
            LIMIT {batch_size}
            OFFSET {offset}
        """.strip()

        log(f"Buscando lote: offset={offset}, limit={batch_size}")

        job_config = bigquery.QueryJobConfig(
            use_query_cache=True,
            use_legacy_sql=False,
        )

        try:
            query_job = client.query(query, job_config=job_config)
            dataframe = query_job.result().to_dataframe()
            log(f"[DEBUG FETCH] Lote carregado: {len(dataframe)} registros")
            if len(dataframe) > 0:
                sample_ids = dataframe['animal_identifier'].head(3).tolist()
                log(f"[DEBUG FETCH] Sample IDs retornados: {sample_ids}")
            return dataframe
        except Exception as exc:
            error_msg = str(exc).lower()
            # Detecta tabela vazia (Hive partition sem arquivos)
            if "cannot query hive partitioned data" in error_msg and "without any associated files" in error_msg:
                log(f"[INFO] Tabela {target_table} existe mas está vazia. Usando query sem JOIN.")
                table_exists = False
            else:
                # Outro tipo de erro - propaga
                raise

    # Query completa (primeira carga ou tabela vazia)
    query = f"""
        WITH animal_unico AS (
            SELECT
                {identifier_field},
                qrcode_dados,
                foto_dados,
                ROW_NUMBER() OVER (PARTITION BY {identifier_field} ORDER BY datalake_loaded_at DESC) as rn
            FROM `{source_table}`
            WHERE qrcode_dados IS NOT NULL OR foto_dados IS NOT NULL
        )
        SELECT
            CAST(a.{identifier_field} AS STRING) AS animal_identifier,
            prop.cpf_numero AS cpf,
            a.qrcode_dados,
            a.foto_dados
        FROM animal_unico a
        LEFT JOIN `{project_dataset}.animal_proprietario` AS ap
            ON a.{identifier_field} = ap.id_animal
            AND ap.fim_datahora IS NULL
        LEFT JOIN `{project_dataset}.proprietario` AS prop
            ON ap.id_proprietario = prop.id_proprietario
        WHERE a.rn = 1
        ORDER BY a.{identifier_field}
        LIMIT {batch_size}
        OFFSET {offset}
    """.strip()

    log(f"[DEBUG FETCH] Buscando lote COMPLETO (primeira carga): offset={offset}, limit={batch_size}")
    log(f"[DEBUG FETCH] Query: {query[:500]}...")

    job_config = bigquery.QueryJobConfig(
        use_query_cache=True,
        use_legacy_sql=False,
    )

    query_job = client.query(query, job_config=job_config)
    dataframe = query_job.result().to_dataframe()

    log(f"[DEBUG FETCH] Lote carregado: {len(dataframe)} registros")
    if len(dataframe) > 0:
        sample_ids = dataframe['animal_identifier'].head(3).tolist()
        log(f"[DEBUG FETCH] Sample IDs retornados: {sample_ids}")
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
    storage_project_id: str,
) -> pd.DataFrame:
    """Faz o upload das imagens dos pets para o GCS e retorna a URL final."""

    if dataframe.empty:
        return dataframe.assign(foto_url=pd.Series(dtype="string"), foto_blob_path=pd.Series(dtype="string"))

    storage_client = storage.Client(project=storage_project_id)
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
    storage_project_id: str,
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
        storage_project_id,
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
    storage_project_id: str,
) -> pd.DataFrame:
    """Versão sem @task para upload de imagens em lote."""
    if dataframe.empty:
        return dataframe.assign(foto_url=pd.Series(dtype="string"), foto_blob_path=pd.Series(dtype="string"))

    storage_client = storage.Client(project=storage_project_id)
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
            cpf=pd.Series(dtype="string"),
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
        "cpf",
        "qrcode_payload",
        "foto_url",
        "ingestao_data",
    ]

    output_df = df[selected_columns]

    # Log de debug para verificar dados que serão gravados
    log(f"[DEBUG OUTPUT] Preparando {len(output_df)} registros para gravação")
    if len(output_df) > 0:
        sample_ids = output_df['id_animal'].head(3).tolist()
        log(f"[DEBUG OUTPUT] Sample IDs que serão gravados: {sample_ids}")
        # Verificar se há NULLs
        null_count = output_df['id_animal'].isna().sum()
        if null_count > 0:
            log(f"[AVISO OUTPUT] {null_count} registros com id_animal NULL!")

    return output_df
