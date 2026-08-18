"""
Writes pipeline results to GCS.

Two output formats are supported:

CSV (legado):
  {base_path}/data_geracao=YYYY-MM-DD/resultado_extracao_modelo_YYYYMMDD_HHMMSS.csv

NDJSON (novo padrão — tabela extracao_pagina):
  {base_path}/data_geracao=YYYY-MM-DD/extracao_pagina_YYYYMMDD_HHMMSS.ndjson

  O formato NDJSON (newline-delimited JSON) preserva campos aninhados
  (match_id_documento como lista, valores_encontrados/cnpjs_encontrados como dict,
  versao_pipeline/versao_prompt como dict) sem conversão, e pode ser carregado
  diretamente no BigQuery via LoadJob com source_format=NEWLINE_DELIMITED_JSON.
"""

import io
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage


class GCSResultsWriter:
    """Writes results DataFrames to GCS with Hive-style date partitioning."""

    def __init__(
        self,
        bucket_name: str,
        credentials_path: Path | None = None,
    ):
        if credentials_path and Path(credentials_path).exists():
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path)
            )
            client = storage.Client(credentials=credentials)
        else:
            client = storage.Client()

        self.bucket = client.bucket(bucket_name)
        self.bucket_name = bucket_name

    def write_results(
        self,
        df: pd.DataFrame,
        base_path: str,
        timestamp: datetime | None = None,
    ) -> str:
        """
        Write results DataFrame to GCS as a partitioned CSV.

        :param df: Results DataFrame (already transformed by prepare_output_for_bq).
        :param base_path: GCS path prefix, e.g. 'results/nf-pipeline/output'.
        :param timestamp: Partition timestamp (defaults to UTC now).
        :returns: Full GCS URI of the written file (gs://bucket/path/...).
        """
        if timestamp is None:
            timestamp = datetime.utcnow()

        date_str = timestamp.strftime("%Y-%m-%d")
        ts_str = timestamp.strftime("%Y%m%d_%H%M%S")

        blob_path = (
            f"{base_path}/data_geracao={date_str}"
            f"/resultado_extracao_modelo_{ts_str}.csv"
        )

        print(
            f"[GCSResultsWriter] Writing {len(df):,} rows to "
            f"gs://{self.bucket_name}/{blob_path}"
        )

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

        full_path = f"gs://{self.bucket_name}/{blob_path}"
        print(f"[GCSResultsWriter] ✓ Written to {full_path}")
        return full_path

    def write_results_ndjson(
        self,
        items: list[dict],
        base_path: str,
        timestamp: datetime | None = None,
    ) -> str:
        """
        Serializa a lista de itens por página como NDJSON e grava no GCS.

        Usa Hive-style date partitioning igual ao CSV legado, mas com extensão
        .ndjson e nome de arquivo extracao_pagina_<timestamp>.

        Campos aninhados (listas, dicts) são preservados nativamente — não há
        necessidade de flatten ou conversão.  O arquivo pode ser carregado no
        BigQuery com:
            LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
            )

        :param items: Lista de dicts por página (saída de _build_json_output).
        :param base_path: Prefixo GCS, ex.: 'staging/brutos_poc_osinfo_ia/extracao_pagina'.
        :param timestamp: Timestamp da partição (default: UTC now).
        :returns: URI completa do arquivo gravado (gs://bucket/path/...).
        """
        if timestamp is None:
            timestamp = datetime.utcnow()

        date_str = timestamp.strftime("%Y-%m-%d")
        ts_str   = timestamp.strftime("%Y%m%d_%H%M%S")

        blob_path = (
            f"{base_path}/data_geracao={date_str}"
            f"/extracao_pagina_{ts_str}.ndjson"
        )

        print(
            f"[GCSResultsWriter] Writing {len(items):,} pages to "
            f"gs://{self.bucket_name}/{blob_path}"
        )

        # Serializa cada item em uma linha JSON separada (NDJSON)
        ndjson_content = "\n".join(
            json.dumps(item, ensure_ascii=False, default=str)
            for item in items
        )

        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(
            ndjson_content.encode("utf-8"),
            content_type="application/x-ndjson",
        )

        full_path = f"gs://{self.bucket_name}/{blob_path}"
        print(f"[GCSResultsWriter] ✓ Written to {full_path}")
        return full_path
