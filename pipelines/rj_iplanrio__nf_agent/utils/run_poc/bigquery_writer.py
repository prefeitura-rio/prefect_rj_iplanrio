"""
BigQuery Writer - Save pipeline results directly to BigQuery.
"""

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


def _has_rate_limit_error(classification_detail, pipeline_error=None) -> bool:
    """
    Return True if a Gemini 429 / quota-exhausted error is present in either:
    - pipeline_classification_detail (justification field de qualquer página), ou
    - pipeline_error (erros que ocorreram durante extração — ponto cego 1.6:
      erros de quota na extração eram registrados em pipeline_error, não em
      classification_detail, e portanto não eram detectados aqui).
    """
    import json

    _RATE_LIMIT_MARKERS = ("429", "resource exhausted", "quota exceeded")

    def _text_has_rate_limit(text: str) -> bool:
        t = text.lower()
        return any(m in t for m in _RATE_LIMIT_MARKERS)

    # Verifica pipeline_error primeiro (cobre erros de extração)
    if pipeline_error is not None:
        raw = pipeline_error if isinstance(pipeline_error, str) else json.dumps(pipeline_error)
        if _text_has_rate_limit(raw):
            return True

    # Verifica classification_detail (cobre erros de classificação)
    if classification_detail is None:
        return False

    if isinstance(classification_detail, str):
        try:
            classification_detail = json.loads(classification_detail)
        except (ValueError, TypeError):
            return False

    if not isinstance(classification_detail, dict):
        return False

    pages = classification_detail.get("pages", [])
    return any(
        _text_has_rate_limit(str(p.get("justification", "")))
        for p in pages
    )


class BigQueryWriter:
    """Write pipeline results to BigQuery table."""

    def __init__(
        self,
        credentials_path: Path | None = None,
        project_id: str | None = None,
        dataset_id: str | None = None,
    ):
        """
        Initialize BigQuery writer.

        :param credentials_path: Path to BigQuery service account JSON (None = use ADC).
        :param project_id: GCP project ID (optional, auto-detected from credentials).
        :param dataset_id: BigQuery dataset ID (falls back to BIGQUERY_DATASET_ID env var).
        """
        # Initialize BigQuery client
        if credentials_path and Path(credentials_path).exists():
            credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path)
            )
            self.client = bigquery.Client(
                credentials=credentials,
                project=credentials.project_id
            )
            self.project_id = credentials.project_id
        else:
            # Use Application Default Credentials (ADC)
            self.client = bigquery.Client(project=project_id)
            self.project_id = project_id or self.client.project

        self.dataset_id = dataset_id or os.getenv("BIGQUERY_DATASET_ID")
        if not self.dataset_id:
            raise ValueError("dataset_id must be provided or BIGQUERY_DATASET_ID env var must be set")
        print(f"[BigQuery] Initialized - Project: {self.project_id}, Dataset: {self.dataset_id}")

    def write_results(
        self,
        df: pd.DataFrame,
        table_id: str,
        write_mode: str = "WRITE_APPEND",
        add_timestamp: bool = True,
    ) -> dict:
        """
        Write results DataFrame to BigQuery table.

        :param df: Results DataFrame to write.
        :param table_id: Target table ID (e.g., 'nf_pipeline_results').
        :param write_mode: Write mode (default: WRITE_APPEND).
            Options:
              - WRITE_APPEND: Append to existing table
              - WRITE_TRUNCATE: Replace table contents
              - WRITE_EMPTY: Only write if table is empty
        :param add_timestamp: If True, add 'processed_at' column with current timestamp.
        :returns: Dict with load job stats:
            - table: Full table reference
            - rows_written: Number of rows written
            - bytes_processed: Bytes processed
            - job_id: BigQuery job ID

        Example::

            # Append results
            writer.write_results(results_df, 'nf_pipeline_results')

            # Replace table
            writer.write_results(
                results_df,
                'nf_pipeline_results',
                write_mode='WRITE_TRUNCATE'
            )
        """
        # Add timestamp column if requested
        if add_timestamp and 'processed_at' not in df.columns:
            df = df.copy()
            df['processed_at'] = datetime.utcnow()

        # Build full table reference
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        print(f"[BigQuery] Writing {len(df):,} rows to {table_ref}")

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_mode,
            # Auto-detect schema from DataFrame
            autodetect=True,
            # Create table if it doesn't exist
            create_disposition="CREATE_IF_NEEDED",
        )

        # Load DataFrame to BigQuery
        load_job = self.client.load_table_from_dataframe(
            df,
            table_ref,
            job_config=job_config
        )

        # Wait for job to complete
        print(f"[BigQuery] Job started: {load_job.job_id}")
        load_job.result()  # Wait for completion

        print(f"[BigQuery] ✓ Successfully loaded {load_job.output_rows:,} rows")

        return {
            "table": table_ref,
            "rows_written": load_job.output_rows,
            "bytes_processed": load_job.total_bytes_processed,
            "job_id": load_job.job_id,
        }

    def create_partitioned_table(
        self,
        table_id: str,
        partition_field: str = "processed_at",
        partition_type: str = "DAY",
        clustering_fields: list | None = None,
    ):
        """
        Create a partitioned table (useful for large result tables).

        :param table_id: Table ID to create.
        :param partition_field: Field to partition by (must be DATE/TIMESTAMP).
        :param partition_type: Partition granularity (DAY, HOUR, MONTH, YEAR).
        :param clustering_fields: Optional list of fields to cluster by
            (e.g., ['pdf_name', 'classification']).

        Example::

            # Create partitioned table for pipeline results
            writer.create_partitioned_table(
                'nf_pipeline_results',
                partition_field='processed_at',
                partition_type='DAY',
                clustering_fields=['pdf_name', 'classification']
            )
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        print(f"[BigQuery] Creating partitioned table: {table_ref}")

        # Build schema (you can customize this based on your results structure)
        schema = [
            bigquery.SchemaField("pdf_name", "STRING"),
            bigquery.SchemaField("id_documento", "STRING"),
            bigquery.SchemaField("classification", "STRING"),
            bigquery.SchemaField("extracted_nfs", "STRING"),  # JSON string
            bigquery.SchemaField("validation_notes", "STRING"),
            bigquery.SchemaField("processing_time_seconds", "FLOAT"),
            bigquery.SchemaField(partition_field, "TIMESTAMP"),
        ]

        table = bigquery.Table(table_ref, schema=schema)

        # Configure partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=getattr(bigquery.TimePartitioningType, partition_type),
            field=partition_field,
        )

        # Configure clustering (optional)
        if clustering_fields:
            table.clustering_fields = clustering_fields

        # Create table
        table = self.client.create_table(table, exists_ok=True)
        print(f"[BigQuery] ✓ Table created: {table_ref}")
        print(f"   Partitioned by: {partition_field} ({partition_type})")
        if clustering_fields:
            print(f"   Clustered by: {clustering_fields}")

        return table

    def _run_upsert_merge(self, status_table: str, status_rows: list, now: datetime) -> None:
        """
        MERGE status rows into the control table.

        Increments retry_count on every error. Falls back gracefully if the
        optional columns (error_message, retry_count) don't exist yet.

        Run these once in BQ console to unlock full tracking:
            ALTER TABLE `<status_table>` ADD COLUMN IF NOT EXISTS error_message STRING;
            ALTER TABLE `<status_table>` ADD COLUMN IF NOT EXISTS retry_count INT64;
        """
        ts = now.strftime('%Y-%m-%d %H:%M:%S')

        def _escape(s):
            return s.replace("'", "\\'")[:500] if s else ""

        # Mode: 'full' | 'no_retry' | 'minimal'
        def _build_rows(mode: str) -> str:
            parts = []
            for r in status_rows:
                msg = _escape(r["error_message"]) if r["error_message"] else ""
                if mode == "full":
                    parts.append(
                        f"STRUCT({r['id_documento']} AS id_documento, '{r['status']}' AS status, "
                        f"'{msg}' AS error_message, TIMESTAMP '{ts}' AS updated_at)"
                    )
                elif mode == "no_retry":
                    parts.append(
                        f"STRUCT({r['id_documento']} AS id_documento, '{r['status']}' AS status, "
                        f"'{msg}' AS error_message, TIMESTAMP '{ts}' AS updated_at)"
                    )
                else:  # minimal
                    parts.append(
                        f"STRUCT({r['id_documento']} AS id_documento, '{r['status']}' AS status, "
                        f"TIMESTAMP '{ts}' AS updated_at)"
                    )
            return ",\n  ".join(parts)

        def _merge_query(mode: str) -> str:
            rows_sql = _build_rows(mode)
            base = f"""
                MERGE `{status_table}` T
                USING UNNEST([
                  {rows_sql}
                ]) S
                  ON CAST(T.id_documento AS STRING) = CAST(S.id_documento AS STRING)
            """
            if mode == "full":
                return base + """
                WHEN MATCHED AND S.status = 'erro' THEN
                    UPDATE SET status = S.status, error_message = S.error_message,
                               retry_count = COALESCE(T.retry_count, 0) + 1,
                               updated_at = S.updated_at
                WHEN MATCHED THEN
                    UPDATE SET status = S.status, error_message = S.error_message,
                               updated_at = S.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (id_documento, status, error_message, retry_count, updated_at)
                    VALUES (S.id_documento, S.status, IF(S.status = 'erro', S.error_message, NULL),
                            IF(S.status = 'erro', 1, 0), S.updated_at)
                """
            elif mode == "no_retry":
                return base + """
                WHEN MATCHED THEN
                    UPDATE SET status = S.status, error_message = S.error_message,
                               updated_at = S.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (id_documento, status, error_message, updated_at)
                    VALUES (S.id_documento, S.status, IF(S.status = 'erro', S.error_message, NULL),
                            S.updated_at)
                """
            else:  # minimal
                return base + """
                WHEN MATCHED THEN
                    UPDATE SET status = S.status, updated_at = S.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (id_documento, status, updated_at)
                    VALUES (S.id_documento, S.status, S.updated_at)
                """

        def _is_missing_col(e: Exception) -> bool:
            msg = str(e).lower()
            return "unrecognized name" in msg or "not found" in msg

        try:
            self.client.query(_merge_query("full")).result()
        except Exception as e:
            if _is_missing_col(e):
                print(
                    "[BigQuery] ⚠ retry_count or error_message column missing — trying without retry_count.\n"
                    f"  Run in BQ console to enable full tracking:\n"
                    f"  ALTER TABLE `{status_table}` ADD COLUMN IF NOT EXISTS error_message STRING;\n"
                    f"  ALTER TABLE `{status_table}` ADD COLUMN IF NOT EXISTS retry_count INT64;"
                )
                try:
                    self.client.query(_merge_query("no_retry")).result()
                except Exception as e2:
                    if _is_missing_col(e2):
                        print("[BigQuery] ⚠ error_message also missing — using minimal merge.")
                        self.client.query(_merge_query("minimal")).result()
                    else:
                        raise
            else:
                raise

    def upsert_status(
        self,
        df_results: pd.DataFrame,
        status_table: str,
    ) -> None:
        """
        Upsert processing status for each document into the control table.

        Status values:
          - 'processado' → pipeline ran successfully (result may be OK/Suspect/etc.)
          - 'erro'       → pipeline threw an exception during processing

        Uses a MERGE statement so repeated runs on the same batch are idempotent.

        :param df_results: Results DataFrame with at least 'id_documento' and
            'pipeline_error' columns.
        :param status_table: Full BQ table ID, e.g. 'project.dataset.controle_processamento'.
        """
        now = datetime.utcnow()

        status_rows = []
        for _, row in df_results.iterrows():
            pipeline_error = row.get("pipeline_error", None)
            has_error = bool(pipeline_error and str(pipeline_error).strip())
            error_message = str(pipeline_error).strip() if has_error else None

            if not has_error:
                has_error = _has_rate_limit_error(
                    row.get("pipeline_classification_detail"),
                    pipeline_error=row.get("pipeline_error"),
                )
                if has_error:
                    error_message = "Rate limit (429): Resource exhausted"

            status_rows.append(
                {
                    "id_documento": int(row["id_documento"]),
                    "status": "erro" if has_error else "processado",
                    "error_message": error_message,
                    "updated_at": now,
                }
            )

        self._run_upsert_merge(status_table, status_rows, now)

        ok = sum(1 for r in status_rows if r["status"] == "processado")
        err = sum(1 for r in status_rows if r["status"] == "erro")
        print(f"[BigQuery] ✓ Status updated — processado: {ok}, erro: {err}")

    def write_run_summary(self, pipeline_runs_table: str, row: dict) -> None:
        """
        Append one row to the pipeline_runs tracking table via streaming insert.

        The table must exist. Create it once with:
            CREATE TABLE `<pipeline_runs_table>` (
                session_id        STRING,
                flow_run_id       STRING,
                started_at        TIMESTAMP,
                finished_at       TIMESTAMP,
                duration_seconds  FLOAT64,
                pdfs_processed    INT64,
                pdfs_failed       INT64,
                pending_after     INT64,
                avg_sec_per_pdf   FLOAT64,
                batch_size        INT64,
                workers           INT64,
                requests_per_minute INT64,
                max_concurrent    INT64
            );
        """
        # Streaming insert requires serialisable types
        serialised = {
            k: (v.isoformat() if hasattr(v, "isoformat") else v)
            for k, v in row.items()
        }
        errors = self.client.insert_rows_json(pipeline_runs_table, [serialised])
        if errors:
            print(f"[BigQuery] ⚠ Failed to write run summary: {errors}")
        else:
            print(f"[BigQuery] ✓ Run summary written to {pipeline_runs_table}")

    def query_results(
        self,
        table_id: str,
        filters: dict | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """
        Query results from BigQuery table.

        :param table_id: Source table ID.
        :param filters: Optional dict of filters (same format as GCSCSVReader).
        :param limit: Max rows to return.
        :returns: DataFrame with query results.

        Example::

            # Get all OK results
            df = writer.query_results(
                'nf_pipeline_results',
                filters={'classification': 'OK'},
                limit=1000
            )
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        query = f"SELECT * FROM `{table_ref}`"

        # Build WHERE clause if filters provided
        if filters:
            where_clauses = []
            for key, value in filters.items():
                if isinstance(value, str):
                    where_clauses.append(f"{key} = '{value}'")
                else:
                    where_clauses.append(f"{key} = {value}")

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

        # Add limit
        if limit:
            query += f" LIMIT {limit}"

        print(f"[BigQuery] Running query: {query}")
        df = self.client.query(query).to_dataframe()
        print(f"[BigQuery] Retrieved {len(df):,} rows")

        return df
