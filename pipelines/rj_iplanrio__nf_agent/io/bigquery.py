"""
BigQuery I/O for the NF pipeline: read pending documents, write status/results.

``BQInputReader`` and ``BigQueryWriter`` are always used together (one flow run
reads a batch, processes it, then writes status back) and are both thin,
NF-pipeline-specific wrappers around BigQuery — merged into one module rather
than kept in separate files.

``BigQueryWriter`` subclasses ``iplanrio_agent_toolkit.bigquery.BigQueryClient``
and adds the NF-pipeline-specific bits the generic client deliberately doesn't
cover: the ``controle_processamento`` MERGE-based status upsert (with its
retry_count/error_message fallback degradation) and the ``pipeline_runs``
streaming insert.

``BQInputReader`` does NOT subclass ``BigQueryClient``: it deliberately builds
its own raw ``bigquery.Client``, because it queries two fully-qualified tables
that can live in different datasets, while ``BigQueryClient.__init__`` requires
exactly one fixed ``dataset_id`` — forcing that fit would need an artificial
dataset_id with no real use (evaluated and rejected during the toolkit
extraction pass, see the plan history).
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from iplanrio_agent_toolkit.bigquery import BigQueryClient

from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)

_RATE_LIMIT_MARKERS = ("429", "resource exhausted", "quota exceeded")
_PDF_SUFFIX_RE = r"\.pdf$"


def _has_rate_limit_error(classification_detail, pipeline_error=None) -> bool:
    """
    Return True if a Gemini 429 / quota-exhausted error is present in either:
    - pipeline_classification_detail (justification field de qualquer página), ou
    - pipeline_error (erros que ocorreram durante extração — ponto cego 1.6:
      erros de quota na extração eram registrados em pipeline_error, não em
      classification_detail, e portanto não eram detectados aqui).
    """

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
    return any(_text_has_rate_limit(str(p.get("justification", ""))) for p in pages)


class BQInputReader:
    """Reads batches of unprocessed documents from a BigQuery view."""

    def __init__(
        self,
        project_id: str | None = None,
        credentials_path: Path | None = None,
    ):
        if credentials_path and Path(credentials_path).exists():
            credentials = service_account.Credentials.from_service_account_file(str(credentials_path))
            self.client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        else:
            self.client = bigquery.Client(project=project_id)

    def read_unprocessed_batch(
        self,
        input_table: str,
        status_table: str,
        batch_size: int,
        max_retries: int = 3,
    ) -> pd.DataFrame:
        """
        Read all unprocessed rows for up to batch_size distinct PDFs.

        batch_size controls how many distinct PDFs are included, not how many
        rows are returned. All declarações for a selected PDF are always fetched
        together so they are never split across batches.

        Includes documents where:
          - status IS NULL  → never attempted
          - status = 'erro' AND retry_count < max_retries → failed but retryable

        Documents with retry_count >= max_retries are permanently excluded.

        :param input_table: Full BQ table/view ID, e.g. ``'project.dataset.vw_name'``.
        :param status_table: Full BQ table ID for the status control table.
        :param batch_size: Maximum number of distinct PDFs to include in this batch.
        :param max_retries: Documents with this many errors are skipped permanently.
        :returns: DataFrame with all columns from input_table for the selected PDFs.
        """
        # Group by descricao (view already strips .pdf suffix).
        query = load_query(
            __file__,
            "unprocessed_batch",
            input_table=input_table,
            status_table=status_table,
            max_retries=max_retries,
            batch_size=batch_size,
        )
        logger.info("Querying all rows for up to %d unprocessed PDFs...", batch_size)
        df = self.client.query(query).to_dataframe()
        distinct_pdfs = (
            df["descricao"].str.replace(_PDF_SUFFIX_RE, "", case=False, regex=True).nunique()
            if "descricao" in df.columns
            else "?"
        )
        logger.info("Got %d rows across %s PDFs", len(df), distinct_pdfs)
        if "descricao_limpa" not in df.columns and "descricao" in df.columns:
            df["descricao_limpa"] = df["descricao"].str.replace(_PDF_SUFFIX_RE, "", case=False, regex=True)
        return df

    def count_pending(self, input_table: str, status_table: str, max_retries: int = 3) -> int:
        """Return the total number of documents still pending processing."""
        query = load_query(
            __file__,
            "count_pending",
            input_table=input_table,
            status_table=status_table,
            max_retries=max_retries,
        )
        result = self.client.query(query).to_dataframe()
        return int(result["total"].iloc[0])

    def count_by_status(self, input_table: str, status_table: str) -> dict:
        """
        Return document and PDF counts per status.

        Returns a dict keyed by status ('processado', 'erro', 'pendente'), each
        containing ``{'docs': N, 'pdfs': M}`` where docs = rows (declarações) and
        pdfs = distinct PDF names.

        Example::

            {
                'processado': {'docs': 182, 'pdfs': 45},
                'erro':       {'docs': 275, 'pdfs': 68},
                'pendente':   {'docs':  43, 'pdfs': 12},
            }
        """
        query = load_query(__file__, "count_by_status", input_table=input_table, status_table=status_table)
        df = self.client.query(query).to_dataframe()
        return {
            row["status"]: {"docs": int(row["total_docs"]), "pdfs": int(row["total_pdfs"])} for _, row in df.iterrows()
        }


class BigQueryWriter(BigQueryClient):
    """Write pipeline results to BigQuery table."""

    def _run_upsert_merge(self, status_table: str, status_rows: list, now: datetime) -> None:
        """
        MERGE status rows into the control table.

        Increments retry_count on every error. Falls back gracefully if the
        optional columns (error_message, retry_count) don't exist yet.

        Run these once in BQ console to unlock full tracking:
            ALTER TABLE `<status_table>` ADD COLUMN IF NOT EXISTS error_message STRING;
            ALTER TABLE `<status_table>` ADD COLUMN IF NOT EXISTS retry_count INT64;
        """
        ts = now.strftime("%Y-%m-%d %H:%M:%S")

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

        _merge_sql_names = {
            "full": "merge_status_full",
            "no_retry": "merge_status_no_retry",
            "minimal": "merge_status_minimal",
        }

        def _merge_query(mode: str) -> str:
            rows_sql = _build_rows(mode)
            return load_query(__file__, _merge_sql_names[mode], status_table=status_table, rows_sql=rows_sql)

        def _is_missing_col(e: Exception) -> bool:
            msg = str(e).lower()
            return "unrecognized name" in msg or "not found" in msg

        try:
            self.client.query(_merge_query("full")).result()
        except Exception as e:
            if _is_missing_col(e):
                logger.warning(
                    "retry_count or error_message column missing — trying without retry_count. "
                    "Run in BQ console to enable full tracking: "
                    "ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS error_message STRING; "
                    "ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS retry_count INT64;",
                    status_table,
                    status_table,
                )
                try:
                    self.client.query(_merge_query("no_retry")).result()
                except Exception as e2:
                    if _is_missing_col(e2):
                        logger.warning("error_message also missing — using minimal merge.")
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
        now = datetime.now(UTC)

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
        logger.info("Status updated — processado: %d, erro: %d", ok, err)

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
        self.insert_row(pipeline_runs_table, row)
