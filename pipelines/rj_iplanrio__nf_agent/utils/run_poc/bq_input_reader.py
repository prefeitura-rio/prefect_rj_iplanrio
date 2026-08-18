"""
Reads unprocessed documents from BigQuery for the NF pipeline.

Joins the input view with the status table to return only documents
that have never been processed (status IS NULL) or that errored (status = 'erro').
"""

from pathlib import Path

import pandas as pd
from google.cloud import bigquery


class BQInputReader:
    """Reads batches of unprocessed documents from a BigQuery view."""

    def __init__(
        self,
        project_id: str | None = None,
        credentials_path: Path | None = None,
    ):
        if credentials_path and Path(credentials_path).exists():
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path)
            )
            self.client = bigquery.Client(
                credentials=credentials, project=credentials.project_id
            )
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
        # COALESCE to id_documento guards against NULL descricao so the
        # INNER JOIN never drops rows due to NULL = NULL being FALSE in SQL.
        query = f"""
            WITH unprocessed AS (
                SELECT v.*
                FROM `{input_table}` v
                LEFT JOIN `{status_table}` c
                  ON CAST(v.id_documento AS STRING) = CAST(c.id_documento AS STRING)
                WHERE c.id_documento IS NULL
                   OR (c.status = 'erro' AND (c.retry_count IS NULL OR c.retry_count < {max_retries}))
            ),
            batch_pdf_keys AS (
                SELECT DISTINCT
                    COALESCE(descricao, CAST(id_documento AS STRING)) AS pdf_key
                FROM unprocessed
                LIMIT {batch_size}
            )
            SELECT u.*
            FROM unprocessed u
            INNER JOIN batch_pdf_keys bk
              ON COALESCE(u.descricao, CAST(u.id_documento AS STRING)) = bk.pdf_key
        """
        print(f"[BQInputReader] Querying all rows for up to {batch_size} unprocessed PDFs...")
        df = self.client.query(query).to_dataframe()
        distinct_pdfs = df["descricao"].str.replace(r"\.pdf$", "", case=False, regex=True).nunique() if "descricao" in df.columns else "?"
        print(f"[BQInputReader] Got {len(df):,} rows across {distinct_pdfs} PDFs")
        if "descricao_limpa" not in df.columns and "descricao" in df.columns:
            df["descricao_limpa"] = df["descricao"].str.replace(r"\.pdf$", "", case=False, regex=True)
        return df

    def count_pending(self, input_table: str, status_table: str, max_retries: int = 3) -> int:
        """Return the total number of documents still pending processing."""
        query = f"""
            SELECT COUNT(*) AS total
            FROM `{input_table}` v
            LEFT JOIN `{status_table}` c
              ON CAST(v.id_documento AS STRING) = CAST(c.id_documento AS STRING)
            WHERE c.id_documento IS NULL
               OR (c.status = 'erro' AND (c.retry_count IS NULL OR c.retry_count < {max_retries}))
        """
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
        query = f"""
            SELECT
                COALESCE(c.status, 'pendente') AS status,
                COUNT(*) AS total_docs,
                COUNT(DISTINCT COALESCE(v.descricao, CAST(v.id_documento AS STRING))) AS total_pdfs
            FROM `{input_table}` v
            LEFT JOIN `{status_table}` c
              ON CAST(v.id_documento AS STRING) = CAST(c.id_documento AS STRING)
            GROUP BY 1
        """
        df = self.client.query(query).to_dataframe()
        return {
            row["status"]: {"docs": int(row["total_docs"]), "pdfs": int(row["total_pdfs"])}
            for _, row in df.iterrows()
        }
