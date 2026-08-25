"""
Reads unprocessed documents from BigQuery for the NF pipeline.

Joins the input view with the status table to return only documents
that have never been processed (status IS NULL) or that errored (status = 'erro').
"""

from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)

_PDF_SUFFIX_RE = r"\.pdf$"


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
