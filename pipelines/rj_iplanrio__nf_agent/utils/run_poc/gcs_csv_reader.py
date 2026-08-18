"""
GCS CSV Reader - Read CSV files directly from Google Cloud Storage.
"""

import io
import os
from pathlib import Path

import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account


class GCSCSVReader:
    """Read CSV files from Google Cloud Storage bucket."""

    def __init__(self, credentials_path: Path | None = None, bucket_name: str | None = None):
        """
        Initialize GCS CSV reader.

        :param credentials_path: Path to GCS service account JSON (None = use ADC).
        :param bucket_name: GCS bucket name (falls back to GCS_BUCKET env var).
        """
        bucket_name = bucket_name or os.getenv("GCS_BUCKET")
        if not bucket_name:
            raise ValueError("bucket_name must be provided or GCS_BUCKET env var must be set")
        self.bucket_name = bucket_name

        # Initialize GCS client
        if credentials_path and Path(credentials_path).exists():
            credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path)
            )
            self.client = storage.Client(credentials=credentials, project=credentials.project_id)
        else:
            # Use Application Default Credentials (ADC)
            self.client = storage.Client()

        self.bucket = self.client.bucket(bucket_name)

    def read_csv(
        self,
        gcs_path: str,
        filters: dict | None = None,
        limit: int | None = None,
        **pandas_kwargs
    ) -> pd.DataFrame:
        """
        Read CSV from GCS and optionally apply filters.

        :param gcs_path: Path to CSV in GCS bucket (e.g., 'data/database.csv').
        :param filters: Optional dict of column filters (SQL-like). For example:
            - ``{'data_envio_gte': '2024-01-01'}`` -> ``data_envio >= '2024-01-01'``
            - ``{'cnpj': '12345678000100'}`` -> ``cnpj == '12345678000100'``
            - ``{'cod_organizacao_in': ['ORG1', 'ORG2']}`` -> ``cod_organizacao in [...]``
        :param limit: Max rows to return (applied AFTER filtering).
        :param pandas_kwargs: Additional arguments passed to ``pd.read_csv()``.
        :returns: DataFrame with filtered data.

        Example::

            # Read entire CSV
            df = reader.read_csv('data/database.csv')

            # Read with filters
            df = reader.read_csv(
                'data/database.csv',
                filters={
                    'data_envio_gte': '2024-01-01',  # >= 2024-01-01
                    'cod_organizacao': 'ORG10',       # == ORG10
                },
                limit=1000
            )
        """
        print(f"[GCS] Reading CSV from gs://{self.bucket_name}/{gcs_path}")

        # Download blob to memory
        blob = self.bucket.blob(gcs_path)
        content = blob.download_as_bytes()

        # Read CSV into DataFrame
        df = pd.read_csv(io.BytesIO(content), **pandas_kwargs)
        print(f"[GCS] Loaded {len(df):,} rows from CSV")

        # Apply filters if provided
        if filters:
            df = self._apply_filters(df, filters)
            print(f"[GCS] After filtering: {len(df):,} rows")

        # Apply limit if provided
        if limit and len(df) > limit:
            df = df.head(limit)
            print(f"[GCS] Limited to {limit:,} rows")

        return df

    def _apply_filters(self, df: pd.DataFrame, filters: dict) -> pd.DataFrame:
        """
        Apply filters to DataFrame.

        Supported filter operators (as suffixes):
            - _gte: Greater than or equal (>=)
            - _lte: Less than or equal (<=)
            - _gt: Greater than (>)
            - _lt: Less than (<)
            - _in: In list
            - _not: Not equal (!=)
            - (no suffix): Equal (==)

        :param df: Input DataFrame.
        :param filters: Dict of ``{column_operator: value}``.
        :returns: Filtered DataFrame.
        """
        filtered = df.copy()

        for filter_key, filter_value in filters.items():
            # Parse filter key (column_operator)
            if filter_key.endswith('_gte'):
                column = filter_key[:-4]
                if column in filtered.columns:
                    filtered = filtered[filtered[column] >= filter_value]
            elif filter_key.endswith('_lte'):
                column = filter_key[:-4]
                if column in filtered.columns:
                    filtered = filtered[filtered[column] <= filter_value]
            elif filter_key.endswith('_gt'):
                column = filter_key[:-3]
                if column in filtered.columns:
                    filtered = filtered[filtered[column] > filter_value]
            elif filter_key.endswith('_lt'):
                column = filter_key[:-3]
                if column in filtered.columns:
                    filtered = filtered[filtered[column] < filter_value]
            elif filter_key.endswith('_in'):
                column = filter_key[:-3]
                if column in filtered.columns:
                    filtered = filtered[filtered[column].isin(filter_value)]
            elif filter_key.endswith('_not'):
                column = filter_key[:-4]
                if column in filtered.columns:
                    filtered = filtered[filtered[column] != filter_value]
            else:
                # Exact match (==)
                column = filter_key
                if column in filtered.columns:
                    filtered = filtered[filtered[column] == filter_value]

        return filtered

    def list_csvs(self, prefix: str = "") -> list:
        """
        List all CSV files in bucket.

        :param prefix: Optional prefix to filter (e.g., 'data/').
        :returns: List of CSV file paths.
        """
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        return [blob.name for blob in blobs if blob.name.endswith('.csv')]
