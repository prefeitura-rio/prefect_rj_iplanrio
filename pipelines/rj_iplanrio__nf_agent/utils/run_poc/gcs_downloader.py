"""
Google Cloud Storage downloader for PDF files.
Downloads PDFs on-demand and provides cleanup utilities.
"""

from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account


class GCSDownloader:
    """Downloads PDF files from Google Cloud Storage bucket."""

    def __init__(
        self,
        credentials_path: Path | None,
        bucket_name: str | None = None,
    ):
        """
        Initialize GCS client.

        :param credentials_path: Path to GCS service account JSON file (None = use ADC).
        :param bucket_name: Name of the GCS bucket (e.g., 'my-gcs-bucket').
        """
        self.credentials_path = Path(credentials_path) if credentials_path else None
        self.bucket_name = bucket_name
        self.default_base_path = "pdfs"
        self._client = None
        self._bucket = None

    @property
    def client(self):
        """Lazy load GCS client."""
        if self._client is None:
            if self.credentials_path is not None:
                if not self.credentials_path.exists():
                    raise FileNotFoundError(
                        f"GCS credentials not found at: {self.credentials_path}\n"
                        f"Please fill in the service account credentials."
                    )
                credentials = service_account.Credentials.from_service_account_file(
                    str(self.credentials_path)
                )
                self._client = storage.Client(credentials=credentials)
            else:
                # Application Default Credentials (covers Infisical-injected creds,
                # GCP VM/pod service accounts, and gcloud auth application-default login)
                self._client = storage.Client()

        return self._client

    @property
    def bucket(self):
        """Lazy load GCS bucket."""
        if self._bucket is None:
            self._bucket = self.client.bucket(self.bucket_name)
        return self._bucket

    def download_pdf(
        self,
        blob_path: str,
        local_dir: Path,
        filename: str | None = None
    ) -> Path:
        """
        Download a PDF from GCS to local directory.

        :param blob_path: Path to file in GCS bucket (e.g., 'pdfs/file.pdf').
        :param local_dir: Local directory to save the file.
        :param filename: Optional custom filename, otherwise uses blob name.
        :returns: Path to downloaded file.
        :raises FileNotFoundError: If blob doesn't exist in GCS.
        """
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        # Get blob from bucket
        blob = self.bucket.blob(blob_path)

        if not blob.exists():
            raise FileNotFoundError(f"Blob not found in GCS: {blob_path}")

        # Determine local filename
        if filename:
            local_path = local_dir / filename
        else:
            # Use blob's basename
            local_path = local_dir / Path(blob_path).name

        # Download
        blob.download_to_filename(str(local_path))

        return local_path

    def download_pdf_by_name(
        self,
        pdf_name: str,
        local_dir: Path,
        base_path: str = None
    ) -> Path:
        """
        Download a PDF by filename from the default bucket path.

        :param pdf_name: Name of the PDF file (exactly as stored in GCS).
        :param local_dir: Local directory to save the file.
        :param base_path: Base path in GCS bucket.
        :returns: Path to downloaded file.
        """
        if base_path is None:
            base_path = self.default_base_path
        blob_path = f"{base_path}/{pdf_name}"

        # For local storage, ensure we have .pdf extension
        local_filename = pdf_name if pdf_name.lower().endswith('.pdf') else f"{pdf_name}.pdf"

        return self.download_pdf(blob_path, local_dir, filename=local_filename)

    def download_pdfs_batch(
        self,
        pdf_names: list,
        local_dir: Path,
        base_path: str = None,
        batch_size: int = 20,
        skip_existing: bool = True
    ) -> dict:
        """
        Download multiple PDFs using concurrent downloads to reduce total time.

        :param pdf_names: List of PDF filenames to download.
        :param local_dir: Local directory to save files.
        :param base_path: Base path in GCS bucket.
        :param batch_size: Number of downloads per batch (for progress reporting).
        :param skip_existing: If True, skip downloading files that already exist
            locally (default: True).
        :returns: Dictionary mapping pdf_name to local_path for successful downloads.
            Failed downloads are omitted from result.
        :note: Downloads PDFs concurrently to minimize total download time.
            Batch size is used for progress reporting only.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if base_path is None:
            base_path = self.default_base_path
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        results = {}
        failed = []

        # Check for existing files first
        to_download = []
        for pdf_name in pdf_names:
            local_filename = pdf_name if pdf_name.lower().endswith('.pdf') else f"{pdf_name}.pdf"
            local_path = local_dir / local_filename

            if skip_existing and local_path.exists() and local_path.stat().st_size > 0:
                # File already exists and has content, skip download
                results[pdf_name] = local_path
            else:
                # Need to download
                to_download.append(pdf_name)

        if skip_existing and results:
            print(f"  [Skipped {len(results)} already-downloaded PDFs]")

        if not to_download:
            print("  [All PDFs already exist locally, skipping download]")
            return results

        print(f"  [Downloading {len(to_download)} new PDFs...]")

        def download_single(pdf_name: str) -> tuple:
            """Download a single PDF and return (pdf_name, local_path) or (pdf_name, None) if failed."""
            try:
                blob_path = f"{base_path}/{pdf_name}"
                local_filename = pdf_name if pdf_name.lower().endswith('.pdf') else f"{pdf_name}.pdf"
                local_path = local_dir / local_filename

                blob = self.bucket.blob(blob_path)
                blob.download_to_filename(str(local_path))
                return (pdf_name, local_path)
            except Exception as e:
                return (pdf_name, None, str(e))

        # Download using thread pool (up to batch_size concurrent downloads)
        with ThreadPoolExecutor(max_workers=min(batch_size, len(to_download))) as executor:
            futures = {executor.submit(download_single, name): name for name in to_download}

            completed = 0
            for future in as_completed(futures):
                result = future.result()
                if len(result) == 2:  # Success
                    pdf_name, local_path = result
                    results[pdf_name] = local_path
                    completed += 1
                else:  # Failed
                    pdf_name, _, error = result
                    failed.append((pdf_name, error))
                    completed += 1

                # Progress reporting every batch_size PDFs
                if completed % batch_size == 0 or completed == len(to_download):
                    print(f"  [Downloaded {completed}/{len(to_download)} PDFs]")

        if failed:
            print(f"  [Warning] {len(failed)} downloads failed:")
            for pdf_name, error in failed[:5]:
                print(f"    - {pdf_name}: {error}")
            if len(failed) > 5:
                print(f"    ... and {len(failed) - 5} more failures")

        return results

    def cleanup_local_file(self, file_path: Path):
        """
        Delete a local file.

        :param file_path: Path to file to delete.
        """
        file_path = Path(file_path)
        if file_path.exists():
            file_path.unlink()

    def cleanup_local_dir(self, dir_path: Path, recursive: bool = False):
        """
        Clean up local directory.

        :param dir_path: Path to directory to clean.
        :param recursive: If True, remove directory and all contents.
        """
        dir_path = Path(dir_path)

        if not dir_path.exists():
            return

        if recursive:
            import shutil
            shutil.rmtree(dir_path)
        else:
            # Remove only PDF files
            for pdf_file in dir_path.glob('*.pdf'):
                pdf_file.unlink()

    def blob_exists(self, blob_path: str) -> bool:
        """
        Check if a blob exists in GCS.

        :param blob_path: Path to file in GCS bucket.
        :returns: True if blob exists, False otherwise.
        """
        blob = self.bucket.blob(blob_path)
        return blob.exists()

    def list_blobs(self, prefix: str = "") -> list:
        """
        List all blobs with given prefix.

        :param prefix: Prefix to filter blobs (e.g., 'pdfs/').
        :returns: List of blob names.
        """
        blobs = self.bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]

    def get_available_pdf_filenames(self, prefix: str = "pdfs/") -> set:
        """
        Get set of all available PDF filenames (without path prefix).

        This method fetches all blob names with the given prefix and extracts
        just the filenames for fast lookup. Use this instead of repeated
        blob_exists() calls for better performance.

        :param prefix: Prefix to filter blobs (default: 'pdfs/').
        :returns: Set of filenames (e.g., ``{'0002_9615_AP51_...', '00091_NF_FAS_...'}``).
        """
        all_blobs = self.list_blobs(prefix=prefix)

        # Extract just filename (remove prefix)
        # Handle both with and without trailing slash
        prefix_normalized = prefix.rstrip('/') + '/'
        filenames = set()

        for blob in all_blobs:
            # Remove the prefix to get just the filename
            if blob.startswith(prefix_normalized):
                filename = blob[len(prefix_normalized):]
            elif blob.startswith(prefix):
                filename = blob[len(prefix):]
            else:
                filename = blob

            # Only add non-empty filenames
            if filename:
                filenames.add(filename)

        return filenames

    def get_available_pdf_filenames_from_csv(
        self,
        csv_path: Path = None
    ) -> set:
        """
        Get set of all available PDF filenames from pre-generated CSV file.

        This is much faster than calling get_available_pdf_filenames() which
        makes a GCS API call to list all blobs.

        :param csv_path: Path to CSV file (default: run_poc/gcs-pdf-list.csv).
        :returns: Set of filenames (e.g., ``{'0002_9615_AP51_...', '171_Nova _Fevereiro_...'}``).
        """
        import pandas as pd

        # Default to gcs-pdf-list.csv in same directory as this file
        if csv_path is None:
            csv_path = Path(__file__).parent / "gcs-pdf-list.csv"

        if not Path(csv_path).exists():
            print(f"[GCSDownloader] {csv_path} not found — falling back to GCS API listing")
            return self.get_available_pdf_filenames()

        # Read CSV and extract filename column
        df = pd.read_csv(csv_path)
        filenames = set(df['filename'].dropna().unique())

        return filenames
