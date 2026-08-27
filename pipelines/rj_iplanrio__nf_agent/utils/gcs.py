"""
PDF-specific GCS downloader.

Thin subclass of ``iplanrio_agent_toolkit.gcs.GCSDownloader`` that restores the
PDF-domain conveniences the generic toolkit class deliberately dropped
(automatic ``.pdf`` extension inference, ``pdfs``-prefixed default base path,
and ``*.pdf``-only local cleanup) — see ``MIGRATION_PLAN.md`` in
iplanrio-agent-toolkit for why those stayed out of the generic class.

``download_pdfs_batch`` cannot simply delegate to the toolkit's
``download_files_batch`` because the GCS blob path (unsuffixed, as stored) and
the local filename (always ``.pdf``-suffixed) can differ — it reimplements the
same concurrent-download flow directly against ``self.bucket``.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from iplanrio_agent_toolkit.gcs import GCSDownloader as _BaseGCSDownloader

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)
# TODO(Trick): logger da iplanrio não exibe logs de nível INFO no Prefect
# (bug em investigação). Workaround temporário: usamos logger.warning()
# nos lugares que logicamente seriam logger.info() abaixo. Reverter para
# logger.info() quando o bug for corrigido.

_MAX_FAILURES_LOGGED = 5


class GCSDownloader(_BaseGCSDownloader):
    """Downloads PDF files from a Google Cloud Storage bucket."""

    def __init__(self, credentials_path: Path | None, bucket_name: str | None = None) -> None:
        """
        Initialize GCS client.

        :param credentials_path: Path to GCS service account JSON file (None = use ADC).
        :param bucket_name: Name of the GCS bucket (e.g., 'my-gcs-bucket').
        """
        super().__init__(credentials_path=credentials_path, bucket_name=bucket_name)
        self.default_base_path = "pdfs"

    def download_pdf_by_name(self, pdf_name: str, local_dir: Path, base_path: str | None = None) -> Path:
        """
        Download a PDF by filename from the default bucket path.

        Ensures the downloaded local file always has a ``.pdf`` extension,
        even if ``pdf_name`` (as stored in GCS) doesn't include one.

        :param pdf_name: Name of the PDF file (exactly as stored in GCS).
        :param local_dir: Local directory to save the file.
        :param base_path: Base path in GCS bucket.
        :returns: Path to downloaded file.
        """
        base_path = self.default_base_path if base_path is None else base_path
        local_filename = pdf_name if pdf_name.lower().endswith(".pdf") else f"{pdf_name}.pdf"
        blob_path = f"{base_path}/{pdf_name}" if base_path else pdf_name
        return self.download_file(blob_path, local_dir, filename=local_filename)

    def download_pdfs_batch(
        self,
        pdf_names: list[str],
        local_dir: Path,
        base_path: str | None = None,
        batch_size: int = 20,
        skip_existing: bool = True,
    ) -> dict[str, Path]:
        """
        Download multiple PDFs using concurrent downloads to reduce total time.

        The GCS blob path is always ``{base_path}/{pdf_name}`` (unsuffixed, as
        stored in the bucket), while the local file always gets a ``.pdf``
        extension — these can differ, which is why this doesn't delegate to
        the generic ``download_files_batch``.

        :param pdf_names: List of PDF filenames to download.
        :param local_dir: Local directory to save files.
        :param base_path: Base path in GCS bucket.
        :param batch_size: Number of downloads per batch (for progress reporting).
        :param skip_existing: If True, skip downloading files that already exist
            locally (default: True).
        :returns: Dictionary mapping pdf_name to local_path for successful downloads.
            Failed downloads are omitted from result.
        """
        base_path = self.default_base_path if base_path is None else base_path
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        results: dict[str, Path] = {}
        to_download: list[str] = []
        for pdf_name in pdf_names:
            local_filename = pdf_name if pdf_name.lower().endswith(".pdf") else f"{pdf_name}.pdf"
            local_path = local_dir / local_filename
            if skip_existing and local_path.exists() and local_path.stat().st_size > 0:
                results[pdf_name] = local_path
            else:
                to_download.append(pdf_name)

        if skip_existing and results:
            logger.warning("Skipped %d already-downloaded PDFs", len(results))
        if not to_download:
            logger.warning("All PDFs already exist locally, skipping download")
            return results

        logger.warning("Downloading %d new PDFs...", len(to_download))

        def download_single(pdf_name: str) -> tuple:
            try:
                blob_path = f"{base_path}/{pdf_name}" if base_path else pdf_name
                local_filename = pdf_name if pdf_name.lower().endswith(".pdf") else f"{pdf_name}.pdf"
                local_path = local_dir / local_filename
                self.bucket.blob(blob_path).download_to_filename(str(local_path))
                return (pdf_name, local_path, None)
            except Exception as e:
                return (pdf_name, None, str(e))

        failed: list[tuple] = []
        completed = 0
        with ThreadPoolExecutor(max_workers=min(batch_size, len(to_download))) as executor:
            futures = {executor.submit(download_single, name): name for name in to_download}
            for future in as_completed(futures):
                pdf_name, local_path, error = future.result()
                completed += 1
                if local_path is not None:
                    results[pdf_name] = local_path
                else:
                    failed.append((pdf_name, error))

                if completed % batch_size == 0 or completed == len(to_download):
                    logger.warning("Downloaded %d/%d PDFs", completed, len(to_download))

        if failed:
            logger.warning("%d downloads failed:", len(failed))
            for pdf_name, error in failed[:_MAX_FAILURES_LOGGED]:
                logger.warning("  - %s: %s", pdf_name, error)
            if len(failed) > _MAX_FAILURES_LOGGED:
                logger.warning("  ... and %d more failures", len(failed) - _MAX_FAILURES_LOGGED)

        return results

    def get_available_pdf_filenames(self, prefix: str = "pdfs/") -> set[str]:
        """
        Get set of all available PDF filenames (without path prefix).

        :param prefix: Prefix to filter blobs (default: 'pdfs/').
        :returns: Set of filenames.
        """
        return self.get_available_filenames(prefix=prefix)

    def get_available_pdf_filenames_from_csv(self, csv_path: Path | None = None) -> set[str]:
        """
        Get set of all available PDF filenames from a pre-generated CSV file.

        :param csv_path: Path to CSV file (default: run_poc/gcs-pdf-list.csv).
        :returns: Set of filenames.
        """
        if csv_path is None:
            csv_path = Path(__file__).parent / "gcs-pdf-list.csv"
        return self.get_available_filenames_from_csv(csv_path, filename_column="filename")

    def cleanup_local_dir(self, dir_path: Path, recursive: bool = False) -> None:
        """
        Clean up local directory, removing only ``*.pdf`` files unless recursive.

        :param dir_path: Path to directory to clean.
        :param recursive: If True, remove directory and all contents.
        """
        super().cleanup_local_dir(dir_path, recursive=recursive, pattern="*.pdf")
