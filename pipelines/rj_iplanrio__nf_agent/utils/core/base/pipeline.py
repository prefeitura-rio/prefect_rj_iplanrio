"""
Base Pipeline Interface - Protocol for NF processing pipelines
"""

from pathlib import Path
from typing import Dict, List, Protocol


class BasePipeline(Protocol):
    """
    Protocol for NF processing pipelines.

    Defines the interface that all pipeline implementations should follow.
    """

    def classify_pdf(self, pdf_path: Path, output_dir: Path = None, save_results: bool = False) -> Dict:
        """
        Classify all pages of a PDF.

        :param pdf_path: Path to PDF file.
        :param output_dir: Directory to save results (optional).
        :param save_results: Whether to save results to JSON (default: False).
        :returns: Dictionary with classification results:
            - pdf_name: str
            - total_pages: int
            - nf_pages: List[int]
            - non_nf_pages: List[int]
            - pages: List[Dict] - per-page results.
        """
        ...

    def classify_batch(self, pdf_files: List[Path], output_dir: Path, skip_existing: bool = True) -> List[Dict]:
        """
        Classify multiple PDFs.

        :param pdf_files: List of PDF file paths.
        :param output_dir: Directory to save results.
        :param skip_existing: Skip already processed files.
        :returns: List of classification results (one per PDF).
        """
        ...

    def get_nf_pages(self, pdf_path: Path) -> List[int]:
        """
        Get list of NF page numbers for a PDF.

        :param pdf_path: Path to PDF file.
        :returns: List of 1-indexed page numbers classified as NF.
        """
        ...
