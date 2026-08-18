"""
Base Extractor Interface - Protocol for NF data extraction
"""

from pathlib import Path
from typing import Dict, List, Protocol


class BaseExtractor(Protocol):
    """
    Protocol for NF data extractors.

    Extracts structured data from NF pages (supplier, items, totals, etc.)
    """

    def extract_from_pdf(
        self,
        pdf_path: Path,
        nf_pages: List[int] = None
    ) -> Dict:
        """
        Extract NF data from PDF pages.

        :param pdf_path: Path to PDF file.
        :param nf_pages: List of NF page numbers (0-indexed). If None, extract from
            all pages.
        :returns: Dictionary with extracted NF data:
            - quantidade_notas_fiscais: int
            - notas_fiscais: List[Dict] - one per NF found
                Each NF contains:
                - numero_nf: str
                - fornecedor: str
                - valor_total: float
                - itens: List[Dict].
        """
        ...

    def extract_from_images(
        self,
        image_paths: List[Path]
    ) -> Dict:
        """
        Extract NF data from image files.

        :param image_paths: List of image file paths.
        :returns: Dictionary with extracted NF data (same format as extract_from_pdf).
        """
        ...
