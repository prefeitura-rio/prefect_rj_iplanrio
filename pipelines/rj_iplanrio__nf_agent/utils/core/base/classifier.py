"""
Base Classifier Interface - All classifiers must implement this interface
to be compatible with the NFPipeline.
"""

from abc import ABC, abstractmethod


class BaseClassifier(ABC):
    """
    Abstract base class for page classifiers.

    All classifiers (OCR-based, Vision-based, etc.) must implement this interface
    to be compatible with NFPipeline.
    """

    @abstractmethod
    def classify(self, page_input) -> tuple[str, float]:
        """
        Classify a single page.

        :param page_input: Page data (text for OCR-based, image bytes for vision-based).
        :returns: Tuple of (classification: str, score: float):
            - classification: "NF", "Non-NF", or "Uncertain"
            - score: Confidence score (higher = more likely NF).
        """
        pass

    @abstractmethod
    def classify_pages(self, inputs: list) -> list[dict]:
        """
        Classify multiple pages.

        :param inputs: List of page inputs (texts or image bytes).
        :returns: List of classification results with:
            - page: int (1-indexed page number)
            - classification: str ("NF", "Non-NF", "Uncertain")
            - score: float
            - is_nf: bool
            - categories: List[str] (optional, for multi-label classifiers).
        """
        pass

    @abstractmethod
    def get_nf_pages(self, inputs: list) -> list[int]:
        """
        Get list of page numbers classified as NF.

        :param inputs: List of page inputs.
        :returns: List of 1-indexed page numbers that are NFs.
        """
        pass

    @property
    @abstractmethod
    def requires_ocr(self) -> bool:
        """
        Whether this classifier requires OCR preprocessing.

        :returns: True if classifier needs OCR text, False if it works directly with images.
        """
        pass
