"""
OCR Processor - Text extraction from PDF pages.
Supports EasyOCR and PaddleOCR engines.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pdf2image import convert_from_path
from PIL import Image

logger = logging.getLogger(__name__)

# Lazy load OCR engines to avoid import errors if not installed
_easyocr_reader = None
_paddleocr_engine = None


def get_easyocr_reader(languages: list[str] | None = None, gpu: bool = True):
    """
    Get or create EasyOCR reader instance (singleton pattern).

    :param languages: List of language codes (default: ['pt', 'en']).
    :param gpu: Whether to use GPU acceleration.
    :returns: EasyOCR Reader instance.
    """
    global _easyocr_reader

    if _easyocr_reader is None:
        import easyocr
        langs = languages or ['pt', 'en']
        _easyocr_reader = easyocr.Reader(langs, gpu=gpu)

    return _easyocr_reader


@dataclass(frozen=True)
class PaddleOCRConfig:
    """Configuration for the PaddleOCR engine."""

    lang: str = "en"
    ocr_version: str = "PP-OCRv5"
    use_doc_orientation_classify: bool = True
    use_doc_unwarping: bool = True
    use_textline_orientation: bool = True
    textline_orientation_batch_size: int = 1
    text_recognition_batch_size: int = 1
    gpu: bool = True


def get_paddleocr_engine(config: PaddleOCRConfig | None = None):
    """
    Get or create PaddleOCR engine instance (singleton pattern).

    :param config: PaddleOCR configuration (default: English, PP-OCRv5, GPU on).
    :returns: PaddleOCR instance.
    """
    global _paddleocr_engine

    config = config or PaddleOCRConfig()

    if _paddleocr_engine is None:
        import paddle
        from paddleocr import PaddleOCR

        logger.info("Checking GPU availability for PaddleOCR...")

        if not paddle.is_compiled_with_cuda():
            logger.warning("PaddlePaddle was installed WITHOUT GPU support. Using CPU.")
            paddle.set_device("cpu")
        elif config.gpu:
            try:
                paddle.set_device("gpu")
                logger.info("PaddleOCR using device: %s", paddle.device.get_device())
            except Exception as e:
                logger.warning("Could not use GPU, falling back to CPU: %s", e)
                paddle.set_device("cpu")
        else:
            paddle.set_device("cpu")
            logger.info("PaddleOCR using CPU (GPU disabled by config)")

        _paddleocr_engine = PaddleOCR(
            use_doc_orientation_classify=config.use_doc_orientation_classify,
            use_doc_unwarping=config.use_doc_unwarping,
            use_textline_orientation=config.use_textline_orientation,
            textline_orientation_batch_size=config.textline_orientation_batch_size,
            text_recognition_batch_size=config.text_recognition_batch_size,
            lang=config.lang,
            ocr_version=config.ocr_version
        )

    return _paddleocr_engine


class OCRProcessor:
    """
    OCR processor supporting EasyOCR and PaddleOCR for text extraction from PDFs.
    """

    def __init__(
        self,
        languages: list[str] = None,
        gpu: bool = True,
        dpi: int = 200,
        engine: Literal["easyocr", "paddleocr"] = "easyocr",
        paddleocr_config: dict = None,
    ):
        """
        Initialize OCR processor.

        :param languages: List of language codes for EasyOCR (default: ['pt', 'en']).
        :param gpu: Whether to use GPU acceleration.
        :param dpi: DPI for PDF to image conversion.
        :param engine: OCR engine to use ('easyocr' or 'paddleocr').
        :param paddleocr_config: Configuration dict for PaddleOCR (optional).
        """
        self.languages = languages or ['pt', 'en']
        self.gpu = gpu
        self.dpi = dpi
        self.engine = engine
        self.paddleocr_config = paddleocr_config or {
            "lang": "en",
            "ocr_version": "PP-OCRv5",
            "use_doc_orientation_classify": True,
            "use_doc_unwarping": True,
            "use_textline_orientation": True,
            "textline_orientation_batch_size": 1,
            "text_recognition_batch_size": 1
        }
        self._reader = None
        self._paddle_engine = None

    @property
    def reader(self):
        """Lazy load EasyOCR reader."""
        if self._reader is None:
            self._reader = get_easyocr_reader(self.languages, self.gpu)
        return self._reader

    @property
    def paddle_engine(self):
        """Lazy load PaddleOCR engine."""
        if self._paddle_engine is None:
            self._paddle_engine = get_paddleocr_engine(
                PaddleOCRConfig(
                    lang=self.paddleocr_config.get("lang", "en"),
                    ocr_version=self.paddleocr_config.get("ocr_version", "PP-OCRv5"),
                    use_doc_orientation_classify=self.paddleocr_config.get("use_doc_orientation_classify", True),
                    use_doc_unwarping=self.paddleocr_config.get("use_doc_unwarping", True),
                    use_textline_orientation=self.paddleocr_config.get("use_textline_orientation", True),
                    textline_orientation_batch_size=self.paddleocr_config.get("textline_orientation_batch_size", 1),
                    text_recognition_batch_size=self.paddleocr_config.get("text_recognition_batch_size", 1),
                    gpu=self.gpu,
                )
            )
        return self._paddle_engine

    def ocr_image(self, image: Image.Image) -> str:
        """
        Extract text from a PIL Image using the configured OCR engine.

        :param image: PIL Image to process.
        :returns: Extracted text as string.
        """
        if self.engine == "paddleocr":
            return self._ocr_image_paddle(image)
        else:
            return self._ocr_image_easyocr(image)

    def _ocr_image_easyocr(self, image: Image.Image) -> str:
        """
        Extract text from a PIL Image using EasyOCR.

        :param image: PIL Image to process.
        :returns: Extracted text as string.
        """
        import numpy as np

        # Convert PIL Image to numpy array
        img_array = np.array(image)

        # Run OCR
        results = self.reader.readtext(img_array)

        # Extract text from results
        text_parts = [result[1] for result in results]
        return '\n'.join(text_parts)

    def _ocr_image_paddle(self, image: Image.Image) -> str:
        """
        Extract text from a PIL Image using PaddleOCR.

        :param image: PIL Image to process.
        :returns: Extracted text as string.
        """
        import numpy as np

        # PaddleOCR can work with numpy array or file path
        img_array = np.array(image)

        # Run OCR prediction
        result = self.paddle_engine.predict(img_array)

        # Extract text from PaddleOCR result structure
        text_parts = []
        if result and len(result) > 0:
            # PaddleOCR returns a list of results, each containing 'rec_texts' or similar
            for page_result in result:
                if hasattr(page_result, 'rec_texts'):
                    text_parts.extend(page_result.rec_texts)
                elif isinstance(page_result, dict) and 'rec_texts' in page_result:
                    text_parts.extend(page_result['rec_texts'])
                elif isinstance(page_result, list):
                    # Handle list format: [[box, (text, confidence)], ...]
                    for item in page_result:
                        if isinstance(item, (list, tuple)) and len(item) >= 2:
                            text_info = item[1]
                            if isinstance(text_info, (list, tuple)) and len(text_info) >= 1:
                                text_parts.append(str(text_info[0]))
                            elif isinstance(text_info, str):
                                text_parts.append(text_info)

        return '\n'.join(text_parts)

    def ocr_pdf_page(self, pdf_path: Path, page_num: int) -> str:
        """
        Extract text from a specific PDF page.

        :param pdf_path: Path to PDF file.
        :param page_num: 1-indexed page number.
        :returns: Extracted text.
        """
        # Convert specific page to image
        images = convert_from_path(
            str(pdf_path),
            first_page=page_num,
            last_page=page_num,
            dpi=self.dpi
        )

        if not images:
            return ""

        return self.ocr_image(images[0])

    def ocr_pdf(self, pdf_path: Path) -> list[str]:
        """
        Extract text from all pages of a PDF.

        :param pdf_path: Path to PDF file.
        :returns: List of extracted texts, one per page.
        """
        images = convert_from_path(str(pdf_path), dpi=self.dpi)

        page_texts = []
        for idx, image in enumerate(images):
            logger.debug("OCR page %d/%d...", idx + 1, len(images))
            text = self.ocr_image(image)
            page_texts.append(text)

        return page_texts

    def ocr_pdf_pages(
        self,
        pdf_path: Path,
        page_numbers: list[int] = None,
    ) -> list[tuple[int, str]]:
        """
        Extract text from specific PDF pages.

        :param pdf_path: Path to PDF file.
        :param page_numbers: List of 1-indexed page numbers (None = all pages).
        :returns: List of (page_number, text) tuples.
        """
        if page_numbers is None:
            texts = self.ocr_pdf(pdf_path)
            return [(i + 1, text) for i, text in enumerate(texts)]

        results = []
        for page_num in page_numbers:
            text = self.ocr_pdf_page(pdf_path, page_num)
            results.append((page_num, text))

        return results


@dataclass(frozen=True)
class OCRConfig:
    """Configuration for running OCR over a PDF via :class:`OCRProcessor`."""

    languages: list[str] | None = None
    gpu: bool = True
    dpi: int = 200
    engine: Literal["easyocr", "paddleocr"] = "easyocr"
    paddleocr_config: dict | None = None


def run_ocr_on_pdf(pdf_path: Path, config: OCRConfig | None = None) -> list[str]:
    """
    Convenience function to run OCR on a PDF file.

    :param pdf_path: Path to PDF file.
    :param config: OCR configuration (default: EasyOCR, pt/en, GPU on).
    :returns: List of extracted texts per page.
    """
    config = config or OCRConfig()
    processor = OCRProcessor(
        languages=config.languages,
        gpu=config.gpu,
        dpi=config.dpi,
        engine=config.engine,
        paddleocr_config=config.paddleocr_config,
    )
    return processor.ocr_pdf(pdf_path)


def get_page_count(pdf_path: Path) -> int:
    """Get the number of pages in a PDF."""
    import fitz  # PyMuPDF
    doc = fitz.open(str(pdf_path))
    count = len(doc)
    doc.close()
    return count


def extract_page_as_image(
    pdf_path: Path,
    page_num: int,
    dpi: int = 200
) -> Image.Image:
    """
    Extract a specific page from PDF as PIL Image.

    :param pdf_path: Path to PDF.
    :param page_num: 1-indexed page number.
    :param dpi: Conversion DPI.
    :returns: PIL Image.
    """
    images = convert_from_path(
        str(pdf_path),
        first_page=page_num,
        last_page=page_num,
        dpi=dpi
    )
    return images[0] if images else None
