"""
NF Data Extractor using the ``NFExtractor`` class.

``NFExtractor`` composes plain-function modules (``auth``, ``prompt``, ``pdf``,
``coalesce``, ``api``) rather than mixin classes: each module operates on an
explicit ``extractor`` instance passed as its first argument, and the class
below just holds state and delegates — preserving the original public/private
method surface other modules and tests call directly (e.g. ``extract_from_pdf``,
``_extract_from_pdf_bytes``, ``model``).
"""

from pathlib import Path

from . import api, auth, coalesce
from . import pdf as pdf_module
from . import prompt as prompt_module


class NFExtractor:
    """Extract structured NF data using Google Gemini."""

    def __init__(
        self,
        model_name: str | None = None,
        service_account_file: str | None = None,
        api_key: str | None = None,
        extraction_prompt: str | None = None,
        batch_size: int = 5,
    ):
        """
        Initialize extractor with Gemini model.

        See ``auth.initialize`` for the authentication priority order and full
        parameter documentation.
        """
        auth.initialize(
            self,
            model_name=model_name,
            service_account_file=service_account_file,
            api_key=api_key,
            extraction_prompt=extraction_prompt,
            batch_size=batch_size,
        )

    @property
    def model(self):
        """Lazy load Gemini model. See ``auth.get_model``."""
        return auth.get_model(self)

    def _build_prompt_with_hint(self, classification_hint: str | None = None) -> str:
        """See ``prompt.build_prompt_with_hint``."""
        return prompt_module.build_prompt_with_hint(self, classification_hint)

    def _parse_response(self, response_text: str) -> dict:
        """See ``prompt.parse_response``."""
        return prompt_module.parse_response(response_text)

    def _create_filtered_pdf(self, pdf_path: Path, pages: list[int]) -> bytes:
        """See ``pdf.create_filtered_pdf``."""
        return pdf_module.create_filtered_pdf(pdf_path, pages)

    def _split_pages_into_batches(self, pages: list[int], batch_size: int = 5) -> list[list[int]]:
        """See ``coalesce.split_pages_into_batches``."""
        return coalesce.split_pages_into_batches(pages, batch_size)

    def _coalesce_nfs_by_numero(self, all_nfs: list[dict]) -> list[dict]:
        """See ``coalesce.coalesce_nfs_by_numero``."""
        return coalesce.coalesce_nfs_by_numero(all_nfs)

    def _count_decimals(self, value: float) -> int:
        """See ``coalesce.count_decimals``."""
        return coalesce.count_decimals(value)

    def _has_suspicious_decimals(self, notas_fiscais: list[dict]) -> bool:
        """See ``coalesce.has_suspicious_decimals``."""
        return coalesce.has_suspicious_decimals(notas_fiscais)

    def _extract_from_pdf_bytes(
        self,
        pdf_bytes: bytes,
        num_pages: int,
        save_api_response: bool = False,
        api_response_path: Path | None = None,
        resolved_prompt: str | None = None,
    ) -> dict:
        """See ``api.extract_from_pdf_bytes``."""
        return api.extract_from_pdf_bytes(
            self, pdf_bytes, num_pages, save_api_response, api_response_path, resolved_prompt
        )

    def extract_from_images(self, images: list) -> dict:
        """See ``api.extract_from_images``."""
        return api.extract_from_images(self, images)

    def extract_from_pdf(
        self,
        pdf_path: Path,
        pages: list[int] | None = None,
        save_api_response: bool = False,
        api_response_output_dir: Path | None = None,
        page_classifications: dict[int, str] | None = None,
    ) -> dict:
        """See ``api.extract_from_pdf``."""
        return api.extract_from_pdf(
            self,
            pdf_path,
            pages=pages,
            save_api_response=save_api_response,
            api_response_output_dir=api_response_output_dir,
            page_classifications=page_classifications,
        )

    def extract_batch(self, pdf_dir: Path, output_dir: Path | None = None) -> list[dict]:
        """See ``api.extract_batch``."""
        return api.extract_batch(self, pdf_dir, output_dir)


def extract_nf_data(
    pdf_path: Path,
    pages: list[int] | None = None,
    service_account_file: str | None = None,
    api_key: str | None = None,
) -> dict:
    """
    Convenience function to extract NF data from a PDF.

    :param pdf_path: Path to PDF file.
    :param pages: Specific pages to process (1-indexed).
    :param service_account_file: Path to Google service account JSON.
    :param api_key: Google API key.
    :returns: Extraction result dictionary.
    """
    extractor = NFExtractor(service_account_file=service_account_file, api_key=api_key)
    return extractor.extract_from_pdf(pdf_path, pages=pages)
