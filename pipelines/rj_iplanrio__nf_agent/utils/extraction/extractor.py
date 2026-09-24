"""
NF Data Extractor using the ``NFExtractor`` class.

``NFExtractor`` composes plain-function modules (``auth``, ``prompt``, ``pdf``,
``coalesce``, ``api``) rather than mixin classes: each module operates on an
explicit ``extractor`` instance passed as its first argument, and the class
below just holds state and delegates — preserving the public/private method
surface other modules and tests call directly (``extract_from_pdf``,
``_extract_from_pdf_bytes``, ``_create_filtered_pdf``, ``model``). ``api.py``
calls the ``coalesce``/``pdf`` module functions directly rather than through
this class, so there are no delegate wrappers for those beyond
``_create_filtered_pdf``.
"""

from pathlib import Path

from . import api, auth
from . import pdf as pdf_module


class NFExtractor:
    """Extract structured NF data using Google Gemini."""

    def __init__(
        self,
        model_name: str | None = None,
        extraction_prompt: str | None = None,
    ):
        """
        Initialize extractor with Gemini model.

        See ``auth.initialize`` for the authentication priority order and full
        parameter documentation.
        """
        auth.initialize(
            self,
            model_name=model_name,
            extraction_prompt=extraction_prompt,
        )

    @property
    def model(self):
        """Lazy load Gemini model. See ``auth.get_model``."""
        return auth.get_model(self)

    def _create_filtered_pdf(self, pdf_path: Path, pages: list[int]) -> bytes:
        """See ``pdf.create_filtered_pdf``."""
        return pdf_module.create_filtered_pdf(pdf_path, pages)

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
