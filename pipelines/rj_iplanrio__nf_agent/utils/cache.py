"""
SQLite database manager for caching API inputs and outputs.

Thin subclass of ``iplanrio_agent_toolkit.cache.SQLiteCache`` (the generic
two-tier content-addressable cache this module's original implementation was
extracted into during the toolkit migration's Fase 1). Keeps only the two
things that are genuinely NF-pipeline domain vocabulary: the
``pdf_name``/``page_number`` parameter names callers already use (mapped onto
the toolkit's generic ``item_key``/``sub_key``), and ``get_cached_classification``,
which additionally parses the cached response JSON into
``category``/``justification`` fields.
"""

import json
from typing import Any

from iplanrio_agent_toolkit.cache import SQLiteCache


class DatabaseManager(SQLiteCache):
    """Manages SQLite database for API call caching (NF-pipeline naming)."""

    def get_or_create_input(  # noqa: PLR0913, PLR0917
        # Overrides SQLiteCache.get_or_create_input, adapting its param names
        # (pdf_name -> item_key) — grouping into a dataclass here would break
        # the override's positional/keyword shape without adding clarity.
        self,
        input_type: str,
        pdf_name: str,
        content: bytes,
        page_number: int | None = None,
        metadata: dict[str, Any] | None = None,
        content_hash_override: str | None = None,
    ) -> tuple[int, bool, str | None, int | None]:
        """
        Get existing input or create new one, with page reference tracking.

        See ``SQLiteCache.get_or_create_input`` for the underlying two-tier
        caching strategy (deduplicated by content hash, referenced by
        ``(pdf_name, page_number, input_type)``).

        :param input_type: 'classification_page' or 'extraction_filtered_pdf'.
        :param pdf_name: Name of the PDF file.
        :param content: Preprocessed input content (page image or filtered PDF).
        :param page_number: Page number for classification, None for extraction.
        :param metadata: Additional metadata to store.
        :param content_hash_override: If provided, use this hash instead of computing from content.
            Useful for extraction where content is just a placeholder.
        :returns: Tuple of (input_id, is_new_blob, cached_pdf_name, cached_page_number).
        """
        return super().get_or_create_input(
            input_type=input_type,
            item_key=pdf_name,
            content=content,
            sub_key=page_number,
            metadata=metadata,
            content_hash_override=content_hash_override,
        )

    def get_cached_classification(self, pdf_name: str, page_number: int) -> dict[str, Any] | None:
        """
        Fast cache lookup by (pdf_name, page_number) without needing PDF bytes.

        This method enables skipping expensive PDF byte preparation on re-runs
        when we already have a cached result for this exact (pdf_name, page_number).

        :param pdf_name: Name of the PDF file.
        :param page_number: Page number (1-indexed).
        :returns: Dictionary with cached result if found, None otherwise:
            - category: The classification category
            - cached_pdf_name: PDF name of the original cached entry
            - cached_page_num: Page number of the original cached entry
        """
        cached = self.get_cached_output_by_reference(
            item_key=pdf_name, sub_key=page_number, input_type="classification_page"
        )
        if not cached:
            return None

        try:
            response = json.loads(cached["response_text"])
        except (json.JSONDecodeError, KeyError):
            return None

        return {
            "category": response.get("categoria", ""),
            "justification": response.get("justificativa", ""),
            "cached_pdf_name": cached["cached_item_key"],
            "cached_page_num": cached["cached_sub_key"],
        }


__all__ = ["DatabaseManager"]
