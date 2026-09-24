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
import threading
from pathlib import Path
from typing import Any

from iplanrio_agent_toolkit.cache import SQLiteCache


class DatabaseManager(SQLiteCache):
    """Manages SQLite database for API call caching (NF-pipeline naming)."""

    def __init__(self, db_path: Path) -> None:
        """Open the cache database and set up its intra-instance concurrency lock.

        ``SQLiteCache``'s connection is opened with ``check_same_thread=False``
        (so a single instance CAN be handed to multiple threads), but that
        flag only disables Python's own thread-affinity check — the
        underlying SQLite C driver still isn't safe for *concurrent* calls
        from multiple threads on one connection. When that happens anyway
        (see ``self.lock``'s docstring below), it corrupts the driver's
        internal state, surfacing as ``sqlite3.InterfaceError: bad parameter
        or other API misuse`` (or similar low-level errors) instead of a
        clean, catchable condition.

        :param db_path: Path to the SQLite database file. Parent directories are created.
        """
        super().__init__(db_path)
        # Guards every ``self.conn``-touching call on THIS instance. Needed
        # because ``process_pdf``'s Step 2 (see
        # ``processing.classification_cache.classify_page_from_cache``) runs
        # up to ``POCProcessor.MAX_INTRA_PDF_WORKERS`` threads concurrently,
        # all sharing the single ``POCProcessor.db_manager`` instance/connection
        # created for that PDF (outer, per-PDF parallelism already gives each
        # thread its OWN ``DatabaseManager``/connection — see
        # ``processing.process.process_single_pdf_worker`` — so this lock only
        # ever contends between a single PDF's own inner classification workers).
        # The lock is taken only around the cache's own (fast, in-process)
        # SQLite calls, never around the slow network calls to the
        # classification/extraction APIs, so the actual parallelism this
        # worker pool exists for is preserved.
        # RLock (not Lock): get_cached_classification below calls
        # get_cached_output_by_reference while already holding the lock —
        # a plain Lock would deadlock on that re-entrant acquisition.
        self.lock = threading.RLock()

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
        with self.lock:
            return super().get_or_create_input(
                input_type=input_type,
                item_key=pdf_name,
                content=content,
                sub_key=page_number,
                metadata=metadata,
                content_hash_override=content_hash_override,
            )

    def get_output(self, input_id: int) -> dict[str, Any] | None:
        """Thread-safe wrapper around ``SQLiteCache.get_output`` — see ``self.lock``."""
        with self.lock:
            return super().get_output(input_id)

    def get_cached_output_by_reference(self, item_key: str, sub_key: str, input_type: str) -> dict[str, Any] | None:
        """Thread-safe wrapper around ``SQLiteCache.get_cached_output_by_reference`` — see ``self.lock``."""
        with self.lock:
            return super().get_cached_output_by_reference(item_key=item_key, sub_key=sub_key, input_type=input_type)

    def save_output(
        self,
        input_id: int,
        model_name: str,
        response_text: str,
        usage_metadata: dict[str, Any] | None = None,
        elapsed_seconds: float | None = None,
    ) -> None:
        """Thread-safe wrapper around ``SQLiteCache.save_output`` — see ``self.lock``."""
        with self.lock:
            super().save_output(
                input_id=input_id,
                model_name=model_name,
                response_text=response_text,
                usage_metadata=usage_metadata,
                elapsed_seconds=elapsed_seconds,
            )

    def get_statistics(self) -> dict[str, Any]:
        """Thread-safe wrapper around ``SQLiteCache.get_statistics`` — see ``self.lock``."""
        with self.lock:
            return super().get_statistics()

    def get_cached_classification(self, pdf_name: str, page_number: int) -> dict[str, Any] | None:
        """
        Fast cache lookup by (pdf_name, page_number) without needing PDF bytes.

        This method enables skipping expensive PDF byte preparation on re-runs
        when we already have a cached result for this exact (pdf_name, page_number).

        :param pdf_name: Name of the PDF file.
        :param page_number: Page number (1-indexed).
        :returns: Dictionary with cached result if found, None otherwise:
            - category: The classification category
            - justification: The classification justification
            - usage_metadata: {"input_tokens", "output_tokens", "total_tokens"} for
              this page's classification call (``{}`` for cache rows written before
              this field existed).
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
            "usage_metadata": response.get("usage_metadata") or {},
            "cached_pdf_name": cached["cached_item_key"],
            "cached_page_num": cached["cached_sub_key"],
        }


__all__ = ["DatabaseManager"]
