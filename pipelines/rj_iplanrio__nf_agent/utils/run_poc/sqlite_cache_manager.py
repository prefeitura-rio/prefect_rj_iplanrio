"""
SQLite database manager for caching API inputs and outputs.
Implements two-table design for efficient caching and reuse.
"""

import hashlib
import json
import random
import sqlite3
import time
from pathlib import Path
from typing import Any


class DatabaseManager:
    """Manages SQLite database for API call caching."""

    def __init__(self, db_path: Path):
        """
        Initialize database connection and create tables if needed.

        :param db_path: Path to SQLite database file.
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            str(self.db_path), timeout=120.0, check_same_thread=False
        )  # Intra-PDF page-level parallelism
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=120000")  # 120 second busy timeout
        self.conn.row_factory = sqlite3.Row  # Access columns by name
        self._create_tables()

    def _create_tables(self):
        """Create database tables if they don't exist."""
        cursor = self.conn.cursor()

        # Table 1: API Inputs (preprocessed data - page images, filtered PDFs)
        # NOTE: content_blob is nullable - we only store the hash for cache lookups
        # TODO: review database schema (page_references has a lot of redundancy with this table)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_inputs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                input_type TEXT NOT NULL,  -- 'classification_page' or 'extraction_filtered_pdf'
                pdf_name TEXT NOT NULL,
                page_number INTEGER,  -- NULL for extraction (full filtered PDF)
                content_hash TEXT NOT NULL,  -- SHA256 hash for deduplication (not unique - multiple pages can have same hash)
                content_blob BLOB,  -- DEPRECATED: No longer stored (nullable for backwards compat)
                metadata TEXT,  -- JSON string with additional metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Index for fast lookup by pdf_name and page_number
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_inputs_lookup
            ON api_inputs(pdf_name, page_number, input_type)
        """)

        # Index for fast lookup by content hash
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_inputs_hash
            ON api_inputs(content_hash)
        """)

        # Table 2: Page References (maps pdf_name+page_number to content_hash)
        # This avoids duplicating large content_blob for pages with identical content
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS page_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pdf_name TEXT NOT NULL,
                page_number INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                input_type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (pdf_name, page_number, input_type),
                FOREIGN KEY (content_hash) REFERENCES api_inputs(content_hash)
            )
        """)

        # Index for fast lookup by (pdf_name, page_number)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_page_refs_lookup
            ON page_references(pdf_name, page_number, input_type)
        """)

        # Index for fast lookup by content_hash (find all pages with same content)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_page_refs_hash
            ON page_references(content_hash)
        """)

        # Table 3: API Outputs (LLM responses)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_outputs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                input_id INTEGER NOT NULL,
                model_name TEXT NOT NULL,
                response_text TEXT NOT NULL,  -- Raw JSON response from LLM
                usage_metadata TEXT,  -- JSON string with token counts
                elapsed_seconds REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (input_id) REFERENCES api_inputs(id)
            )
        """)

        # Index for fast lookup by input_id
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_outputs_input
            ON api_outputs(input_id)
        """)

        self.conn.commit()

    def compute_content_hash(self, content: bytes) -> str:
        """
        Compute SHA256 hash of content for deduplication.

        :param content: Binary content to hash.
        :returns: Hexadecimal hash string.
        """
        return hashlib.sha256(content).hexdigest()

    # TODO: rethink the content and content_hash_override parameters,
    # content_hash_override is used only on the extraction and for the
    # extraction there's no need for a hash. It would be simpler to just
    # check the pdf name for extraction caching.
    def get_or_create_input(
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

        This method implements a two-tier caching strategy:
        1. api_inputs: Stores unique content blobs (deduplicated by content_hash)
        2. page_references: Maps (pdf_name, page_number) → content_hash

        This avoids duplicating large blobs when multiple PDFs have identical pages.

        :param input_type: 'classification_page' or 'extraction_filtered_pdf'.
        :param pdf_name: Name of the PDF file.
        :param content: Preprocessed input content (page image or filtered PDF).
        :param page_number: Page number for classification, None for extraction.
        :param metadata: Additional metadata to store.
        :param content_hash_override: If provided, use this hash instead of computing from content.
            Useful for extraction where content is just a placeholder.
        :returns: Tuple of (input_id, is_new_blob, cached_pdf_name, cached_page_number):
            - input_id: ID of the api_inputs record
            - is_new_blob: True if new content blob was created, False if blob already existed
            - cached_pdf_name: PDF name that originally cached this content (None if this is first)
            - cached_page_number: Page number that originally cached this content (None if this is first)
        """
        max_retries = 10
        base_delay = 0.1

        for attempt in range(max_retries):
            try:
                return self._get_or_create_input_impl(
                    input_type, pdf_name, content, page_number, metadata, content_hash_override
                )
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                    time.sleep(delay)
                    continue
                raise

    def _get_or_create_input_impl(
        self,
        input_type: str,
        pdf_name: str,
        content: bytes,
        page_number: int | None = None,
        metadata: dict[str, Any] | None = None,
        content_hash_override: str | None = None,
    ) -> tuple[int, bool, str | None, int | None]:
        """Internal implementation of get_or_create_input."""
        # Use override hash if provided, otherwise compute from content
        if content_hash_override:
            content_hash = content_hash_override
        else:
            content_hash = self.compute_content_hash(content)
        cursor = self.conn.cursor()

        # Step 1: Get or create api_inputs row (by content_hash)
        # Use INSERT OR IGNORE to handle race conditions where multiple threads
        # try to insert the same content_hash simultaneously
        cursor.execute("SELECT id FROM api_inputs WHERE content_hash = ?", (content_hash,))
        row = cursor.fetchone()

        if row:
            # Hash already exists
            input_id = row["id"]
            is_new_blob = False
        else:
            # Create new entry in api_inputs (no blob stored - only hash for lookups)
            # Use INSERT OR IGNORE to handle race condition gracefully
            metadata_json = json.dumps(metadata) if metadata else None
            try:
                cursor.execute(
                    """
                    INSERT INTO api_inputs (input_type, pdf_name, page_number, content_hash, content_blob, metadata)
                    VALUES (?, ?, ?, ?, NULL, ?)
                    """,
                    (input_type, pdf_name, page_number, content_hash, metadata_json),
                )
                input_id = cursor.lastrowid
                is_new_blob = True
            except sqlite3.IntegrityError:
                # Race condition: another thread inserted while we were checking
                # Just fetch the existing record
                cursor.execute("SELECT id FROM api_inputs WHERE content_hash = ?", (content_hash,))
                row = cursor.fetchone()
                input_id = row["id"]
                is_new_blob = False

        # Step 2: Check if THIS (pdf_name, page_number) already has a page reference
        # Skip if page_number is None (extraction doesn't use page references)
        if page_number is not None:
            cursor.execute(
                "SELECT id FROM page_references WHERE pdf_name = ? AND page_number = ? AND input_type = ?",
                (pdf_name, page_number, input_type),
            )
            existing_ref = cursor.fetchone()

            if not existing_ref:
                # Create new page reference
                cursor.execute(
                    """
                    INSERT INTO page_references (pdf_name, page_number, content_hash, input_type)
                    VALUES (?, ?, ?, ?)
                    """,
                    (pdf_name, page_number, content_hash, input_type),
                )

        # Step 3: Find which page originally cached this content (if any)
        # This is the page reference that was created FIRST for this content_hash
        if page_number is not None:
            cursor.execute(
                """
                SELECT pdf_name, page_number FROM page_references
                WHERE content_hash = ? AND NOT (pdf_name = ? AND page_number = ?)
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (content_hash, pdf_name, page_number),
            )
            cached_source = cursor.fetchone()
        else:
            cached_source = None

        self.conn.commit()

        if cached_source:
            return (input_id, is_new_blob, cached_source["pdf_name"], cached_source["page_number"])
        else:
            return (input_id, is_new_blob, None, None)

    def get_output(self, input_id: int) -> dict[str, Any] | None:
        """
        Get cached output for a given input ID.

        :param input_id: ID of the input record.
        :returns: Dictionary with response data if found, None otherwise.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT model_name, response_text, usage_metadata, elapsed_seconds, created_at
            FROM api_outputs
            WHERE input_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (input_id,),
        )
        row = cursor.fetchone()

        if not row:
            return None

        return {
            "model_name": row["model_name"],
            "response_text": row["response_text"],
            "usage_metadata": json.loads(row["usage_metadata"]) if row["usage_metadata"] else None,
            "elapsed_seconds": row["elapsed_seconds"],
            "created_at": row["created_at"],
            "cached": True,
        }

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
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT o.response_text, pr.pdf_name as cached_pdf_name, pr.page_number as cached_page_num
            FROM page_references pr
            JOIN api_inputs i ON pr.content_hash = i.content_hash
            JOIN api_outputs o ON i.id = o.input_id
            WHERE pr.pdf_name = ? AND pr.page_number = ? AND pr.input_type = 'classification_page'
            LIMIT 1
            """,
            (pdf_name, page_number),
        )
        row = cursor.fetchone()

        if row:
            try:
                response = json.loads(row["response_text"])
                return {
                    "category": response.get("categoria", ""),
                    "justification": response.get("justificativa", ""),
                    "cached_pdf_name": row["cached_pdf_name"],
                    "cached_page_num": row["cached_page_num"],
                }
            except (json.JSONDecodeError, KeyError):
                return None
        return None

    def save_output(
        self,
        input_id: int,
        model_name: str,
        response_text: str,
        usage_metadata: dict[str, Any] | None = None,
        elapsed_seconds: float | None = None,
    ):
        """
        Save API output to database.

        :param input_id: ID of the corresponding input.
        :param model_name: Name of the LLM model used.
        :param response_text: Raw JSON response from LLM.
        :param usage_metadata: Token usage information.
        :param elapsed_seconds: Time taken for API call.
        """
        cursor = self.conn.cursor()
        usage_json = json.dumps(usage_metadata) if usage_metadata else None

        cursor.execute(
            """
            INSERT INTO api_outputs (input_id, model_name, response_text, usage_metadata, elapsed_seconds)
            VALUES (?, ?, ?, ?, ?)
            """,
            (input_id, model_name, response_text, usage_json, elapsed_seconds),
        )
        self.conn.commit()

    def get_statistics(self) -> dict[str, Any]:
        """
        Get database statistics.

        :returns: Dictionary with cache statistics.
        """
        cursor = self.conn.cursor()

        # Count inputs by type
        cursor.execute("""
            SELECT input_type, COUNT(*) as count
            FROM api_inputs
            GROUP BY input_type
        """)
        inputs_by_type = {row["input_type"]: row["count"] for row in cursor.fetchall()}

        # Count outputs
        cursor.execute("SELECT COUNT(*) as count FROM api_outputs")
        total_outputs = cursor.fetchone()["count"]

        return {
            "total_inputs": sum(inputs_by_type.values()),
            "inputs_by_type": inputs_by_type,
            "total_outputs": total_outputs,
            "cache_hit_rate": f"{(total_outputs / max(sum(inputs_by_type.values()), 1)) * 100:.1f}%",
        }

    def close(self):
        """Close database connection."""
        self.conn.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
