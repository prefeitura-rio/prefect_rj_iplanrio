"""Page-count-based session sizing for Bifrost Batch API jobs.

Bifrost's own docs (https://docs.getbifrost.ai) don't state a row-count
limit for a batch job — unlike the old direct-Vertex implementation, which
at least had Vertex's documented (if not BigQuery-path-confirmed)
200,000-request cap to size against (see this module's git history).
``MAX_CLASSIFICATION_ROWS_DEFAULT`` is therefore a conservative starting
guess for row count, not a value derived from a documented limit.

There IS a hard, empirically-confirmed limit that matters more in practice:
Bifrost rejects the whole upload request with a generic "Error when parsing
request" (no structured error body — see ``bifrost_batch.py``) somewhere
between ~99.7MB and ~101.2MB of request body — bisected directly against
the staging Bifrost instance (2026-09-19), consistent with a round 100MB
proxy/body-size limit in front of Bifrost itself. Page COUNT alone can't
protect against this: each JSONL row inlines a full base64-encoded single
PDF page (see ``classification_submit.py``), and scanned NF pages commonly
run several hundred KB to a few MB each — a 1000-row budget sized only by
page count can build a >300MB JSONL file, well past the limit, while
plenty of small-page sessions would stay far under it. ``MAX_CLASSIFICATION_BYTES_DEFAULT``
tracks estimated JSONL size alongside row count; selection stops at
whichever budget fills first.

The synchronous pipeline sizes a batch by PDF count (``batch_size``/
``max_pdfs``), which doesn't translate directly here: the classification job
submits **one row per page**, and a PDF's page count is unknown until the
file is actually opened. Selecting PDFs "count of PDFs" therefore risks
building an input file with far more (or fewer) rows than intended.
Instead, this module selects PDFs by *accumulating actual page counts and
estimated byte size*, stopping as soon as either running total would
exceed its configured budget.
"""

from dataclasses import dataclass
from pathlib import Path

import fitz  # PyMuPDF

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

# Conservative starting guess — see module docstring. Deliberately close to
# the synchronous pipeline's own default batch_size (1000 PDFs, see
# flow.py), on the assumption that "roughly as much data as one synchronous
# batch already handles" is a safe starting point, not a proven Bifrost
# limit. Override via BatchSessionBudget once a real limit is confirmed.
MAX_CLASSIFICATION_ROWS_DEFAULT = 1_000

# Empirically confirmed ceiling is ~100MB (see module docstring) — this
# default targets 60MB to leave comfortable headroom for: (a) base64
# overhead being estimated from whole-PDF file size rather than actual
# per-page-split size (splitting a multi-page PDF into single-page PDFs
# adds a bit of per-page header/xref overhead the whole-file estimate
# doesn't capture), (b) per-row JSON/prompt text overhead (small, ~2KB/row,
# but adds up across a large row count), and (c) safety margin against the
# limit being a bit under 100MB rather than exactly at it.
MAX_CLASSIFICATION_BYTES_DEFAULT = 60_000_000

# Multiplier applied to a PDF's on-disk file size to estimate its
# contribution to the JSONL's base64-encoded size. Base64 itself is 4/3
# the size of its input; the extra factor is a safety margin for the
# per-page-split overhead described above.
_BASE64_SIZE_ESTIMATE_FACTOR = 4 / 3 * 1.05


@dataclass(frozen=True)
class SessionBudget:
    """Bundles the two independent limits a batch session must fit within.

    Kept together as one value (rather than two loose parameters) mainly
    to keep call sites like ``prepare_session_pdfs`` under the project's
    max-parameter-count lint rule — see ``select_pdfs_within_row_budget``
    for why both limits are needed (row count alone doesn't protect
    against Bifrost's ~100MB request-size ceiling).
    """

    max_rows: int = MAX_CLASSIFICATION_ROWS_DEFAULT
    max_bytes: int = MAX_CLASSIFICATION_BYTES_DEFAULT


@dataclass(frozen=True)
class BatchSessionSelection:
    """Result of selecting a page-count-and-size-bounded subset of pending PDFs."""

    selected_pdf_names: list[str]  # PDFs included in this session, in input order
    total_pages: int  # sum of page counts across selected_pdf_names
    skipped_pdf_names: list[str]  # PDFs that didn't fit this session's budget (stay pending)
    unreadable_pdf_names: list[str]  # PDFs whose page count couldn't be determined (also stay pending)
    total_bytes_estimate: int = 0  # estimated JSONL size in bytes across selected_pdf_names


def _count_pages(pdf_path: Path) -> int | None:
    """Return the page count of a local PDF, or ``None`` if it can't be opened.

    :param pdf_path: Path to a local PDF file.
    :returns: Page count, or ``None`` on any read failure (corrupted file,
        wrong format, etc.) — the caller treats this PDF as unreadable and
        leaves it pending rather than failing the whole selection.
    """
    try:
        doc = fitz.open(str(pdf_path))
        try:
            return doc.page_count
        finally:
            doc.close()
    except Exception as exc:
        logger.warning("Could not open %s to count pages: %s", pdf_path, exc)
        return None


def _estimate_jsonl_bytes(pdf_path: Path) -> int:
    """Estimate this PDF's total contribution to the JSONL batch file's size.

    :param pdf_path: Path to a local PDF file.
    :returns: Estimated bytes across all of this PDF's pages once
        base64-encoded and inlined into JSONL rows — see
        ``_BASE64_SIZE_ESTIMATE_FACTOR`` for why this over-estimates the
        raw base64 math slightly (safety margin, not an exact figure).
    """
    return int(pdf_path.stat().st_size * _BASE64_SIZE_ESTIMATE_FACTOR)


def select_pdfs_within_row_budget(
    pdf_paths: dict[str, Path],
    max_rows: int = MAX_CLASSIFICATION_ROWS_DEFAULT,
    max_bytes: int = MAX_CLASSIFICATION_BYTES_DEFAULT,
) -> BatchSessionSelection:
    """Select a prefix of ``pdf_paths`` fitting both the row and byte-size budgets.

    Iterates ``pdf_paths`` in the order given (callers should pass an
    already-sorted/deterministic mapping — e.g. ``dict(sorted(...))`` — so
    repeated runs behave predictably), opening each PDF just far enough to
    read its page count, and adds it to the selection only if doing so
    would not exceed ``max_rows`` *or* ``max_bytes``. Stops accumulating as
    soon as the next candidate would overflow either budget — later
    candidates are also left out (not skipped-and-continued), since PDFs
    are typically similar in size and skipping one only to add a smaller
    one later would make session composition less predictable across runs.

    The byte budget exists because Bifrost rejects the whole upload once
    the JSONL file passes ~100MB (see module docstring) — row count alone
    doesn't protect against that, since page byte size varies a lot
    (a scanned NF page can be several hundred KB to a few MB).

    :param pdf_paths: Mapping of pending PDF name -> local downloaded path
        (already downloaded, e.g. via ``GCSDownloader.download_pdfs_batch``).
    :param max_rows: Maximum total page count (= classification request
        rows) allowed in this session.
    :param max_bytes: Maximum estimated JSONL size (bytes) allowed in this
        session — see ``MAX_CLASSIFICATION_BYTES_DEFAULT``.
    :returns: A :class:`BatchSessionSelection` with the chosen subset, page
        count, and estimated byte size.
    """
    selected: list[str] = []
    skipped: list[str] = []
    unreadable: list[str] = []
    total_pages = 0
    total_bytes_estimate = 0
    budget_exhausted = False

    for pdf_name, path in pdf_paths.items():
        if budget_exhausted:
            skipped.append(pdf_name)
            continue

        page_count = _count_pages(path)
        if page_count is None:
            unreadable.append(pdf_name)
            continue

        pdf_bytes_estimate = _estimate_jsonl_bytes(path)

        if (
            total_pages + page_count > max_rows
            or total_bytes_estimate + pdf_bytes_estimate > max_bytes
        ):
            # Stop accumulating entirely (don't skip-and-continue looking for
            # a smaller later candidate that might still fit) — see
            # docstring for why: keeps session composition predictable
            # across runs instead of depending on input ordering quirks.
            skipped.append(pdf_name)
            budget_exhausted = True
            continue

        selected.append(pdf_name)
        total_pages += page_count
        total_bytes_estimate += pdf_bytes_estimate

    logger.warning(
        "Session sizing: %d PDFs selected (%d pages, ~%.1fMB / budget=%d rows, %.1fMB) "
        "| %d skipped (budget) | %d unreadable",
        len(selected),
        total_pages,
        total_bytes_estimate / 1_000_000,
        max_rows,
        max_bytes / 1_000_000,
        len(skipped),
        len(unreadable),
    )

    return BatchSessionSelection(
        selected_pdf_names=selected,
        total_pages=total_pages,
        total_bytes_estimate=total_bytes_estimate,
        skipped_pdf_names=skipped,
        unreadable_pdf_names=unreadable,
    )
