"""Page-count-based session sizing for Bifrost Batch API jobs.

Bifrost's own docs (https://docs.getbifrost.ai) don't state a row-count or
file-size limit for a batch job — unlike the old direct-Vertex
implementation, which at least had Vertex's documented (if not
BigQuery-path-confirmed) 200,000-request cap to size against (see this
module's git history). ``MAX_CLASSIFICATION_ROWS_DEFAULT`` below is
therefore a much more conservative starting guess, not a value derived from
a documented limit — it needs empirical validation against a real Bifrost
batch submission (the first staging run) before being trusted at any larger
size. It's also a *file-size* concern now, not just a row-count one: each
JSONL row inlines a full base64-encoded single PDF page (see
``classification_submit.py``), so this pipeline's rows are far heavier than
a typical short-prompt batch row.

The synchronous pipeline sizes a batch by PDF count (``batch_size``/
``max_pdfs``), which doesn't translate directly here: the classification job
submits **one row per page**, and a PDF's page count is unknown until the
file is actually opened. Selecting PDFs "count of PDFs" therefore risks
building an input file with far more (or fewer) rows than intended.
Instead, this module selects PDFs by *accumulating actual page counts*,
stopping as soon as the running total would exceed the configured budget.
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


@dataclass(frozen=True)
class BatchSessionSelection:
    """Result of selecting a page-count-bounded subset of pending PDFs."""

    selected_pdf_names: list[str]  # PDFs included in this session, in input order
    total_pages: int  # sum of page counts across selected_pdf_names
    skipped_pdf_names: list[str]  # PDFs that didn't fit this session's budget (stay pending)
    unreadable_pdf_names: list[str]  # PDFs whose page count couldn't be determined (also stay pending)


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


def select_pdfs_within_row_budget(
    pdf_paths: dict[str, Path],
    max_rows: int = MAX_CLASSIFICATION_ROWS_DEFAULT,
) -> BatchSessionSelection:
    """Select a prefix of ``pdf_paths`` whose accumulated page count fits ``max_rows``.

    Iterates ``pdf_paths`` in the order given (callers should pass an
    already-sorted/deterministic mapping — e.g. ``dict(sorted(...))`` — so
    repeated runs behave predictably), opening each PDF just far enough to
    read its page count, and adds it to the selection only if doing so
    would not exceed ``max_rows``. Stops accumulating as soon as the next
    candidate would overflow the budget — later candidates are also left
    out (not skipped-and-continued), since PDFs are typically similar in
    size and skipping one only to add a smaller one later would make
    session composition less predictable across runs.

    :param pdf_paths: Mapping of pending PDF name -> local downloaded path
        (already downloaded, e.g. via ``GCSDownloader.download_pdfs_batch``).
    :param max_rows: Maximum total page count (= classification request
        rows) allowed in this session.
    :returns: A :class:`BatchSessionSelection` with the chosen subset and
        page-count total.
    """
    selected: list[str] = []
    skipped: list[str] = []
    unreadable: list[str] = []
    total_pages = 0
    budget_exhausted = False

    for pdf_name, path in pdf_paths.items():
        if budget_exhausted:
            skipped.append(pdf_name)
            continue

        page_count = _count_pages(path)
        if page_count is None:
            unreadable.append(pdf_name)
            continue

        if total_pages + page_count > max_rows:
            # Stop accumulating entirely (don't skip-and-continue looking for
            # a smaller later candidate that might still fit) — see
            # docstring for why: keeps session composition predictable
            # across runs instead of depending on input ordering quirks.
            skipped.append(pdf_name)
            budget_exhausted = True
            continue

        selected.append(pdf_name)
        total_pages += page_count

    logger.warning(
        "Session sizing: %d PDFs selected (%d pages, budget=%d) | %d skipped (budget) | %d unreadable",
        len(selected),
        total_pages,
        max_rows,
        len(skipped),
        len(unreadable),
    )

    return BatchSessionSelection(
        selected_pdf_names=selected,
        total_pages=total_pages,
        skipped_pdf_names=skipped,
        unreadable_pdf_names=unreadable,
    )
