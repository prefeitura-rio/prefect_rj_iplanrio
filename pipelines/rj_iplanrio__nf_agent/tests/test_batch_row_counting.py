"""Tests for ``utils/batch/row_counting.py``'s page-count-bounded PDF selection."""

from __future__ import annotations

from pathlib import Path

from pipelines.rj_iplanrio__nf_agent.utils.batch import row_counting


def test_all_pdfs_fit_within_budget(make_pdf):
    pdf_paths = {
        "a": make_pdf(n_pages=3, name="a.pdf"),
        "b": make_pdf(n_pages=2, name="b.pdf"),
    }

    selection = row_counting.select_pdfs_within_row_budget(pdf_paths, max_rows=100)

    assert selection.selected_pdf_names == ["a", "b"]
    assert selection.total_pages == 5
    assert selection.skipped_pdf_names == []
    assert selection.unreadable_pdf_names == []


def test_stops_accumulating_at_first_overflow_and_skips_the_rest(make_pdf):
    # Budget of 4: "a" (3 pages) fits, "b" (3 pages) would overflow (3+3=6>4)
    # and is skipped; "c" (1 page) would fit on its own but must NOT be
    # included after the budget is exhausted (see row_counting.py's
    # docstring on why later-smaller-candidates aren't opportunistically
    # squeezed in).
    pdf_paths = {
        "a": make_pdf(n_pages=3, name="a.pdf"),
        "b": make_pdf(n_pages=3, name="b.pdf"),
        "c": make_pdf(n_pages=1, name="c.pdf"),
    }

    selection = row_counting.select_pdfs_within_row_budget(pdf_paths, max_rows=4)

    assert selection.selected_pdf_names == ["a"]
    assert selection.total_pages == 3
    assert selection.skipped_pdf_names == ["b", "c"]


def test_unreadable_pdf_is_excluded_but_does_not_stop_selection(make_pdf, tmp_path: Path):
    corrupted = tmp_path / "corrupted.pdf"
    corrupted.write_bytes(b"not a real pdf")

    pdf_paths = {
        "good": make_pdf(n_pages=2, name="good.pdf"),
        "bad": corrupted,
    }

    selection = row_counting.select_pdfs_within_row_budget(pdf_paths, max_rows=100)

    assert selection.selected_pdf_names == ["good"]
    assert selection.unreadable_pdf_names == ["bad"]
    assert selection.total_pages == 2


def test_empty_input_returns_empty_selection():
    selection = row_counting.select_pdfs_within_row_budget({}, max_rows=100)

    assert selection.selected_pdf_names == []
    assert selection.total_pages == 0
    assert selection.skipped_pdf_names == []
    assert selection.unreadable_pdf_names == []


def test_default_budget_constant_matches_synchronous_pipeline_batch_size():
    # Bifrost publishes no documented row-count limit for a batch job
    # (see row_counting.py's module docstring) — the default is deliberately
    # set close to the synchronous pipeline's own default batch_size (1000
    # PDFs, see flow.py) as a conservative, unvalidated starting guess.
    assert row_counting.MAX_CLASSIFICATION_ROWS_DEFAULT == 1_000


def _inflate_pdf(path: Path, extra_bytes: int) -> None:
    """Append junk bytes to a PDF file to simulate a larger scanned page.

    PDF readers tolerate trailing garbage after the ``%%EOF`` marker, so
    this inflates on-disk file size (what byte-budget estimation reads via
    ``Path.stat().st_size``) without touching page count/content.
    """
    with path.open("ab") as f:
        f.write(b"\n% " + b"0" * extra_bytes)


class TestByteBudget:
    """Bifrost rejects the whole upload above ~100MB (see row_counting.py's
    module docstring — bisected directly against staging on 2026-09-19).
    Row count alone doesn't protect against this since page byte size
    varies a lot; these tests cover the byte-budget stopping behavior
    added alongside the row budget.
    """

    def test_stops_at_byte_budget_even_when_row_budget_has_room(self, make_pdf):
        # Row budget (100) has plenty of room for both PDFs, but the byte
        # budget (with its ~1.4x base64 estimate factor) only fits the
        # first one: ~5MB on-disk -> ~7MB estimated each, so two together
        # (~14MB) overflow a 10MB budget but one alone (~7MB) fits.
        a = make_pdf(n_pages=1, name="a.pdf")
        _inflate_pdf(a, 5_000_000)
        b = make_pdf(n_pages=1, name="b.pdf")
        _inflate_pdf(b, 5_000_000)

        selection = row_counting.select_pdfs_within_row_budget(
            {"a": a, "b": b}, max_rows=100, max_bytes=10_000_000
        )

        assert selection.selected_pdf_names == ["a"]
        assert selection.skipped_pdf_names == ["b"]
        assert selection.total_bytes_estimate > 0

    def test_row_budget_still_applies_when_byte_budget_has_room(self, make_pdf):
        # Byte budget is generous; row budget is what stops selection —
        # confirms the two budgets are independent, either can bind first.
        pdf_paths = {
            "a": make_pdf(n_pages=3, name="a.pdf"),
            "b": make_pdf(n_pages=3, name="b.pdf"),
        }

        selection = row_counting.select_pdfs_within_row_budget(
            pdf_paths, max_rows=4, max_bytes=1_000_000_000
        )

        assert selection.selected_pdf_names == ["a"]
        assert selection.skipped_pdf_names == ["b"]

    def test_default_byte_budget_is_below_the_confirmed_bifrost_ceiling(self):
        # Confirmed by bisection against the real staging Bifrost instance:
        # ~99.7MB succeeds (or gets a further/different error past upload),
        # ~101.2MB fails with a generic "Error when parsing request" — see
        # module docstring. The default must stay comfortably under that.
        assert row_counting.MAX_CLASSIFICATION_BYTES_DEFAULT < 99_700_000
