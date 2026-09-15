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
    # Bifrost publishes no documented row/file-size limit for a batch job
    # (see row_counting.py's module docstring) — the default is deliberately
    # set close to the synchronous pipeline's own default batch_size (1000
    # PDFs, see flow.py) as a conservative, unvalidated starting guess.
    assert row_counting.MAX_CLASSIFICATION_ROWS_DEFAULT == 1_000
