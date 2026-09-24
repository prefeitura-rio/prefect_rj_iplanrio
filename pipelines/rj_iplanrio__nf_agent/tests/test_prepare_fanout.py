"""Tests for multi-session fan-out in ``utils.pipeline.prepare_session_pdfs``.

Uses a fake GCS downloader serving real (tiny) PDFs from a temp dir, with
``PageStatusReader.find_pending_files`` mocked to all-pending — so these
exercise the grouping/download loop without any GCS/BigQuery calls.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import fitz

from pipelines.rj_iplanrio__nf_agent.utils import pipeline as pipeline_mod
from pipelines.rj_iplanrio__nf_agent.utils.batch.row_counting import SessionBudget


def _write_pdf(path: Path, n_pages: int, extra_bytes: int = 0) -> None:
    doc = fitz.open()
    for i in range(n_pages):
        page = doc.new_page()
        page.insert_text((72, 72), f"test page {i + 1}")
    doc.save(str(path))
    doc.close()
    if extra_bytes:
        with path.open("ab") as f:
            f.write(b"\n% " + b"0" * extra_bytes)


class _FakeDownloader:
    """Serves pre-generated PDFs; records download/BQ-check calls."""

    def __init__(self, src_dir: Path, names: list[str]):
        self.src_dir = src_dir
        self.names = names
        self.download_calls: list[list[str]] = []

    def get_available_pdf_filenames(self) -> set[str]:
        return set(self.names)

    def download_pdfs_batch(self, pdf_names: list[str], local_dir: Path, batch_size: int = 20) -> dict[str, Path]:  # noqa: ARG002 - mirrors the real signature
        self.download_calls.append(list(pdf_names))
        out: dict[str, Path] = {}
        for name in pdf_names:
            dest = Path(local_dir) / f"{name}.pdf"
            dest.write_bytes((self.src_dir / f"{name}.pdf").read_bytes())
            out[name] = dest
        return out


def _run_prepare(names_spec: dict[str, tuple[int, int] | None], tmp_path: Path, **kwargs) -> tuple[dict, list]:
    """Generate PDFs per spec {name: (pages, extra_bytes)} and run prepare_session_pdfs.

    A spec value of None writes garbage bytes instead (unreadable PDF).
    """
    src = tmp_path / "src"
    src.mkdir()
    for name, details in names_spec.items():
        if details is None:
            (src / f"{name}.pdf").write_bytes(b"not a real pdf")
        else:
            pages, extra = details
            _write_pdf(src / f"{name}.pdf", pages, extra)
    fake_dl = MagicMock(wraps=_FakeDownloader(src, list(names_spec)))
    # wraps + attribute access: reach the underlying fake for call assertions
    fake = fake_dl._mock_wraps

    def _all_pending(_self, candidate_filenames, **_kwargs):
        return set(candidate_filenames)

    with (
        patch.object(pipeline_mod.PageStatusReader, "find_pending_files", _all_pending),
        patch.object(pipeline_mod, "get_git_info", return_value={"commit": "abc123"}),
        patch.object(pipeline_mod, "_SESSION_PREP_SLICE_SIZE", 3),
    ):
        params = {
            "gcs_downloader": fake_dl,
            "bq_extracao_pagina_table": "proj.ds.extracao_pagina",
            "budget": SessionBudget(max_rows=4, max_bytes=1_000_000_000),
            "local_dir": tmp_path / "work",
            "workers": 10,
            "max_sessions": 1,
            "total_max_rows": None,
        }
        params.update(kwargs)
        (tmp_path / "work").mkdir(exist_ok=True)
        paths, groups = pipeline_mod.prepare_session_pdfs(**params)
    return paths, groups, fake


def _pages(names: list[str], spec: dict) -> int:
    return sum(spec[n][0] for n in names)


class TestFanOut:
    def test_single_session_matches_legacy_prefix_behavior(self, tmp_path: Path):
        # 5 PDFs x 2 pages, budget 4 rows -> first 2 PDFs, then overflow stops.
        spec = {f"f{i}": (2, 0) for i in range(5)}
        paths, groups, fake = _run_prepare(spec, tmp_path)

        assert len(groups) == 1
        assert groups[0].selected_pdf_names == ["f0", "f1"]
        assert groups[0].total_pages == 4
        assert set(paths) == {"f0", "f1"}
        # Early stop: only the first slice (3 files) was BQ-checked/downloaded.
        assert len(fake.download_calls) == 1

    def test_fan_out_forms_disjoint_consecutive_groups(self, tmp_path: Path):
        # 11 PDFs x 2 pages, budget 4 rows/session, up to 5 sessions:
        # groups [f00,f01] ... [f08,f09], leftover [f10].
        # (Zero-padded names: plain f0..f10 would sort lexicographically
        # with f10 between f1 and f2, which is correct behavior, just
        # confusing to assert on.)
        spec = {f"f{i:02d}": (2, 0) for i in range(11)}
        paths, groups, _ = _run_prepare(spec, tmp_path, max_sessions=5)

        assert [g.selected_pdf_names for g in groups] == [
            ["f00", "f01"],
            ["f02", "f03"],
            ["f04", "f05"],
            ["f06", "f07"],
            ["f08", "f09"],
        ]
        assert all(g.total_pages == 4 for g in groups)
        # Disjoint and covering everything except the leftover tail.
        flat = [n for g in groups for n in g.selected_pdf_names]
        assert len(set(flat)) == len(flat) == 10
        assert set(paths) == set(flat)

    def test_max_sessions_caps_groups_and_stops_downloading(self, tmp_path: Path):
        spec = {f"f{i}": (2, 0) for i in range(9)}
        paths, groups, fake = _run_prepare(spec, tmp_path, max_sessions=2)

        assert [g.selected_pdf_names for g in groups] == [["f0", "f1"], ["f2", "f3"]]
        # Stopped right after the 2nd group closed (2 slices), never scanned slice 3.
        assert len(fake.download_calls) == 2
        assert set(paths) == {"f0", "f1", "f2", "f3"}

    def test_total_cap_stops_across_groups(self, tmp_path: Path):
        spec = {f"f{i}": (2, 0) for i in range(9)}
        _, groups, _ = _run_prepare(spec, tmp_path, max_sessions=5, total_max_rows=5)

        total = sum(g.total_pages for g in groups)
        assert total == 4  # f0+f1; f2 would push to 6 > 5
        assert [g.selected_pdf_names for g in groups] == [["f0", "f1"]]

    def test_oversized_single_file_is_skipped_not_stalling(self, tmp_path: Path):
        # f0 inflated past the byte budget; f1/f2 are small enough to fit.
        # (A real minimal 1-page PDF is ~1KB on disk, ~1.4KB estimated, so
        # the budget must sit between that and f0's inflated estimate.)
        spec = {"f0": (1, 5000), "f1": (1, 0), "f2": (1, 0)}
        _, groups, _ = _run_prepare(
            spec, tmp_path, max_sessions=2, budget=SessionBudget(max_rows=100, max_bytes=3000)
        )

        flat = [n for g in groups for n in g.selected_pdf_names]
        assert "f0" not in flat
        assert flat == ["f1", "f2"]

    def test_empty_listing_returns_no_groups(self, tmp_path: Path):
        paths, groups, fake = _run_prepare({}, tmp_path, max_sessions=3)
        assert groups == []
        assert paths == {}
        assert fake.download_calls == []

    def test_unreadable_files_are_excluded(self, tmp_path: Path):
        spec = {"f0": (2, 0), "bad": None, "f1": (2, 0)}
        _, groups, _ = _run_prepare(spec, tmp_path, max_sessions=2)

        flat = [n for g in groups for n in g.selected_pdf_names]
        assert "bad" not in flat
        assert flat == ["f0", "f1"]
