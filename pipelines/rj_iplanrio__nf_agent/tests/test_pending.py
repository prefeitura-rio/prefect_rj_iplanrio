"""Tests for the already-processed lookup."""

from unittest.mock import patch

from pipelines.rj_iplanrio__nf_agent.utils import pending


def test_find_done_pdfs_chunks_and_unions(monkeypatch):
    monkeypatch.setattr(pending, "CHUNK_SIZE", 2)
    calls = []

    # Mirrors run_query's signature exactly since it's used as its side_effect.
    def fake_query(caller_file, name, table, params):  # noqa: ARG001
        names = next(p for p in params if p.name == "nomes").values
        calls.append(names)
        return [{"nome_arquivo": n} for n in names if n != "b"]

    with patch.object(pending, "run_query", side_effect=fake_query):
        done = pending.find_done_pdfs("p.d.t", ["a", "b", "c"], "auto-abc")
    assert calls == [["a", "b"], ["c"]]
    assert done == {"a", "c"}


def test_find_done_pdfs_with_no_names_skips_query():
    with patch.object(pending, "run_query") as query:
        assert pending.find_done_pdfs("p.d.t", [], "v") == set()
    query.assert_not_called()
