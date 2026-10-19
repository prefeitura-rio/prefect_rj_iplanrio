"""Tests for the already-processed lookup."""

from pathlib import Path
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


def test_list_downloaded_pdfs_queries_the_view_by_month_and_builds_refs():
    rows = [{"name": "a", "uri": "gs://b/raiz/a"}, {"name": "b", "uri": "gs://b/mes_envio=2022-04-01/b"}]
    with patch.object(pending, "run_query", return_value=rows) as query:
        refs = pending.list_downloaded_pdfs("p.d.vw", "2022-04-01")
    assert [(r.name, r.uri) for r in refs] == [("a", "gs://b/raiz/a"), ("b", "gs://b/mes_envio=2022-04-01/b")]
    assert query.call_args.args[1:3] == ("downloaded_pdfs", "p.d.vw")
    param = query.call_args.args[3][0]
    assert (param.name, param.type_, param.value) == ("mes_envio", "DATE", "2022-04-01")


def test_downloaded_pdfs_query_only_takes_downloaded_files_of_the_month():
    sql = (Path(pending.__file__).parent / "queries" / "downloaded_pdfs.sql").read_text()
    assert "n_arquivos_gcs > 0" in sql
    assert "pdf.uri IS NOT NULL" in sql
    assert "DATE(mes_envio) = @mes_envio" in sql
