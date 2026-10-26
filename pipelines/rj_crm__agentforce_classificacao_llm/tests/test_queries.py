from pathlib import Path

from utils.utils import render_query

ANCHOR = str(Path(__file__).parent.parent / "tasks.py")


def test_the_extraction_query_takes_the_window_and_the_tables() -> None:
    """Put the days, the tables and the limit of the HSM in the query, with nothing left to fill."""
    sql = render_query(
        ANCHOR,
        "extract_sessions",
        lookback_days=7,
        hsm_max_days_before=3,
        source_table="p.d.src",
        destination_table="p.d.dst",
    )

    assert "INTERVAL 7 DAY" in sql
    assert "DEFAULT 3;" in sql
    assert "`p.d.src`" in sql
    assert "`p.d.dst`" in sql
    assert "{{" not in sql


def test_the_buttons_query_lists_the_journeys() -> None:
    """Quote each journey in the `IN` list."""
    sql = render_query(ANCHOR, "hsm_buttons", catalog_table="p.d.cat", journeys=["a", "b"])

    assert "jornada_nome IN ('a', 'b')" in sql
    assert "`p.d.cat`" in sql


def test_the_merge_queries_name_their_tables_and_columns() -> None:
    """Fill the set clause, the insert and the tables."""
    merge = render_query(
        ANCHOR,
        "merge_classifications",
        destination="p.d.t",
        tmp="p.d.tmp",
        primary_key="id_sessao",
        set_clause="t.a = s.a",
        insert_cols="id_sessao, a",
        insert_vals="s.id_sessao, s.a",
    )
    themes = render_query(ANCHOR, "merge_themes", destination="p.d.t", tmp="p.d.tmp")

    assert "ON t.id_sessao = s.id_sessao" in merge
    assert "UPDATE SET t.a = s.a" in merge
    assert "UPDATE SET t.tema_nome = s.tema_nome, t.causa_nome = s.causa_nome" in themes
    assert render_query(ANCHOR, "truncate_table", table="p.d.tmp").strip() == "TRUNCATE TABLE `p.d.tmp`"
