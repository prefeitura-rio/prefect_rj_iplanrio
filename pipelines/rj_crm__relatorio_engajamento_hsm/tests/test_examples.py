from datetime import UTC, date, datetime

import numpy as np
import polars as pl
import pytest

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import EXAMPLES_SCHEMA
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog, ValidCatalog
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.examples import (
    SyncPlan,
    campaign_state,
    drive_state,
    fresh_rows,
    load_store,
    merge_store,
    parse_reviews,
    plan_sync,
    retrieve,
    review_date,
    select_examples,
    top_k,
)


def valid_catalog(*names: str) -> ValidCatalog:
    """Return a validated catalog with these active categories."""
    active = Catalog()
    for name in names:
        active.register(name, "")

    return ValidCatalog(active=active, every=active)


def review_sheet(**columns: list[str]) -> pl.DataFrame:
    """Return a reviewed conversations sheet with the header the report writes."""
    size = len(next(iter(columns.values())))
    base = {
        "sheet_row": [str(number) for number in range(2, 2 + size)],
        "id_sessao": [f"s{number}" for number in range(size)],
        "categoria": ["Health"] * size,
        "conversa": ["CIDADÃO: oi"] * size,
        "revisao": [""] * size,
        "categorias_corretas": [""] * size,
        "justificativa_revisao": [""] * size,
    }

    return pl.DataFrame({**base, **columns})


def test_a_row_marked_wrong_becomes_one_example_with_the_correct_category() -> None:
    """Use `categorias_corretas` for a wrong row."""
    sheet = review_sheet(revisao=["Wrong"], categorias_corretas=["Saúde"])

    found, discarded = parse_reviews(sheet, valid_catalog("Saúde", "Health"))

    assert found.row(0, named=True) == {
        "session_id": "s0",
        "conversation": "CIDADÃO: oi",
        "categories": ["Saúde"],
        "review": "Wrong",
        "review_note": "",
    }
    assert discarded == []


def test_a_row_marked_right_keeps_the_category_of_the_model_and_accepts_portuguese() -> None:
    """Read `Certo` as Right and keep the classified category."""
    found, _ = parse_reviews(review_sheet(revisao=["certo"]), valid_catalog("Health"))

    assert found["categories"].to_list() == [["Health"]]
    assert found["review"].to_list() == ["Right"]


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        pytest.param({"revisao": ["Wrong"], "categorias_corretas": ["Old"]}, "not active", id="an_inactive_category"),
        pytest.param(
            {"revisao": ["Wrong"], "categorias_corretas": [""]}, "no category", id="a_wrong_row_without_category"
        ),
        pytest.param({"revisao": ["maybe"]}, "maybe", id="an_unknown_review"),
        pytest.param({"revisao": ["Right"], "conversa": [""]}, "no conversation", id="no_conversation"),
        pytest.param(
            {"revisao": ["Wrong"], "categorias_corretas": ["Health;Old"]}, "not active", id="one_inactive_of_two"
        ),
    ],
)
def test_a_row_with_a_bad_review_is_discarded_with_its_reason(changes: dict[str, list[str]], reason: str) -> None:
    """Drop the whole row and say why."""
    found, discarded = parse_reviews(review_sheet(**changes), valid_catalog("Health"))

    assert found.is_empty()
    assert len(discarded) == 1
    assert reason in discarded[0]
    assert discarded[0].startswith("row 2")


def test_a_row_not_reviewed_yet_is_not_a_discard() -> None:
    """Ignore a row with no review without a warning."""
    found, discarded = parse_reviews(review_sheet(revisao=[""]), valid_catalog("Health"))

    assert found.is_empty()
    assert discarded == []


def test_two_categories_keep_their_order_without_repeats() -> None:
    """Split on the separator and drop the repeats."""
    sheet = review_sheet(revisao=["Wrong"], categorias_corretas=["Saúde; Health ;saúde"])

    found, _ = parse_reviews(sheet, valid_catalog("Saúde", "Health"))

    assert found["categories"].to_list() == [["Saúde", "Health"]]


def test_a_sheet_without_a_review_column_is_ignored_with_a_reason() -> None:
    """Never read a sheet that lost a column."""
    found, discarded = parse_reviews(review_sheet(revisao=["Right"]).drop("revisao"), valid_catalog("Health"))

    assert found.is_empty()
    assert "revisao" in discarded[0]


@pytest.mark.parametrize(
    ("name", "modified", "expected"),
    [
        pytest.param("x_revisada_2026-03-02", "2026-04-01T10:00:00.000Z", date(2026, 3, 2), id="the_date_of_the_name"),
        pytest.param("x_revisada_2026-13-45", "2026-04-01T10:00:00.000Z", date(2026, 4, 1), id="a_bad_name_date"),
        pytest.param("x_revisada", "2026-04-01T10:00:00.000Z", date(2026, 4, 1), id="no_name_date"),
        pytest.param("x_revisada", "not a time", None, id="no_date_at_all"),
    ],
)
def test_the_review_date(name: str, modified: str, expected: date | None) -> None:
    """Prefer the date in the file name over the Drive time."""
    assert review_date(name, modified) == expected


def test_sync_reads_new_changed_and_other_model_files() -> None:
    """Plan new, changed, removed, and model-mismatched files."""
    drive = pl.DataFrame({"file_id": ["same", "changed", "new", "model"], "modified_at": ["1", "2", "1", "1"]})
    stored = pl.DataFrame(
        {
            "file_id": ["same", "changed", "removed", "model"],
            "modified_at": ["1", "1", "1", "1"],
            "model": ["m", "m", "m", "old"],
        }
    )
    plan = plan_sync(drive, stored, "m")
    assert (plan.read, plan.remove, plan.unchanged) == (["changed", "new", "model"], ["removed"], 1)


def test_examples_keep_active_categories_and_latest_revision() -> None:
    """Discard inactive categories and keep the latest session revision."""
    frame = pl.DataFrame(
        {
            "session_id": ["a", "a", "b"],
            "categories": [["Health"], ["Health"], ["Old"]],
            "revision_date": [date(2026, 1, 1), date(2026, 2, 1), date(2026, 2, 1)],
            "ingested_at": [datetime(2026, 1, 1), datetime(2026, 2, 1), datetime(2026, 2, 1)],
        }
    )
    found = select_examples(frame, valid_catalog("Health"))
    assert found["session_id"].to_list() == ["a"]
    assert found["revision_date"].to_list() == [date(2026, 2, 1)]


def test_examples_come_back_with_the_canonical_name_of_the_current_catalog() -> None:
    """Rename a category to the spelling of the catalog today."""
    frame = pl.DataFrame(
        {
            "session_id": ["a"],
            "categories": [["saude"]],
            "revision_date": [date(2026, 1, 1)],
            "ingested_at": [datetime(2026, 1, 1)],
        }
    )

    assert select_examples(frame, valid_catalog("Saúde"))["categories"].to_list() == [["Saúde"]]


def test_top_k_orders_the_nearest_vectors() -> None:
    """Return the nearest vectors in descending similarity order."""
    indices, scores = top_k(np.array([[1.0, 0.0]]), np.array([[0.0, 1.0], [1.0, 0.0], [0.8, 0.6]]), 2)
    assert indices.tolist() == [[1, 2]]
    assert scores.tolist() == [[1.0, 0.8]]


def stored(**columns: list[object]) -> pl.DataFrame:
    """Return the example table with the columns that a test sets, and an empty rest."""
    size = len(next(iter(columns.values())))
    base: dict[str, list[object]] = {
        "campaign": ["c"] * size,
        "session_id": [f"s{number}" for number in range(size)],
        "conversation": ["CIDADÃO: oi"] * size,
        "categories": [["Health"]] * size,
        "review": ["Right"] * size,
        "review_note": [""] * size,
        "file_id": ["f1"] * size,
        "file_name": ["n"] * size,
        "modified_at": ["1"] * size,
        "revision_date": [date(2026, 1, 1)] * size,
        "embedding": [[1.0, 0.0]] * size,
        "model": ["m"] * size,
        "ingested_at": [datetime(2026, 1, 1, tzinfo=UTC)] * size,
    }

    return pl.DataFrame({**base, **columns}, schema=EXAMPLES_SCHEMA)


def test_the_state_of_a_campaign_lists_its_files_once() -> None:
    """Give the plan the file, the Drive time and the model of the campaign only."""
    table = stored(campaign=["c", "c", "other"], file_id=["f1", "f1", "f9"])

    state = campaign_state(table, "c")

    assert state.to_dicts() == [{"file_id": "f1", "modified_at": "1", "model": "m"}]


def test_the_drive_state_has_the_id_and_the_time_of_each_file() -> None:
    """Compare Drive with the table by id and modified time."""
    files = [{"id": "f1", "name": "x", "modifiedTime": "2026-01-01T00:00:00Z"}]

    assert drive_state(files).to_dicts() == [{"file_id": "f1", "modified_at": "2026-01-01T00:00:00Z"}]


def test_a_fresh_file_gives_rows_with_the_origin_and_the_review_date() -> None:
    """Keep the Drive file, its time and the date of the review in each row."""
    examples = pl.DataFrame(
        {
            "session_id": ["s1"],
            "conversation": ["CIDADÃO: oi"],
            "categories": [["Health"]],
            "review": ["Wrong"],
            "review_note": ["n"],
        }
    )
    file = {"id": "f1", "name": "x_revisada_2026-03-02", "modifiedTime": "2026-04-01T10:00:00Z"}

    rows = fresh_rows(examples, "c", file, [[0.0, 1.0]], ("m", datetime(2026, 5, 1, tzinfo=UTC)))

    assert rows.schema == EXAMPLES_SCHEMA
    assert rows.row(0, named=True) | {"ingested_at": None} == {
        "campaign": "c",
        "session_id": "s1",
        "conversation": "CIDADÃO: oi",
        "categories": ["Health"],
        "review": "Wrong",
        "review_note": "n",
        "file_id": "f1",
        "file_name": "x_revisada_2026-03-02",
        "modified_at": "2026-04-01T10:00:00Z",
        "revision_date": date(2026, 3, 2),
        "embedding": [0.0, 1.0],
        "model": "m",
        "ingested_at": None,
    }


def test_the_merge_replaces_changed_and_removed_files_and_keeps_other_campaigns() -> None:
    """Rebuild the whole table: other campaigns and unchanged files stay as they are."""
    table = stored(
        campaign=["c", "c", "c", "other"],
        file_id=["same", "changed", "removed", "changed"],
        session_id=["a", "b", "c", "d"],
    )
    fresh = stored(campaign=["c"], file_id=["changed"], session_id=["b2"])
    plan = SyncPlan(read=["changed"], remove=["removed"], unchanged=1)

    merged = merge_store(table, plan, "c", fresh)

    assert sorted(merged["session_id"].to_list()) == ["a", "b2", "d"]


def test_the_store_has_only_valid_examples_of_the_campaign_and_the_model() -> None:
    """Drop other campaigns, other models, and categories that are no longer active."""
    table = stored(
        campaign=["c", "c", "c", "other"],
        model=["m", "old", "m", "m"],
        categories=[["Health"], ["Health"], ["Gone"], ["Health"]],
        session_id=["keep", "old_model", "inactive", "other_campaign"],
        embedding=[[1.0, 0.0], [0.0, 1.0], [1.0, 1.0], [0.5, 0.5]],
    )

    store = load_store(table, "c", "m", valid_catalog("Health"))

    assert store.examples["session_id"].to_list() == ["keep"]
    assert store.vectors.shape == (1, 2)
    assert "embedding" not in store.examples.columns


def test_an_empty_store_has_no_vectors() -> None:
    """Classify with no example when the campaign has none."""
    store = load_store(stored(campaign=["other"]), "c", "m", valid_catalog("Health"))

    assert store.examples.is_empty()
    assert store.vectors.shape[0] == 0


def test_the_nearest_examples_come_back_for_each_query() -> None:
    """Give each conversation its own examples, nearest first."""
    table = stored(
        session_id=["north", "east", "northeast"],
        embedding=[[0.0, 1.0], [1.0, 0.0], [0.6, 0.8]],
        categories=[["Health"]] * 3,
    )
    store = load_store(table, "c", "m", valid_catalog("Health"))

    found = retrieve(store, np.array([[0.0, 1.0], [1.0, 0.0]]), 2)

    assert [frame["session_id"].to_list() for frame in found] == [["north", "northeast"], ["east", "northeast"]]


def test_no_store_gives_each_query_an_empty_frame() -> None:
    """Zero-shot: nothing to retrieve."""
    store = load_store(stored(campaign=["other"]), "c", "m", valid_catalog("Health"))

    found = retrieve(store, np.array([[1.0, 0.0]]), 3)

    assert len(found) == 1
    assert found[0].is_empty()
