from datetime import date

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import TYPE_REPORT, TYPE_VALIDATION
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.drive import (
    escape_query,
    find_file,
    find_reviewed_sheets,
    list_all,
    prepare,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tests.conftest import RecorderFactory
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Job, Window


def sheet(file_id: str) -> dict[str, str]:
    """Return a Drive sheet entry."""
    return {"id": file_id, "name": f"conversas_classificadas_revisada_{file_id}"}


def test_a_query_value_escapes_quotes_and_backslashes() -> None:
    """Never let an apostrophe in a name change the search."""
    assert escape_query("a'b\\c") == "a\\'b\\\\c"


def test_every_page_of_a_listing_is_read(make_recorder: RecorderFactory) -> None:
    """Follow `nextPageToken`, or a large folder would give only the first page."""
    drive = make_recorder(
        files__list=[
            {"files": [{"id": "a"}], "nextPageToken": "p2"},
            {"files": [{"id": "b"}]},
        ]
    )

    assert [item["id"] for item in list_all(drive, "q")] == ["a", "b"]
    assert [kwargs.get("pageToken") for _, kwargs in drive.calls] == [None, "p2"]


def test_an_exact_name_finds_one_file_and_not_a_similar_name(make_recorder: RecorderFactory) -> None:
    """Search by the exact name, so `Catálogo_old` is not `Catálogo`."""
    drive = make_recorder(files__list={"files": []})

    assert find_file(drive, "Catálogo", "folder", "mime") is None
    query = str(drive.calls[0][1]["q"])
    assert "name = 'Catálogo'" in query
    assert "'folder' in parents" in query
    assert "mimeType = 'mime'" in query


def test_the_reviewed_sheets_come_from_every_folder_but_the_quick_ones(make_recorder: RecorderFactory) -> None:
    """Never go into a quick report folder, whose files must not feed the examples."""
    drive = make_recorder(
        files__list=[
            {"files": [sheet("r1")]},
            {
                "files": [
                    {"id": "round", "name": "2026-11-05_08h__x"},
                    {"id": "quick", "name": "2026-11-05_08h__x__catalogo_automatico"},
                ]
            },
            {"files": [sheet("r2")]},
            {"files": []},
        ]
    )

    found = find_reviewed_sheets(drive, "root")

    assert [item["id"] for item in found] == ["r1", "r2"]
    assert not any("'quick' in parents" in str(kwargs["q"]) for _, kwargs in drive.calls)


def test_the_campaign_folder_is_created_and_a_round_that_exists_is_skipped(make_recorder: RecorderFactory) -> None:
    """Skip a report whose round folder exists, with the link of that folder."""
    drive = make_recorder(
        files__get={"id": "root"},
        files__list=[
            {"files": [{"id": "campaign", "name": "camp"}]},
            {"files": []},
            {"files": [{"id": "round", "name": "2026-11-05_08h__x"}]},
        ],
    )
    job = Job("camp", TYPE_REPORT, Window("camp", "nome_campanha", date(2026, 11, 1), date(2026, 11, 4)))

    prepared = prepare(drive, job, "root", "2026-11-05_08h__x")

    assert prepared.folder_id == "campaign"
    assert prepared.outcome is not None
    assert prepared.outcome.status == "pulado: relatório '2026-11-05_08h__x' já existe"
    assert prepared.outcome.link == "https://drive.google.com/drive/folders/round"


def test_a_validation_looks_for_the_catalog_and_never_for_a_round(make_recorder: RecorderFactory) -> None:
    """Report the existing `Catalog` so the request skips and points to it."""
    drive = make_recorder(
        files__get={"id": "root"},
        files__list=[
            {"files": [{"id": "campaign", "name": "camp"}]},
            {"files": [{"id": "cat", "name": "Catálogo", "webViewLink": "https://cat"}]},
        ],
    )
    job = Job("camp", TYPE_VALIDATION, Window("camp", "nome_campanha", date(2026, 11, 1), date(2026, 11, 4)))

    prepared = prepare(drive, job, "root", "ignored")

    assert (prepared.catalog_id, prepared.catalog_link, prepared.outcome) == ("cat", "https://cat", None)
