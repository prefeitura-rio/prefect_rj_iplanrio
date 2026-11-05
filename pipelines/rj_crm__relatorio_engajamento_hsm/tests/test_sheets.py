import polars as pl
import pytest

from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.sheets import (
    column_letter,
    create_sheet,
    format_sheet,
    quote_tab,
    read_sheet,
    update_request_row,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tests.conftest import RecorderFactory

HEADER = ["Carimbo de data/hora", "Endereço de e-mail", "status", "link_resultado", "processado_em"]
TAB = {"sheets": [{"properties": {"sheetId": 7, "title": "catalogo"}}]}


def test_a1_names_quote_tabs_and_columns() -> None:
    """Build safe A1 tab and column names."""
    assert quote_tab("A'b") == "'A''b'"
    assert [column_letter(value) for value in (1, 26, 27, 52, 53)] == ["A", "Z", "AA", "AZ", "BA"]


def test_a_sheet_is_read_as_text_without_blank_rows_and_keeps_row_numbers(make_recorder: RecorderFactory) -> None:
    """Fill short rows, drop empty rows, and number the rows of the sheet."""
    service = make_recorder(
        spreadsheets__get={"sheets": [{"properties": {"title": "first"}}]},
        spreadsheets__values__get={"values": [["a", "b"], ["1"], [], ["3", "4", "extra"]]},
    )

    frame = read_sheet(service, "id")

    assert frame.to_dicts() == [
        {"sheet_row": 2, "a": "1", "b": ""},
        {"sheet_row": 4, "a": "3", "b": "4"},
    ]


def test_a_request_row_is_updated_in_one_call_by_timestamp_and_email(make_recorder: RecorderFactory) -> None:
    """Write status, link and time with one `values.batchUpdate`."""
    service = make_recorder()
    frame = pl.DataFrame(
        {
            "sheet_row": [2, 3],
            "Carimbo de data/hora": ["t1", "t2"],
            "Endereço de e-mail": ["a@x", "b@x"],
            "status": ["", ""],
            "link_resultado": ["", ""],
            "processado_em": ["", ""],
        }
    )
    key = ("t2", "b@x")

    update_request_row(service, ("sheet", "Respostas"), frame, key, {"status": "concluido", "processado_em": "now"})

    [(path, kwargs)] = service.calls
    assert path == "spreadsheets.values.batchUpdate"
    assert kwargs["body"] == {
        "valueInputOption": "RAW",
        "data": [
            {"range": "'Respostas'!C3", "values": [["concluido"]]},
            {"range": "'Respostas'!E3", "values": [["now"]]},
        ],
    }


@pytest.mark.parametrize(
    ("timestamps", "message"),
    [
        pytest.param(["t1", "t3"], "found 0", id="the_row_is_gone"),
        pytest.param(["t2", "t2"], "found 2", id="two_rows_look_the_same"),
    ],
)
def test_a_request_that_matches_no_single_row_is_not_written(
    make_recorder: RecorderFactory, timestamps: list[str], message: str
) -> None:
    """Fail loud instead of writing a status on the wrong row."""
    service = make_recorder()
    frame = pl.DataFrame(
        {
            "sheet_row": [2, 3],
            "Carimbo de data/hora": timestamps,
            "Endereço de e-mail": ["b@x", "b@x"],
            "status": ["", ""],
        }
    )
    key = ("t2", "b@x")

    with pytest.raises(ValueError, match=message):
        update_request_row(service, ("sheet", "Respostas"), frame, key, {"status": "x"})

    assert service.calls == []


def test_the_format_is_one_batch_with_colors_dropdown_highlights_and_layout(make_recorder: RecorderFactory) -> None:
    """Send one `batchUpdate` with a ONE_OF_LIST rule for Keep, Exclude and New."""
    service = make_recorder(
        spreadsheets__get=TAB,
        spreadsheets__values__get={"values": [["Categoria", "Decisão"]]},
    )

    format_sheet(
        service,
        "sheet",
        header_colors={"Categoria": (0.1, 0.2, 0.3), "Decisão": (1.0, 0.8, 0.2), "Missing": (0, 0, 0)},
        dropdowns={"Decisão": ("Keep", "Exclude", "New")},
        highlights={"Decisão": (("Keep", (0.7, 0.8, 0.6)), ("Exclude", (0.9, 0.6, 0.6)))},
    )

    batches = [kwargs for path, kwargs in service.calls if path == "spreadsheets.batchUpdate"]
    assert len(batches) == 1
    requests = batches[0]["body"]["requests"]
    colored = [r["repeatCell"]["range"]["startColumnIndex"] for r in requests if "repeatCell" in r]
    assert colored == [0, 1]
    [validation] = [r["setDataValidation"] for r in requests if "setDataValidation" in r]
    assert validation["range"]["startColumnIndex"] == 1
    assert validation["rule"]["condition"]["type"] == "ONE_OF_LIST"
    assert validation["rule"]["strict"] is True
    assert [v["userEnteredValue"] for v in validation["rule"]["condition"]["values"]] == ["Keep", "Exclude", "New"]
    rules = [r["addConditionalFormatRule"]["rule"] for r in requests if "addConditionalFormatRule" in r]
    assert [rule["booleanRule"]["condition"]["values"][0]["userEnteredValue"] for rule in rules] == ["Keep", "Exclude"]
    frozen = [r["updateSheetProperties"] for r in requests if "updateSheetProperties" in r]
    assert frozen[0]["properties"]["gridProperties"]["frozenRowCount"] == 1
    assert any("updateDimensionProperties" in r for r in requests)


def test_a_dropdown_on_a_missing_column_is_an_error(make_recorder: RecorderFactory) -> None:
    """Never skip a dropdown without a word, because the secretariat needs it."""
    service = make_recorder(spreadsheets__get=TAB, spreadsheets__values__get={"values": [["Categoria"]]})

    with pytest.raises(ValueError, match="Decisão"):
        format_sheet(service, "sheet", dropdowns={"Decisão": ("Keep",)})


def test_a_sheet_is_created_by_drive_in_the_folder_then_named_and_filled(make_recorder: RecorderFactory) -> None:
    """Create the file with Drive, rename the tab, and write RAW values."""
    drive = make_recorder(files__create={"id": "new", "name": "Catálogo", "webViewLink": "https://x"})
    sheets = make_recorder(spreadsheets__get={"sheets": [{"properties": {"sheetId": 0, "title": "Sheet1"}}]})
    frame = pl.DataFrame({"a": ["1", None], "b": ["x" * 60000, "y"]})

    created = create_sheet(drive, sheets, "folder", ("Catálogo", "catalogo"), frame)

    assert created["id"] == "new"
    [(_, create)] = drive.calls
    assert create["body"] == {
        "name": "Catálogo",
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": ["folder"],
    }
    assert create["supportsAllDrives"] is True
    paths = [path for path, _ in sheets.calls]
    assert paths == ["spreadsheets.get", "spreadsheets.batchUpdate", "spreadsheets.values.update"]
    written = sheets.calls[-1][1]["body"]["values"]
    assert sheets.calls[-1][1]["valueInputOption"] == "RAW"
    assert written[0] == ["a", "b"]
    assert written[1][0] == "1"
    assert written[2][0] == ""
    assert len(written[1][1]) < 50000
