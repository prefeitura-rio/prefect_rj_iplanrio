from datetime import UTC, datetime

import pytest
from pydantic import JsonValue

from pipelines.rj_crm__get_history_data.helpers.history import (
    build_rows,
    filter_recent,
    find_columns,
    keep_history,
    merge_record,
    normalize_field_name,
    parse_entry_date,
)
from pipelines.rj_crm__get_history_data.helpers.sfmc import (
    build_fields_envelope,
    build_list_envelope,
    parse_extensions,
    parse_fields,
)
from pipelines.rj_crm__get_history_data.types import Extension, Extraction, Field, Row

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)

LIST_ANSWER = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body><RetrieveResponseMsg xmlns="http://exacttarget.com/wsdl/partnerAPI">
<OverallStatus>MoreDataAvailable</OverallStatus><RequestID>req-1</RequestID>
<Results><Name>Clientes_Historico</Name><CustomerKey>k1</CustomerKey><ObjectID>o1</ObjectID></Results>
<Results><Name>Outra</Name><CustomerKey>k2</CustomerKey></Results>
</RetrieveResponseMsg></soapenv:Body></soapenv:Envelope>"""

FIELDS_ANSWER = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body><RetrieveResponseMsg xmlns="http://exacttarget.com/wsdl/partnerAPI">
<OverallStatus>OK</OverallStatus>
<Results><Name>Telefone</Name><FieldType>Phone</FieldType><IsPrimaryKey>false</IsPrimaryKey></Results>
<Results><Name>Id</Name><FieldType>Text</FieldType><IsPrimaryKey>True</IsPrimaryKey></Results>
</RetrieveResponseMsg></soapenv:Body></soapenv:Envelope>"""


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("  Data_de-Entrada ", "data de entrada", id="separators_and_capitals"),
        pytest.param("Data   de entrada", "data de entrada", id="runs_of_spaces"),
    ],
)
def test_a_field_name_is_normalized_to_be_compared(name: str, expected: str) -> None:
    """Join spaces, underscores and hyphens."""
    assert normalize_field_name(name) == expected


def test_only_the_extensions_that_end_in_historico_are_kept() -> None:
    """Match the suffix in any case."""
    extensions = [Extension("A_HISTORICO", "1"), Extension("a_historico_x", "2"), Extension("b_historico", "3")]

    assert [item.external_key for item in keep_history(extensions)] == ["1", "3"]


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        pytest.param(
            [Field("Tel", "Phone", False), Field("Id", "Text", True), Field("Data de Entrada", "Date", False)],
            ("Tel", "Id", "Data de Entrada"),
            id="the_name_has_data_de_entrada",
        ),
        pytest.param(
            [Field("Tel", "phone", False), Field("Quando", "Date", False)],
            ("Tel", None, "Quando"),
            id="the_only_date_column",
        ),
        pytest.param(
            [Field("A", "Date", False), Field("B", "Date", False)],
            (None, None, None),
            id="two_date_columns_are_ambiguous",
        ),
        pytest.param([], (None, None, None), id="no_field"),
    ],
)
def test_the_columns_of_the_phone_the_key_and_the_entry_date_are_found(
    fields: list[Field], expected: tuple[str | None, str | None, str | None]
) -> None:
    """Take the entry date by name, or the only column of type date."""
    assert find_columns(fields) == expected


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        pytest.param({"keys": {"id": 1}, "values": {"a": 2}}, {"id": 1, "a": 2}, id="keys_and_values"),
        pytest.param({"values": {"a": 2}}, {"a": 2}, id="only_values"),
        pytest.param({"a": 1}, {"a": 1}, id="a_flat_record"),
        pytest.param({"keys": "bad", "values": {"a": 2}}, {"a": 2}, id="keys_that_are_not_an_object"),
    ],
)
def test_a_record_of_the_rest_api_becomes_one_object(
    record: dict[str, JsonValue], expected: dict[str, JsonValue]
) -> None:
    """Join `keys` and `values`."""
    assert merge_record(record) == expected


def test_each_record_becomes_a_row_with_the_columns_found_in_any_case() -> None:
    """Look up the phone, key and date ignoring case, and skip a failed extension."""
    fields = [Field("Telefone", "Phone", False), Field("Id", "Text", True), Field("Data de Entrada", "Date", False)]
    records: list[dict[str, JsonValue]] = [
        {"keys": {"id": "7"}, "values": {"telefone": "5521", "data de entrada": "2026-09-01"}}
    ]

    rows = build_rows([Extraction("DE_historico", records, fields), Extraction("broken", error="boom")])

    assert len(rows) == 1
    assert (rows[0]["de_nome"], rows[0]["telefone"], rows[0]["id_de"], rows[0]["entrada_data"]) == (
        "DE_historico",
        "5521",
        "7",
        "2026-09-01",
    )
    assert "5521" in str(rows[0]["dados_json"])


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("2026-09-01", datetime(2026, 9, 1), id="iso_date"),
        pytest.param("2026-09-01T10:00:00-03:00", datetime(2026, 9, 1, 13), id="iso_with_an_offset_goes_to_utc"),
        pytest.param("9/15/2026 3:04:05 PM", datetime(2026, 9, 15, 15, 4, 5), id="month_first_with_am_pm"),
        pytest.param("15/09/2026 10:00:00", datetime(2026, 9, 15, 10), id="day_first_when_the_day_is_over_12"),
        pytest.param("not a date", None, id="not_a_date"),
        pytest.param("", None, id="empty"),
        pytest.param(None, None, id="nothing"),
        pytest.param(5, None, id="a_number"),
    ],
)
def test_the_entry_date_is_read_in_the_formats_of_sfmc(value: JsonValue, expected: datetime | None) -> None:
    """Return a time in UTC with no zone."""
    assert parse_entry_date(value) == expected


def row(entry: str | None, phone: str = "1", de: str = "de") -> Row:
    """Build a row of the history."""
    return {"de_nome": de, "telefone": phone, "id_de": "x", "entrada_data": entry, "dados_json": "{}"}


def test_only_the_rows_of_the_window_stay_and_a_row_with_no_date_always_does() -> None:
    """Drop the old rows, stamp the day, and keep the first of a repeated key."""
    rows = [
        row("2026-09-20", "1"),
        row("2026-08-01", "2"),
        row(None, "3"),
        row("not a date", "4"),
        row("2026-09-20", "1"),
    ]

    kept = filter_recent(rows, 30, NOW)

    assert [item["telefone"] for item in kept] == ["1", "3", "4"]
    assert [item["data_particao"] for item in kept] == ["2026-09-20", "2026-09-29", "2026-09-29"]


def test_the_cutoff_is_exact_to_the_day_count() -> None:
    """Keep a row that is exactly at the cutoff, drop the one just before."""
    assert len(filter_recent([row("2026-08-30T12:00:00")], 30, NOW)) == 1
    assert filter_recent([row("2026-08-30T11:59:59")], 30, NOW) == []


def test_the_soap_messages_carry_the_token_and_the_request() -> None:
    """Build the list, the next page of the list and the fields."""
    first = build_list_envelope("tok").decode()
    more = build_list_envelope("tok", "req-1").decode()
    fields = build_fields_envelope("tok", "key-9").decode()

    assert "<fueloauth" in first
    assert ">tok<" in first
    assert "<ObjectType>DataExtension</ObjectType>" in first
    assert "<ContinueRequest>req-1</ContinueRequest>" in more
    assert "<ObjectType>" not in more
    assert "<Value>key-9</Value>" in fields


def test_the_answers_of_the_soap_api_are_read() -> None:
    """Read the status, the id of the request, the extensions and the fields."""
    status, request_id, extensions = parse_extensions(LIST_ANSWER)

    assert (status, request_id) == ("MoreDataAvailable", "req-1")
    assert extensions == [Extension("Clientes_Historico", "k1"), Extension("Outra", "k2")]
    assert parse_fields(FIELDS_ANSWER) == [Field("Telefone", "Phone", False), Field("Id", "Text", True)]


def test_an_answer_with_no_response_message_is_refused() -> None:
    """Raise `ValueError`."""
    with pytest.raises(ValueError, match="RetrieveResponseMsg"):
        parse_fields('<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"/>')
