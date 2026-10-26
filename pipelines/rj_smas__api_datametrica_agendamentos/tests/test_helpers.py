from datetime import date

import pytest
from pydantic import JsonValue

from pipelines.rj_smas__api_datametrica_agendamentos.helpers import (
    build_dispatch_parameters,
    build_headers,
    build_rows,
    read_target_date,
)

FULL: dict[str, JsonValue] = {
    "id": 1,
    "id_capacidade": 2,
    "nome_completo": "Ana Maria Silva",
    "primeiro_nome": "Ana",
    "cpf": "123",
    "telefone": "5521",
    "tipo": "x",
    "data_hora": "2026-05-27 10:00:00",
    "nome": "CRAS Centro",
    "endereco": "Rua A",
    "bairro": "Centro",
}


@pytest.mark.parametrize(
    ("today", "given", "expected"),
    [
        pytest.param(date(2026, 5, 25), None, date(2026, 5, 27), id="monday_looks_two_days_ahead"),
        pytest.param(date(2026, 5, 28), None, date(2026, 6, 1), id="thursday_looks_four_days_ahead"),
        pytest.param(date(2026, 5, 29), None, date(2026, 6, 2), id="friday_looks_four_days_ahead"),
        pytest.param(date(2026, 5, 30), None, None, id="saturday_does_nothing"),
        pytest.param(date(2026, 5, 31), None, None, id="sunday_does_nothing"),
        pytest.param(date(2026, 5, 30), "2026-06-10", date(2026, 6, 10), id="a_date_given_wins_on_a_weekend"),
    ],
)
def test_the_date_to_fetch_follows_the_weekday_unless_one_is_given(
    today: date, given: str | None, expected: date | None
) -> None:
    """Read the target date of the run."""
    assert read_target_date(today, given) == expected


def test_a_record_becomes_a_row_with_the_names_of_the_table_and_a_json_text_is_read() -> None:
    """Rename the unit fields, parse JSON text, skip empty text, and raise for a missing field."""
    text: JsonValue = '{"id": 1, "id_capacidade": 2, "nome_completo": "A", "primeiro_nome": "A", "cpf": "1", "telefone": "2", "tipo": "x", "data_hora": "d", "nome": "U", "endereco": "E", "bairro": "B"}'
    records: list[JsonValue] = [FULL, text, "  ", 3]
    rows = build_rows(records)

    assert len(rows) == 2
    assert rows[0]["unidade_nome"] == "CRAS Centro"
    assert rows[0]["unidade_bairro"] == "Centro"
    assert rows[1]["unidade_endereco"] == "E"

    with pytest.raises(KeyError):
        build_rows([{"id": 1}])


def test_the_headers_carry_both_tokens() -> None:
    """Send the proxy token and the bearer token."""
    headers = build_headers("t", "p")

    assert headers["X-Proxy-Api-Token"] == "p"
    assert headers["Authorization"] == "Bearer t"


def test_the_dispatch_parameters_carry_the_query_for_the_table_and_the_days() -> None:
    """Render the dispatch query with the table, the days ahead and the message."""
    parameters = build_dispatch_parameters("p.d.t", 2)
    query = str(parameters["query"])

    assert "FROM `p.d.t`" in query
    assert "CAST(2 AS int64)" in query
    assert "nome_hsm = 'confirma_agendamento_cadunico_prod_v2'" in query
    assert parameters["test_mode"] is False
    assert "{{" not in query
