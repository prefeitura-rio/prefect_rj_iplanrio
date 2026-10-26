from datetime import date
from pathlib import Path

import polars as pl
import pytest
from httpx2 import ConnectError, Response
from pydantic import JsonValue

from pipelines.rj_crm__disparo_template.helpers.debts import (
    enrich_with_debts,
    escape_json,
    fetch_debts,
    format_debts,
    is_success,
)
from pipelines.rj_crm__disparo_template.helpers.notices import (
    is_confirmed,
    mask_cpf,
    sample_text,
    success_notice,
)
from pipelines.rj_crm__disparo_template.helpers.queries import build_query
from pipelines.rj_crm__disparo_template.helpers.sftp import SftpTarget, remote_file, upload
from pipelines.rj_crm__disparo_template.tests.conftest import FakeSftp, make_client
from pipelines.rj_crm__disparo_template.types import Audience, Csv, Report

MONDAY = date(2026, 7, 13)
SATURDAY = date(2026, 7, 18)
TARGET = SftpTarget(host="h", user="u", password="p", host_key="ssh-rsa AAAA", remote_path="/Import/")
URL = "https://debts.test/consulta"


def test_the_query_is_read_from_a_file_of_the_folder(tmp_path: Path) -> None:
    """Read the file, and keep its text as it is when there are no replacements."""
    (tmp_path / "q.sql").write_text("SELECT {x}", encoding="utf-8")

    assert build_query(Audience(query_file="q.sql"), tmp_path, MONDAY) == "SELECT {x}"


def test_the_query_can_be_inline_and_with_replacements(tmp_path: Path) -> None:
    """Fill the placeholders, with a processor if one is named."""
    audience = Audience(query="SELECT {x}", replacements={"x": 7})
    weekend = Audience(
        query="SELECT {days_ahead_placeholder}", replacements={"days_ahead_placeholder": 1}, processor="skip_weekends"
    )

    assert build_query(audience, tmp_path, MONDAY) == "SELECT 7"
    assert build_query(weekend, tmp_path, MONDAY) == "SELECT 1"
    assert build_query(weekend, tmp_path, SATURDAY) is None


def test_a_run_without_a_query_is_refused(tmp_path: Path) -> None:
    """Raise `ValueError`."""
    with pytest.raises(ValueError, match="query"):
        build_query(Audience(), tmp_path, MONDAY)


@pytest.mark.parametrize(
    ("remote_path", "expected"),
    [
        pytest.param("/Import/", "/Import/a.csv", id="a_folder_with_a_slash"),
        pytest.param("/Import", "/Import/a.csv", id="a_folder_without_it"),
        pytest.param("", "a.csv", id="no_folder"),
        pytest.param("/", "a.csv", id="the_root_gives_the_bare_name"),
    ],
)
def test_the_path_of_a_file_on_the_server(remote_path: str, expected: str) -> None:
    """Join the folder and the name with one slash."""
    target = SftpTarget(host="h", user="u", password="p", host_key="k", remote_path=remote_path)

    assert remote_file(target, "a.csv") == expected


async def test_a_file_goes_to_the_server_with_its_text() -> None:
    """Put the file at the path of the folder, and give the path."""
    server = FakeSftp()

    remote = await upload(TARGET, Csv("a_20260717.csv", "telefone\n5521999\n", "2026-07-17 09:30:05"), server.opener)

    assert remote == "/Import/a_20260717.csv"
    assert server.sent == [("/Import/a_20260717.csv", "telefone\n5521999\n")]
    assert server.targets == [TARGET]


@pytest.mark.parametrize(
    ("open_error", "put_error"),
    [
        pytest.param(ConnectionRefusedError("host key"), None, id="the_server_is_refused"),
        pytest.param(None, OSError("disk full"), id="the_copy_fails"),
    ],
)
async def test_a_failure_of_the_server_reaches_the_flow(
    open_error: Exception | None, put_error: Exception | None
) -> None:
    """Do not swallow the error: a file that did not go out must fail the run."""
    server = FakeSftp(open_error, put_error)

    with pytest.raises((ConnectionRefusedError, OSError)):
        await upload(TARGET, Csv("a.csv", "x", "2026-07-17 09:30:05"), server.opener)

    assert server.sent == []


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(True, True, id="true"),
        pytest.param(" TRUE ", True, id="text"),
        pytest.param("false", False, id="false_text"),
        pytest.param(None, False, id="nothing"),
        pytest.param(1, False, id="a_number"),
    ],
)
def test_the_success_flag_of_the_api_is_read(value: object, expected: bool) -> None:
    """Accept a bool or the text true."""
    assert is_success(value) is expected  # pyright: ignore[reportArgumentType]


def test_a_value_is_escaped_for_sfmc() -> None:
    """Escape inner quotes with a backslash, add no quotes, and keep a null."""
    assert escape_json({"a": "b"}) == '{\\"a\\": \\"b\\"}'
    assert escape_json(None) is None
    assert escape_json(["x"]) == '[\\"x\\"]'


def test_the_debts_are_written_for_a_person_to_read() -> None:
    """Number the lines, and write each kind of debt in its own way."""
    debts: JsonValue = [
        {"cda": "01/02", "valor": "R$10,00"},
        {"guia": "2026/1", "data_ultimo_pagamento": "05/07/2026"},
        {"outro": "x"},
    ]

    assert format_debts(debts) == (
        "1. CDA 01/02 - Valor: R$10,00\n2. Guia nº 2026/1 - Data do Último Pagamento: 05/07/2026\n3. Outro: x"
    )
    assert format_debts('[{"cda": "1", "valor": "2"}]') == "1. CDA 1 - Valor: 2"
    assert format_debts(None) is None
    assert format_debts([]) is None


async def test_the_debts_of_a_cpf_are_asked_again_after_a_failure() -> None:
    """Wait 1 and 2 seconds between the tries, and stop at the first answer."""
    client, seen = make_client(Response(500), ConnectError("down"), Response(200, json={"api_resposta_sucesso": True}))
    waits: list[float] = []

    async def pause(seconds: float) -> None:
        waits.append(seconds)

    answer = await fetch_debts(client, "111", URL, pause)

    assert answer == {"api_resposta_sucesso": True}
    assert waits == [1.0, 2.0]
    assert len(seen) == 3


async def test_the_debts_of_a_cpf_give_nothing_after_every_try_fails() -> None:
    """Return nothing after three failures."""
    client, seen = make_client(Response(500), Response(500), Response(500))

    async def pause(seconds: float) -> None:
        del seconds

    assert await fetch_debts(client, "111", URL, pause) is None
    assert len(seen) == 3


async def test_only_the_cpfs_with_a_confirmed_debt_stay_in_the_frame() -> None:
    """Keep the CPF that the API confirms, and drop the rest."""
    client, _ = make_client(
        Response(
            200,
            json={
                "api_resposta_sucesso": "true",
                "debitos_msg": [{"cda": "1", "valor": "R$1"}],
                "dicionario_itens": {"a": 1},
                "lista_cdas": [{"cda": "1"}],
            },
        ),
        Response(200, json={"api_resposta_sucesso": False}),
        Response(500),
        Response(500),
        Response(500),
    )
    frame = pl.DataFrame({"cpf": ["111", "222", "333"], "telefone": ["a", "b", "c"]})

    async def pause(seconds: float) -> None:
        del seconds

    enriched = await enrich_with_debts(frame, "cpf", URL, client, pause)

    assert enriched["cpf"].to_list() == ["111"]
    assert enriched["debitos_msg"].to_list() == ["1. CDA 1 - Valor: R$1"]
    assert enriched["itens_informados"].to_list() == ['[\\"a\\"]']
    assert enriched["api_resposta_sucesso"].to_list() == ["true"]
    assert "total_parcelado" in enriched.columns


def test_the_cpf_is_masked_in_the_example() -> None:
    """Show three characters of the CPF."""
    assert mask_cpf("12345678901") == "123********"
    assert mask_cpf(None) == ""
    assert sample_text({"telefone": "5521999", "cpf": "12345678901", "nome": "Ana"}) == (
        '{\n  "telefone": "5521999",\n  "cpf": "123********",\n  "nome": "Ana"\n}'
    )


def test_the_success_notice_says_the_try_and_has_a_masked_example() -> None:
    """Write the numbers, the campaign, the try and the example."""
    report = Report("campanha", "2026-07-17 09:30:05", 12, 2, test_mode=False)

    text = success_notice(report, {"telefone": "5521999", "cpf": "12345678901"}, 1, 3)

    assert "12 disparos" in text
    assert "2 lotes" in text
    assert "Tentativa:** 1 de 3" in text
    assert "123********" in text
    assert "[TESTE]" not in text
    assert "[TESTE]" in success_notice(Report("c", "d", 1, 1, test_mode=True), None, 1, 1)


@pytest.mark.parametrize(
    ("count", "total", "expected"),
    [
        pytest.param(20, 100, True, id="exactly_a_fifth"),
        pytest.param(19, 100, False, id="under_a_fifth"),
        pytest.param(0, 0, True, id="nothing_to_send"),
    ],
)
def test_a_fifth_of_the_sends_with_a_status_confirms_the_dispatch(count: int, total: int, expected: bool) -> None:
    """Compare the count with a fifth of the total."""
    assert is_confirmed(count, total) is expected
