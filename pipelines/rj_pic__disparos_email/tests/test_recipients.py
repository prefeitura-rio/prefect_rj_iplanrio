from pathlib import Path

import pytest

from pipelines.rj_pic__disparos_email.constants import TEMPLATE_DIR
from pipelines.rj_pic__disparos_email.helpers.recipients import (
    build_mail,
    display_name,
    parse_filter,
    parse_students,
    render_body,
)

STUDENT = {"cpf": "1", "nome": "Beto", "status": "irregular", "protocolo": "P-1"}


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("", [], id="empty"),
        pytest.param("sem arroba", [], id="no_address"),
        pytest.param("a@x.org", ["a@x.org"], id="one_address"),
        pytest.param(" a@x.org , b@x.org", ["a@x.org", "b@x.org"], id="spaces_are_trimmed"),
        pytest.param("o'neil@x.org", ["o\\'neil@x.org"], id="a_quote_is_escaped"),
    ],
)
def test_the_filter_gives_the_addresses(text: str, expected: list[str]) -> None:
    """Split by comma, trim, and escape quotes."""
    assert parse_filter(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("", [], id="empty"),
        pytest.param("[]", [], id="an_empty_list"),
        pytest.param('[{"nome": "Beto"}]', [{"nome": "Beto"}], id="json_list"),
        pytest.param("[{'nome': 'Beto'}]", [{"nome": "Beto"}], id="python_list_with_single_quotes"),
        pytest.param('{"nome": "Beto"}', [{"nome": "Beto"}], id="one_object_becomes_a_list"),
        pytest.param("not a literal", [], id="neither_json_nor_python"),
        pytest.param("[1, 2]", [], id="items_that_are_not_objects"),
        pytest.param("null", [], id="json_null"),
    ],
)
def test_the_students_come_from_json_or_a_python_literal(text: str, expected: list[dict[str, str]]) -> None:
    """Read JSON first, then a Python literal, and never raise."""
    assert parse_students(text) == expected


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        pytest.param({"recipiente_email": "ana@x.org", "recipiente_nome": " Ana "}, "Ana", id="the_name"),
        pytest.param({"recipiente_email": "ana@x.org", "recipiente_nome": ""}, "ana", id="the_part_of_the_address"),
        pytest.param({"recipiente_email": "", "recipiente_nome": "Ana"}, "", id="no_address"),
    ],
)
def test_the_display_name(row: dict[str, str], expected: str) -> None:
    """Use the name, then the part of the address before the arroba."""
    assert display_name(row) == expected


def test_the_body_lists_the_students_and_escapes_html() -> None:
    """Render the students with their status, and escape the markup of a name."""
    body = render_body(
        TEMPLATE_DIR,
        {
            "recipiente_nome": "Ana <b>",
            "alunos": [STUDENT, {**STUDENT, "nome": "<script>", "status": "regular"}],
            "total_alunos": 2,
            "data_atualizacao": "2026-07-17",
        },
    )

    assert "Olá, Ana &lt;b&gt;!" in body
    assert "<strong>2</strong>" in body
    assert "&lt;script&gt;" in body
    assert "<script>" not in body
    assert "P-1" in body


def test_a_row_becomes_a_mail_for_its_address() -> None:
    """Build the mail with the address, the name, the subject and the rendered body."""
    row = {"recipiente_email": " ana@x.org ", "recipiente_nome": "Ana", "dados": '[{"nome": "Beto"}]'}

    mail = build_mail(row, "Assunto", TEMPLATE_DIR)

    assert mail is not None
    assert (mail.to, mail.name, mail.subject) == ("ana@x.org", "Ana", "Assunto")
    assert "Beto" in mail.body


def test_a_row_with_no_address_gives_no_mail(tmp_path: Path) -> None:
    """Skip the row, and do not read the template."""
    assert build_mail({"recipiente_email": "  "}, "Assunto", tmp_path) is None
