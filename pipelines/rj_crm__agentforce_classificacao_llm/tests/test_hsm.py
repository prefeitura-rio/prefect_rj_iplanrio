import pytest
from pydantic import JsonValue

from pipelines.rj_crm__agentforce_classificacao_llm.helpers.hsm import (
    build_button_lookup,
    button_titles,
    choose_button_row,
    enrich_sessions,
    find_fuzzy,
    is_late_button_reply,
    lcs_length,
    normalize_text,
    read_variables,
    render_hsm,
)
from pipelines.rj_crm__agentforce_classificacao_llm.tests.conftest import SessionFactory
from pipelines.rj_crm__agentforce_classificacao_llm.types import ButtonRow

BUTTONS = '{"0": {"title": "Sim"}, "1": {"title": "Não, obrigado"}, "2": "not a button"}'


def row(
    journey: str, hsm: str, buttons: str | None, activity: str | None = "a", *, post: bool | None = False
) -> ButtonRow:
    """Build a row of the catalog."""
    return ButtonRow(
        jornada_nome=journey,
        atividade_nome=activity,
        hsm_nome=hsm,
        hsm_botoes_json=buttons,
        template_pos_versao_indicador=post,
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("  Não,   Obrigado ", "nao, obrigado", id="accents_capitals_and_spaces"),
        pytest.param("SIM", "sim", id="capitals"),
        pytest.param(5, "5", id="a_number"),
    ],
)
def test_a_text_is_normalized_to_be_compared(text: object, expected: str) -> None:
    """Drop accents and capitals and join the spaces."""
    assert normalize_text(text) == expected


@pytest.mark.parametrize(
    ("buttons", "expected"),
    [
        pytest.param(BUTTONS, ["Sim", "Não, obrigado"], id="titles_of_the_buttons"),
        pytest.param(None, [], id="nothing"),
        pytest.param("  ", [], id="blank"),
        pytest.param("not json", [], id="not_json"),
        pytest.param("[1, 2]", [], id="a_list"),
    ],
)
def test_the_titles_of_the_buttons_come_from_their_json(buttons: str | None, expected: list[str]) -> None:
    """Read each button that is an object, and give none for a bad JSON."""
    assert button_titles(buttons) == expected


def test_the_row_of_the_catalog_prefers_a_version_before_the_change_and_breaks_a_tie_by_activity() -> None:
    """Choose a row that does not come after a change of the template, then by the name of the activity."""
    rows = [row("j", "h", "after", "a", post=True), row("j", "h", "second", "c"), row("j", "h", "first", "b")]

    assert choose_button_row(rows)["hsm_botoes_json"] == "first"
    assert choose_button_row([row("j", "h", "only", "a", post=True)])["hsm_botoes_json"] == "only"


def test_the_lookup_has_one_buttons_json_for_each_journey_and_hsm() -> None:
    """Count a repeated row once, and keep each pair apart."""
    rows = [row("j1", "h", "x"), row("j1", "h", "x"), row("j2", "h", "y"), row("j1", "other", None)]

    assert build_button_lookup(rows) == {("j1", "h"): "x", ("j2", "h"): "y", ("j1", "other"): None}


@pytest.mark.parametrize(
    ("first", "second", "length"),
    [
        pytest.param("", "abc", 0, id="empty"),
        pytest.param("abc", "abc", 3, id="equal"),
        pytest.param("solicitacao", "cc_wt_solicitacao", 11, id="inside_a_longer_key"),
        pytest.param("abc", "xyz", 0, id="nothing_in_common"),
        pytest.param("axbxc", "abc", 3, id="a_subsequence_that_is_not_contiguous"),
    ],
)
def test_the_longest_common_subsequence_has_its_length(first: str, second: str, length: int) -> None:
    """Count the letters that appear in the same order."""
    assert lcs_length(first, second) == length


@pytest.mark.parametrize(
    ("placeholder", "variables", "expected"),
    [
        pytest.param(
            "solicitacao", {"cc_wt_solicitacao": "entulho", "outra": "x"}, "entulho", id="a_key_with_a_prefix"
        ),
        pytest.param("canal", {"CC_WT_CANAL": "whatsapp"}, "whatsapp", id="capitals_do_not_count"),
        pytest.param("solicitacao", {"abc": "x"}, None, id="a_key_that_does_not_look_alike"),
        pytest.param("solicitacao", {}, None, id="no_key"),
    ],
)
def test_a_placeholder_finds_the_key_that_looks_most_like_it(
    placeholder: str, variables: dict[str, JsonValue], expected: JsonValue
) -> None:
    """Take the key with the longest common subsequence, if it covers 70% of the placeholder."""
    assert find_fuzzy(placeholder, variables) == expected


@pytest.mark.parametrize(
    ("variables", "expected"),
    [
        pytest.param('{"a": 1}', {"a": 1}, id="json_text"),
        pytest.param({"a": 1}, {"a": 1}, id="an_object"),
        pytest.param("not json", None, id="bad_json"),
        pytest.param("[1]", None, id="a_list"),
        pytest.param(None, None, id="nothing"),
    ],
)
def test_the_variables_of_an_hsm_are_an_object_or_nothing(
    variables: str | dict[str, JsonValue] | None, expected: object
) -> None:
    """Parse JSON text, and keep only an object."""
    assert read_variables(variables) == expected


@pytest.mark.parametrize(
    ("text", "variables", "expected"),
    [
        pytest.param(
            "olá ${nome}, seu ${tipo}", '{"nome": "Ana", "tipo": "pedido"}', "olá Ana, seu pedido", id="exact_keys"
        ),
        pytest.param("de ${solicitacao}", {"cc_wt_solicitacao": "poda"}, "de poda", id="a_key_with_a_prefix"),
        pytest.param("de ${outra}", {"nome": "Ana"}, "de ${outra}", id="a_placeholder_with_no_key_stays"),
        pytest.param("de ${a}", {"a": None}, "de ${a}", id="a_null_value_stays"),
        pytest.param("de ${a}", {"a": 5}, "de 5", id="a_number"),
        pytest.param("sem placeholder", {"a": 1}, "sem placeholder", id="no_placeholder"),
        pytest.param("de ${a}", None, "de ${a}", id="no_variables"),
        pytest.param("de ${a}", "not json", "de ${a}", id="bad_variables"),
        pytest.param(None, {"a": 1}, None, id="no_text"),
    ],
)
def test_the_hsm_text_gets_its_variables(
    text: str | None, variables: str | dict[str, JsonValue] | None, expected: str | None
) -> None:
    """Fill each placeholder, and leave visible the one that finds no value."""
    assert render_hsm(text, variables) == expected


@pytest.mark.parametrize(
    ("count", "message", "buttons", "late"),
    [
        pytest.param(1, "sim", BUTTONS, True, id="the_text_of_a_button"),
        pytest.param(1, " NÃO, Obrigado ", BUTTONS, True, id="accents_and_capitals_do_not_count"),
        pytest.param(2, "sim", BUTTONS, False, id="two_messages_never_count"),
        pytest.param(1, "talvez", BUTTONS, False, id="another_text"),
        pytest.param(1, "sim", None, False, id="no_buttons"),
    ],
)
def test_a_reply_that_is_the_text_of_a_button_is_late(
    make_session: SessionFactory, count: int, message: str, buttons: str | None, late: bool
) -> None:
    """Take a one-message session whose text is a button as a late reply."""
    session = make_session(qtd_mensagens_usuario=count, mensagens_usuario_concatenadas=message, hsm_botoes_json=buttons)

    assert is_late_button_reply(session) is late


def test_the_sessions_get_the_text_the_buttons_and_the_late_flag(make_session: SessionFactory) -> None:
    """Render the text, take the buttons of the journey and HSM, and mark the late reply."""
    sessions = [
        make_session(
            id_sessao="a", hsm_texto="oi ${n}", hsm_variaveis_json='{"n": "Ana"}', mensagens_usuario_concatenadas="sim"
        ),
        make_session(id_sessao="b", jornada_nome=None, hsm_nome=None, mensagens_usuario_concatenadas="sim"),
    ]

    first, second = enrich_sessions(sessions, {("j1", "hsm1"): BUTTONS})

    assert (first.hsm_texto, first.hsm_variaveis_json, first.hsm_botoes_json, first.late_button) == (
        "oi Ana",
        None,
        BUTTONS,
        True,
    )
    assert (second.hsm_botoes_json, second.late_button) == (None, False)
