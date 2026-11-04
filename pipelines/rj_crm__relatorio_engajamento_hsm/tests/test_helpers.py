import pytest

from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.conversations import build_conversations
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.prompts import (
    fill_prompt,
    first_hsm_text,
    format_conversations,
    format_judged,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.text import normalize_label, slugify_hsm
from pipelines.rj_crm__relatorio_engajamento_hsm.tests.conftest import (
    ClassificationFactory,
    ConversationFactory,
    MessageFactory,
)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("Campanha 2026 - A!", "campanha_2026_a", id="symbols_become_one_underscore"),
        pytest.param("__x__", "x", id="edges_are_trimmed"),
        pytest.param("Ação", "a_o", id="accents_become_underscores"),
    ],
)
def test_an_hsm_name_becomes_a_slug(name: str, expected: str) -> None:
    """Keep letters and digits in lower case."""
    assert slugify_hsm(name) == expected


@pytest.mark.parametrize(
    ("first", "second"),
    [
        pytest.param("Benefício_Social", "beneficio social", id="accent_capitals_and_underscore"),
        pytest.param("  A   B ", "a b", id="runs_of_spaces"),
    ],
)
def test_two_names_that_differ_in_form_are_the_same_label(first: str, second: str) -> None:
    """Normalize before comparing."""
    assert normalize_label(first) == normalize_label(second)


def test_the_catalog_keeps_the_first_spelling_and_never_adds_on_a_find() -> None:
    """Register once per normalized name, and let a find give nothing if unknown."""
    catalog = Catalog()

    assert catalog.register("BENEFICIO_SOCIAL", " ajuda ") == "BENEFICIO_SOCIAL"
    assert catalog.register("Benefício Social", "other") == "BENEFICIO_SOCIAL"
    assert catalog.find("beneficio social") == "BENEFICIO_SOCIAL"
    assert catalog.find("unknown") is None
    assert catalog.as_dict() == {"BENEFICIO_SOCIAL": "ajuda"}
    assert len(catalog.entries) == 1


def test_the_catalog_prompt_is_sorted_or_says_that_it_is_empty() -> None:
    """List the categories by name."""
    catalog = Catalog()

    assert "nenhuma categoria" in catalog.as_prompt()

    catalog.register("B", "two")
    catalog.register("A", "one")

    assert catalog.as_prompt() == "- A: one\n- B: two"


def test_a_conversation_has_the_citizen_and_the_agent_lines_without_the_hsm(make_message: MessageFactory) -> None:
    """Order by time, label each line, keep the HSM text apart, count each session."""
    messages = [
        make_message("s1", "AI_AGENT_AGENTE", "resposta", minute=2),
        make_message("s1", "HSM", "texto do hsm", minute=0),
        make_message("s1", "AI_AGENT_CIDADAO", "pergunta", minute=1),
        make_message("s2", "HSM", "texto do hsm", minute=0),
        make_message("s2", "AI_AGENT_AGENTE", "so o agente", minute=1),
    ]

    result = build_conversations(messages)

    assert (result.total_dispatches, result.engaged) == (2, 1)
    [conversation] = result.conversations
    assert conversation.text == "CIDADÃO: pergunta\nAGENTE: resposta"
    assert conversation.hsm_text == "texto do hsm"
    assert (conversation.session_id, conversation.cpf, conversation.phone) == ("s1", "123", "5521")


def test_a_message_without_a_text_or_a_time_still_counts(make_message: MessageFactory) -> None:
    """Read a missing text as empty and put a message with no time last."""
    messages = [
        make_message("s1", "AI_AGENT_AGENTE", None, minute=0, msg_data=None),
        make_message("s1", "AI_AGENT_CIDADAO", "oi", minute=1),
    ]

    [conversation] = build_conversations(messages).conversations

    assert conversation.text == "CIDADÃO: oi\nAGENTE: "


def test_no_message_gives_no_conversation() -> None:
    """Return empty totals."""
    result = build_conversations([])

    assert (result.conversations, result.total_dispatches, result.engaged) == ([], 0, 0)


def test_the_prompt_puts_the_hsm_the_catalog_and_the_conversations_in_place(
    make_conversation: ConversationFactory,
) -> None:
    """Replace the three markers."""
    template = "H=<<HSM_TEXTO>> C=<<CATALOGO_CATEGORIAS>> V=<<CONVERSAS>>"

    assert fill_prompt(template, "hsm", "cat", "talk") == "H=hsm C=cat V=talk"
    assert first_hsm_text([make_conversation(hsm_text=""), make_conversation(hsm_text="second")]) == "second"
    assert first_hsm_text([]) == ""


def test_the_conversations_are_numbered_from_one(make_conversation: ConversationFactory) -> None:
    """Show the id of the session in each block."""
    text = format_conversations([make_conversation("a", "x"), make_conversation("b", "y")])

    assert text == "### CONVERSA 1 (id_sessao_48h=a)\nx\n\n### CONVERSA 2 (id_sessao_48h=b)\ny"


def test_the_judge_sees_the_category_and_the_justification(make_classification: ClassificationFactory) -> None:
    """Add the proposed category after each conversation."""
    text = format_judged([make_classification("a", "CAT", justification="because")])

    assert text.endswith("Categoria proposta: CAT\nJustificativa proposta: because")
    assert text.startswith("### CONVERSA 1 (id_sessao_48h=a)")
