import pytest
from pydantic import JsonValue

from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.steps import (
    judge_target,
    read_classifications,
    read_verdicts,
    register_discoveries,
    sample_conversations,
    select_for_judge,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tests.conftest import ClassificationFactory, ConversationFactory


def test_a_sample_is_capped_and_the_same_for_the_same_seed(make_conversation: ConversationFactory) -> None:
    """Shuffle with the seed, and cut at the cap."""
    conversations = [make_conversation(str(number)) for number in range(10)]

    first = sample_conversations(conversations, 4, 42)

    assert len(first) == 4
    assert first == sample_conversations(conversations, 4, 42)
    assert sorted(item.session_id for item in sample_conversations(conversations, 20, 1)) == sorted(
        item.session_id for item in conversations
    )


def test_a_discovery_adds_new_categories_and_counts_the_items_that_fail() -> None:
    """Add a new name once, ignore a repeat of the catalog, and fail an item with no name."""
    catalog = Catalog()
    catalog.register("EXISTENTE", "d")
    answer: list[dict[str, JsonValue]] = [
        {"categoria_nova": "NOVA", "categoria_nova_descricao": "descrição"},
        {"categoria_nova": "existente", "categoria_nova_descricao": "other"},
        {"categoria_nova": ""},
        {"outra": "x"},
    ]

    assert register_discoveries(answer, catalog) == (1, 2)
    assert catalog.as_dict() == {"EXISTENTE": "d", "NOVA": "descrição"}


def test_a_classification_picks_only_a_category_that_exists(
    make_conversation: ConversationFactory,
) -> None:
    """Match by the number, and count why each of the others failed."""
    catalog = Catalog()
    catalog.register("A", "d")
    batch = [make_conversation(str(number)) for number in range(1, 6)]
    answer: list[dict[str, JsonValue]] = [
        {"id": 1, "categoria": "a", "justificativa": " why ", "resumo": "sum"},
        {"id": 2, "categoria": "NENHUMA_DO_CATALOGO"},
        {"id": 3, "categoria": "unknown"},
        {"id": 4, "categoria": ""},
    ]

    results, failures = read_classifications(answer, batch, catalog)

    assert [(item.conversation.session_id, item.category, item.justification, item.summary) for item in results] == [
        ("1", "A", "why", "sum")
    ]
    assert dict(failures) == {
        "nenhuma_do_catalogo": 1,
        "categoria_nao_existe_no_catalogo": 1,
        "sem_categoria_na_resposta": 2,
    }
    assert catalog.as_dict() == {"A": "d"}


@pytest.mark.parametrize(
    ("size", "target"),
    [
        pytest.param(5, 5, id="a_small_category_is_judged_in_full"),
        pytest.param(19, 19, id="just_under_the_threshold"),
        pytest.param(20, 6, id="at_the_threshold_a_share_is_judged"),
        pytest.param(100, 30, id="a_large_category_gets_the_share"),
        pytest.param(33, 10, id="the_share_rounds_up"),
    ],
)
def test_the_judge_looks_at_all_of_a_small_category_and_a_share_of_a_large_one(size: int, target: int) -> None:
    """Judge in full under 20 conversations, else 30%."""
    assert judge_target(size, 20, 30.0) == target


def test_the_judge_sample_takes_a_share_of_each_category_in_alphabetical_order(
    make_classification: ClassificationFactory,
) -> None:
    """Keep the small category whole, and sample the large one."""
    items = [make_classification(f"b{number}", "B") for number in range(3)]
    items += [make_classification(f"a{number}", "A") for number in range(40)]

    selected = select_for_judge(items, 20, 30.0, 42)

    assert [item.category for item in selected] == ["A"] * 12 + ["B"] * 3
    assert len({item.conversation.session_id for item in selected}) == 15
    assert select_for_judge([], 20, 30.0, 42) == []


def test_a_verdict_matches_by_number_and_keeps_the_expected_category_only_when_incorrect(
    make_classification: ClassificationFactory,
) -> None:
    """Fail a verdict that is not valid or has no item, and clear the expected category of a correct one."""
    batch = [make_classification(str(number)) for number in range(1, 5)]
    answer: list[dict[str, JsonValue]] = [
        {"id": 1, "veredito": "correto", "categoria_esperada": "ignored", "justificativa": "ok"},
        {"id": 2, "veredito": "INCORRETO", "categoria_esperada": "B", "justificativa": "no"},
        {"id": 3, "veredito": "talvez"},
    ]

    judged, failed = read_verdicts(answer, batch)

    assert failed == 2
    assert (judged["1"].verdict, judged["1"].expected_category, judged["1"].judge_note) == ("CORRETO", "", "ok")
    assert (judged["2"].verdict, judged["2"].expected_category) == ("INCORRETO", "B")
    assert set(judged) == {"1", "2"}
