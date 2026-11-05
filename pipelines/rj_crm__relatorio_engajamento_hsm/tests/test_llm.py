from collections import Counter
from collections.abc import Sequence
from typing import cast, final

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import STALE_BATCHES_LIMIT
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.llm import (
    classify_batches,
    classify_each,
    discover_catalog,
    judge_batches,
    suggest_categories,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tests.conftest import ClassificationFactory, ConversationFactory
from utils.bifrost import Bifrost, BifrostError


@final
class FakeBifrost:
    """Answer each call with the next scripted text, or raise the scripted error."""

    def __init__(self, answers: Sequence[str | BifrostError]) -> None:
        """Keep the answers, and record each call."""
        self.answers = list(answers)
        self.calls = 0
        self.asked: list[tuple[str, int]] = []

    async def ask(self, prompt: str, max_output_tokens: int) -> str:
        """Record the call, and return the next answer."""
        self.asked.append((prompt, max_output_tokens))
        answer = self.answers[self.calls]
        self.calls += 1

        if isinstance(answer, BifrostError):
            raise answer

        return answer


def gateway(fake: FakeBifrost) -> Bifrost:
    """Return the fake as the gateway that the helpers expect."""
    return cast("Bifrost", cast("object", fake))


async def test_the_discovery_stops_after_the_stale_limit(make_conversation: ConversationFactory) -> None:
    """Stop after batches in a row with nothing new, and keep the first discovery."""
    sample = [make_conversation(str(number)) for number in range(20)]
    new = '[{"categoria_nova": "NOVA", "categoria_nova_descricao": "d"}]'
    fake = FakeBifrost([new] + ["[]"] * 9)
    catalog = Catalog()

    await discover_catalog(gateway(fake), sample, catalog, 2)

    assert fake.calls == 1 + STALE_BATCHES_LIMIT
    assert catalog.as_dict() == {"NOVA": "d"}


async def test_a_failed_batch_of_the_discovery_counts_as_stale(make_conversation: ConversationFactory) -> None:
    """Treat a gateway error as a batch with nothing new, and go on."""
    sample = [make_conversation(str(number)) for number in range(4)]
    fake = FakeBifrost([BifrostError("down"), '[{"categoria_nova": "NOVA"}]'])
    catalog = Catalog()

    await discover_catalog(gateway(fake), sample, catalog, 2)

    assert fake.calls == 2
    assert catalog.as_dict() == {"NOVA": ""}


async def test_the_classification_counts_a_batch_that_fails(make_conversation: ConversationFactory) -> None:
    """Classify the first batch, and count the whole second batch as failed."""
    conversations = [make_conversation(str(number)) for number in range(3)]
    catalog = Catalog()
    catalog.register("A", "d")
    answer = '[{"id": 1, "categoria": "A"}, {"id": 2, "categoria": "A"}]'
    fake = FakeBifrost([answer, BifrostError("down")])

    results, failures = await classify_batches(gateway(fake), conversations, catalog, 2)

    assert [item.conversation.session_id for item in results] == ["0", "1"]
    assert failures == Counter({"lote_inteiro_falhou": 1})


async def test_the_judge_returns_the_verdicts_and_counts_the_failures(
    make_classification: ClassificationFactory,
) -> None:
    """Judge two batches, where the second one fails."""
    selected = [make_classification(str(number)) for number in range(3)]
    fake = FakeBifrost(['[{"id": 1, "veredito": "CORRETO"}, {"id": 2, "veredito": "CORRETO"}]', BifrostError("down")])

    judged, failed = await judge_batches(gateway(fake), selected, {"A": "d"}, 2)

    assert sorted(judged) == ["0", "1"]
    assert {item.verdict for item in judged.values()} == {"CORRETO"}
    assert failed == 1


async def test_the_discovery_returns_the_example_sessions_of_each_new_category(
    make_conversation: ConversationFactory,
) -> None:
    """Map the example numbers of a batch to session ids."""
    sample = [make_conversation(f"s{number}") for number in range(1, 5)]
    answer = '[{"categoria_nova": "NOVA", "categoria_nova_descricao": "d", "exemplos": [2, 4]}]'
    catalog = Catalog()

    examples = await discover_catalog(gateway(FakeBifrost([answer, "[]", "[]", "[]", "[]", "[]"])), sample, catalog, 4)

    assert examples == {"NOVA": ["s2", "s4"]}


async def test_the_suggestions_leave_out_every_category_that_the_catalog_has(
    make_conversation: ConversationFactory,
) -> None:
    """Never suggest an excluded category, and never change the catalog of the run."""
    sample = [make_conversation("s1")]
    known = Catalog()
    known.register("EXCLUDED", "was removed")
    answer = (
        '[{"categoria_nova": "excluded", "categoria_nova_descricao": "again"},'
        ' {"categoria_nova": "FRESH", "categoria_nova_descricao": "new topic"}]'
    )

    suggestions = await suggest_categories(gateway(FakeBifrost([answer] + ["[]"] * 5)), sample, known, 1)

    assert suggestions == {"FRESH": "new topic"}
    assert known.as_dict() == {"EXCLUDED": "was removed"}


async def test_each_conversation_is_classified_with_its_own_examples(make_conversation: ConversationFactory) -> None:
    """Send one call for each conversation and keep going after a failure."""
    conversations = [make_conversation("s1", "CIDADÃO: um"), make_conversation("s2", "CIDADÃO: dois")]
    catalog = Catalog()
    catalog.register("A", "d")
    fake = FakeBifrost(['[{"id": 1, "categoria": "A", "resumo": "r"}]', BifrostError("down")])

    results, failures = await classify_each(gateway(fake), conversations, catalog, ["EX-ONE", "EX-TWO"], 1)

    assert [item.conversation.session_id for item in results] == ["s1"]
    assert failures == Counter({"lote_inteiro_falhou": 1})
    assert "EX-ONE" in fake.asked[0][0]
    assert "CIDADÃO: um" in fake.asked[0][0]
    assert "EX-TWO" in fake.asked[1][0]
    assert "CIDADÃO: dois" in fake.asked[1][0]
