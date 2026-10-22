"""The steps of the model: how to read what it answers for the discovery, the classification and the judge."""

from collections import Counter
from collections.abc import Mapping, Sequence
from math import ceil
from random import Random

from pydantic import JsonValue

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import INCORRECT, NO_CATEGORY, VALID_VERDICTS
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Classification, Conversation


def text_of(value: JsonValue) -> str:
    """Return a value as trimmed text, with nothing as empty text."""
    return str(value or "").strip()


def sample_conversations(conversations: Sequence[Conversation], cap: int, seed: int) -> list[Conversation]:
    """Return the conversations in a random order, at most `cap` of them, the same for the same seed."""
    shuffled = list(conversations)
    Random(seed).shuffle(shuffled)

    return shuffled[:cap]


def register_discoveries(answer: Sequence[Mapping[str, JsonValue]], catalog: Catalog) -> tuple[int, int]:
    """Add the new categories of an answer to the catalog, and return how many were new and how many items failed.

    An item with no `categoria_nova` fails, and one that the catalog already has is not new.
    """
    new = failed = 0

    for item in answer:
        name = text_of(item.get("categoria_nova"))

        if not name:
            failed += 1
            continue

        before = len(catalog.entries)
        catalog.register(name, text_of(item.get("categoria_nova_descricao")))
        new += len(catalog.entries) > before

    return new, failed


def read_classifications(
    answer: Sequence[Mapping[str, JsonValue]], batch: Sequence[Conversation], catalog: Catalog
) -> tuple[list[Classification], Counter[str]]:
    """Return the classifications of an answer, matched by the number of each conversation, and why the others failed.

    A conversation fails when the answer has no category for it, says `NENHUMA_DO_CATALOGO`, or
    names a category that the catalog does not have. The classification never adds a category.
    """
    by_number = {item.get("id"): item for item in answer}
    results: list[Classification] = []
    failures: Counter[str] = Counter()

    for number, conversation in enumerate(batch, start=1):
        item = by_number.get(number)
        raw = text_of(item.get("categoria")) if item else ""

        if not item or not raw:
            failures["sem_categoria_na_resposta"] += 1
        elif raw.upper() == NO_CATEGORY:
            failures["nenhuma_do_catalogo"] += 1
        elif (name := catalog.find(raw)) is None:
            failures["categoria_nao_existe_no_catalogo"] += 1
        else:
            results.append(
                Classification(
                    conversation,
                    name,
                    text_of(item.get("justificativa")),
                    text_of(item.get("resumo")),
                )
            )

    return results, failures


def judge_target(size: int, judge_all_below: int, large_category_pct: float) -> int:
    """Return how many conversations of a category of this size the judge looks at.

    A small category is judged in full, because a sample of a few would leave a huge margin of error.
    A large one is judged in a share, since the margin is small well before the whole of it.
    """
    return size if size < judge_all_below else ceil(large_category_pct / 100 * size)


def select_for_judge(
    items: Sequence[Classification], judge_all_below: int, large_category_pct: float, seed: int
) -> list[Classification]:
    """Return the classifications for the judge: a sample of each category, in alphabetical order of the categories."""
    by_category: dict[str, list[Classification]] = {}

    for item in items:
        by_category.setdefault(item.category, []).append(item)

    selected: list[Classification] = []

    for category in sorted(by_category):
        group = by_category[category]
        target = min(judge_target(len(group), judge_all_below, large_category_pct), len(group))
        selected += Random(seed).sample(group, target)

    return selected


def read_verdicts(
    answer: Sequence[Mapping[str, JsonValue]], batch: Sequence[Classification]
) -> tuple[dict[str, Classification], int]:
    """Return the judged classifications of an answer by session id, and how many conversations had no valid verdict.

    The expected category is kept only for an `INCORRETO`.
    """
    by_number = {item.get("id"): item for item in answer}
    judged: dict[str, Classification] = {}
    failed = 0

    for number, current in enumerate(batch, start=1):
        item = by_number.get(number)
        verdict = text_of(item.get("veredito")).upper() if item else ""

        if not item or verdict not in VALID_VERDICTS:
            failed += 1
            continue

        judged[current.conversation.session_id] = Classification(
            current.conversation,
            current.category,
            current.justification,
            current.summary,
            verdict,
            text_of(item.get("categoria_esperada")) if verdict == INCORRECT else "",
            text_of(item.get("justificativa")),
        )

    return judged, failed
