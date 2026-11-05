"""The calls to the model that discover, classify and judge the conversations."""

from asyncio import Semaphore, gather
from collections import Counter
from collections.abc import Sequence

from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    ANSWER_ATTEMPTS,
    CLASSIFICATION_TOKENS,
    STALE_BATCHES_LIMIT,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog, catalog_of
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.prompts import (
    fill_prompt,
    first_hsm_text,
    format_conversations,
    format_judged,
    read_prompt,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.steps import (
    read_classifications,
    read_verdicts,
    register_discoveries,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Classification, Conversation
from utils.bifrost import Bifrost, BifrostError, parse_json_list, scale_output_tokens


async def ask_list(
    bifrost: Bifrost, prompt: str, max_tokens: int, step: str, batch: int
) -> list[dict[str, JsonValue]] | None:
    """Ask the model and return the objects of its answer, or log and return nothing.

    An answer that is not a whole JSON list, such as one that the gateway cut off, is
    asked again up to `ANSWER_ATTEMPTS` times. A gateway error is not, because
    `Bifrost.answer` already tried. Nothing comes back after the last try.
    """
    for attempt in range(1, ANSWER_ATTEMPTS + 1):
        try:
            return parse_json_list(await bifrost.ask(prompt, max_output_tokens=max_tokens))
        except BifrostError as error:
            logger.warning("A batch of the %s failed: conversations=%d error=%s", step, batch, error)
            return None
        except ValueError as error:
            logger.warning(
                "A batch of the %s has no valid answer: conversations=%d attempt=%d of %d error=%s",
                step,
                batch,
                attempt,
                ANSWER_ATTEMPTS,
                error,
            )

    return None


async def discover_catalog(
    bifrost: Bifrost, sample: Sequence[Conversation], catalog: Catalog, batch_size: int
) -> dict[str, list[str]]:
    """Add the categories that the sample shows to the catalog, in batches.

    Return the session ids of the examples of each new category. Stop after
    `STALE_BATCHES_LIMIT` batches in a row with nothing new.
    """
    stale = 0
    examples: dict[str, list[str]] = {}

    for start in range(0, len(sample), batch_size):
        batch = sample[start : start + batch_size]
        prompt = fill_prompt(
            read_prompt("descoberta"), first_hsm_text(batch), catalog.as_prompt(), format_conversations(batch)
        )
        answer = await ask_list(bifrost, prompt, scale_output_tokens(batch_size), "discovery", len(batch))
        found, failed = register_discoveries(answer, batch, catalog) if answer is not None else ({}, len(batch))
        examples |= found
        logger.info(
            "Discovery: batch=%d new=%d failed=%d catalog=%d",
            start // batch_size + 1,
            len(found),
            failed,
            len(catalog.entries),
        )
        stale = 0 if found else stale + 1

        if stale >= STALE_BATCHES_LIMIT:
            logger.info("The catalog is stable after %d batches with nothing new", stale)
            break

    return examples


async def suggest_categories(
    bifrost: Bifrost, sample: Sequence[Conversation], known: Catalog, batch_size: int
) -> dict[str, str]:
    """Return the new categories that the sample suggests, with their descriptions.

    The discovery starts from `known`, so it never suggests a category that the
    catalog has, even one that the secretariat excluded. The suggestions never enter
    the catalog that the classification uses.
    """
    working = catalog_of(known.as_dict())
    before = set(working.entries)
    await discover_catalog(bifrost, sample, working, batch_size)

    return {name: description for key, (name, description) in working.entries.items() if key not in before}


async def classify_batches(
    bifrost: Bifrost, conversations: Sequence[Conversation], catalog: Catalog, batch_size: int
) -> tuple[list[Classification], Counter[str]]:
    """Classify every conversation in batches, and count the ones that failed."""
    results: list[Classification] = []
    failures: Counter[str] = Counter()

    for start in range(0, len(conversations), batch_size):
        batch = conversations[start : start + batch_size]
        prompt = fill_prompt(
            read_prompt("classificacao"), first_hsm_text(batch), catalog.as_prompt(), format_conversations(batch)
        )
        answer = await ask_list(bifrost, prompt, scale_output_tokens(batch_size), "classification", len(batch))

        if answer is None:
            failures["lote_inteiro_falhou"] += len(batch)
            continue

        found, missed = read_classifications(answer, batch, catalog)
        results += found
        failures += missed
        logger.info(
            "Classification: batch=%d classified=%d failed=%d",
            start // batch_size + 1,
            len(found),
            sum(missed.values()),
        )

    return results, failures


async def classify_each(
    bifrost: Bifrost,
    conversations: Sequence[Conversation],
    catalog: Catalog,
    examples: Sequence[str],
    concurrency: int,
) -> tuple[list[Classification], Counter[str]]:
    """Classify each conversation in its own call, with its own reviewed examples.

    `examples` has the prompt block of each conversation, in the same order. At most
    `concurrency` calls run at the same time. A call that fails leaves only its
    conversation unclassified.
    """
    gate = Semaphore(max(1, concurrency))

    async def one(conversation: Conversation, block: str) -> tuple[list[Classification], Counter[str]]:
        prompt = fill_prompt(
            read_prompt("classificacao"),
            conversation.hsm_text,
            catalog.as_prompt(),
            format_conversations([conversation]),
            block,
        )

        async with gate:
            answer = await ask_list(bifrost, prompt, CLASSIFICATION_TOKENS, "classification", 1)

        if answer is None:
            return [], Counter({"lote_inteiro_falhou": 1})

        return read_classifications(answer, [conversation], catalog)

    outcomes = await gather(*(one(item, block) for item, block in zip(conversations, examples, strict=True)))
    results: list[Classification] = []
    failures: Counter[str] = Counter()

    for found, missed in outcomes:
        results += found
        failures += missed

    return results, failures


async def judge_batches(
    bifrost: Bifrost, selected: Sequence[Classification], catalog: dict[str, str], batch_size: int
) -> tuple[dict[str, Classification], int]:
    """Judge the selected classifications in batches, and count the ones that failed."""
    catalog_text = "\n".join(f"- {name}: {text}" for name, text in sorted(catalog.items())) or "(catálogo vazio)"
    judged: dict[str, Classification] = {}
    failed = 0

    for start in range(0, len(selected), batch_size):
        batch = selected[start : start + batch_size]
        prompt = fill_prompt(
            read_prompt("juiz"),
            first_hsm_text([item.conversation for item in batch]),
            catalog_text,
            format_judged(batch),
        )
        answer = await ask_list(bifrost, prompt, scale_output_tokens(50), "judge", len(batch))

        if answer is None:
            failed += len(batch)
            continue

        found, missed = read_verdicts(answer, batch)
        judged.update(found)
        failed += missed

    return judged, failed
