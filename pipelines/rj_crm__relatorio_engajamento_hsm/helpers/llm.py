"""The calls to the model that discover, classify and judge the conversations."""

from collections import Counter
from collections.abc import Sequence

from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import STALE_BATCHES_LIMIT
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog
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

    It does so when the batch fails.
    """
    try:
        return parse_json_list(await bifrost.ask(prompt, max_output_tokens=max_tokens))
    except (BifrostError, ValueError) as error:
        logger.warning("A batch of the %s failed: conversations=%d error=%s", step, batch, error)
        return None


async def discover_catalog(bifrost: Bifrost, sample: Sequence[Conversation], catalog: Catalog, batch_size: int) -> None:
    """Add the categories that the sample shows to the catalog, in batches.

    Stop after `STALE_BATCHES_LIMIT` batches in a row with nothing new.
    """
    stale = 0

    for start in range(0, len(sample), batch_size):
        batch = sample[start : start + batch_size]
        prompt = fill_prompt(
            read_prompt("descoberta"), first_hsm_text(batch), catalog.as_prompt(), format_conversations(batch)
        )
        answer = await ask_list(bifrost, prompt, scale_output_tokens(batch_size), "discovery", len(batch))
        new, failed = register_discoveries(answer, catalog) if answer is not None else (0, len(batch))
        logger.info(
            "Discovery: batch=%d new=%d failed=%d catalog=%d",
            start // batch_size + 1,
            new,
            failed,
            len(catalog.entries),
        )
        stale = 0 if new else stale + 1

        if stale >= STALE_BATCHES_LIMIT:
            logger.info("The catalog is stable after %d batches with nothing new", stale)
            break


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
