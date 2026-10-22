"""Tasks of rj_crm__relatorio_engajamento_hsm."""

from asyncio import to_thread
from collections import Counter
from datetime import date, datetime

from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    CHATBOT_DATASET,
    CHATBOT_TABLE,
    DEFAULT_CLASSIFICATION_BATCH,
    DEFAULT_DISCOVERY_BATCH,
    DEFAULT_DISCOVERY_CAP,
    DEFAULT_EXAMPLES,
    DEFAULT_JUDGE_ALL_BELOW,
    DEFAULT_JUDGE_BATCH,
    DEFAULT_LARGE_CATEGORY_PCT,
    DISCOVERY_SEED,
    DISPATCHES_TABLE,
    DOCX_MIME,
    JUDGE_SEED,
    PROJECT_ID,
    SOURCES,
    SP_TZ,
    STALE_BATCHES_LIMIT,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers import bq, drive
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.conversations import build_conversations
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.prompts import (
    fill_prompt,
    first_hsm_text,
    format_conversations,
    format_judged,
    read_prompt,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.report import (
    build_categories,
    build_docx,
    categories_csv,
    detail_csv,
    report_slug,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.steps import (
    read_classifications,
    read_verdicts,
    register_discoveries,
    sample_conversations,
    select_for_judge,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.types import (
    Classification,
    Conversation,
    Dispatch,
    Extraction,
    Limits,
    Report,
)
from utils.bifrost import Bifrost, BifrostError, parse_json_list, scale_output_tokens
from utils.utils import render_query


@task(retries=2, retry_delay_seconds=30)
async def read_dispatches_task(reference: date) -> list[Dispatch]:
    """Return the dispatches that asked for a report on a day, with their campaign and start date."""
    sql = render_query(__file__, "pending_dispatches", dispatches_table=DISPATCHES_TABLE)
    dispatches = await to_thread(bq.read_dispatches, sql, reference)
    logger.info("Dispatches with a report for %s: %d", reference, len(dispatches))

    return dispatches


@task
async def report_exists_task(hsm_name: str, reference: date, root_id: str) -> bool:
    """Tell whether the folder of the HSM already has the report of the day, so a repeat adds nothing."""
    return await to_thread(lambda: drive.report_exists(drive.connect(), root_id, hsm_name, reference))


def conversations_sql(hsm_name: str, start: str, sources: str) -> str:
    """Render the query of the conversations with the expressions that stand for the name, the date and the sources."""
    return render_query(
        __file__,
        "extract_conversations",
        project=PROJECT_ID,
        dataset=CHATBOT_DATASET,
        table=CHATBOT_TABLE,
        hsm_name=hsm_name,
        start_date=start,
        sources=sources,
    )


@task(retries=2, retry_delay_seconds=30)
async def extract_task(dispatch: Dispatch) -> Extraction:
    """Read the messages of the sessions that got the HSM since its start date, and build the conversations.

    Only the sessions where the citizen answered make a conversation, and the totals count them all.
    """
    sql = conversations_sql("@nome_hsm", "@data_inicio", "unnest(@fontes)")
    messages = await to_thread(bq.read_messages, sql, dispatch.hsm_name, dispatch.sent_on)
    extraction = build_conversations(messages)
    logger.info(
        "Extracted: hsm=%s dispatches=%d engaged=%d", dispatch.hsm_name, extraction.total_dispatches, extraction.engaged
    )

    return extraction


async def ask_list(
    bifrost: Bifrost, prompt: str, max_tokens: int, step: str, batch: int
) -> list[dict[str, JsonValue]] | None:
    """Ask the model and return the objects of its answer, or nothing, and log it, if the batch fails."""
    try:
        return parse_json_list(await bifrost.ask(prompt, max_output_tokens=max_tokens))
    except (BifrostError, ValueError) as error:
        logger.warning("A batch of the %s failed: conversations=%d error=%s", step, batch, error)
        return None


@task
async def classify_task(
    conversations: list[Conversation], model: str, limits: Limits
) -> tuple[list[Classification], dict[str, str]]:
    """Discover the categories of the dispatch from a random sample, then classify every conversation.

    The discovery looks at the sample in batches and stops after 5 batches in a row with nothing new.
    It is the only step that adds a category. The classification only picks one that exists.
    Return the classifications and the catalog.
    """
    catalog = Catalog()
    discovery_batch = limits.get("discovery_batch", DEFAULT_DISCOVERY_BATCH)
    batch_size = limits.get("classification_batch", DEFAULT_CLASSIFICATION_BATCH)
    sample = sample_conversations(conversations, limits.get("discovery_cap", DEFAULT_DISCOVERY_CAP), DISCOVERY_SEED)

    async with Bifrost.connect(model) as bifrost:
        stale = 0

        for start in range(0, len(sample), discovery_batch):
            batch = sample[start : start + discovery_batch]
            prompt = fill_prompt(
                read_prompt("descoberta"), first_hsm_text(batch), catalog.as_prompt(), format_conversations(batch)
            )
            answer = await ask_list(bifrost, prompt, scale_output_tokens(discovery_batch), "discovery", len(batch))
            new, failed = register_discoveries(answer, catalog) if answer is not None else (0, len(batch))
            logger.info(
                "Discovery: batch=%d new=%d failed=%d catalog=%d",
                start // discovery_batch + 1,
                new,
                failed,
                len(catalog.entries),
            )
            stale = 0 if new else stale + 1

            if stale >= STALE_BATCHES_LIMIT:
                logger.info("The catalog is stable after %d batches with nothing new", stale)
                break

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

    logger.info("Classified: %d, failures: %s", len(results), dict(failures))

    return results, catalog.as_dict()


@task
async def judge_task(
    items: list[Classification], catalog: dict[str, str], model: str, limits: Limits
) -> list[Classification]:
    """Have a second call of the model audit a sample of the classifications, and return all of them with the verdicts.

    A category under `judge_all_below` conversations is judged in full, a larger one in `large_category_pct` percent.
    """
    below = limits.get("judge_all_below", DEFAULT_JUDGE_ALL_BELOW)
    pct = limits.get("large_category_pct", DEFAULT_LARGE_CATEGORY_PCT)
    batch_size = limits.get("judge_batch", DEFAULT_JUDGE_BATCH)
    selected = select_for_judge(items, below, pct, JUDGE_SEED)

    if not selected:
        logger.info("Nothing to judge")
        return items

    catalog_text = "\n".join(f"- {name}: {text}" for name, text in sorted(catalog.items())) or "(catálogo vazio)"
    judged: dict[str, Classification] = {}
    failed = 0

    async with Bifrost.connect(model) as bifrost:
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

    logger.info("Judged: %d, failed: %d", len(judged), failed)

    return [judged.get(item.conversation.session_id, item) for item in items]


@task(retries=2, retry_delay_seconds=30)
async def publish_task(report: Report, root_id: str, examples_per_category: int = DEFAULT_EXAMPLES) -> None:
    """Build the Word report and the two CSV files of a dispatch, and publish them in the folder of its HSM.

    The files are `<slug>.docx`, `categorias_<slug>.csv` and `classificacoes_<slug>.csv`.
    """
    categories = build_categories(report.items, report.catalog, examples_per_category)
    slug = report_slug(report.dispatch.hsm_name, report.reference)
    csv_names = [f"categorias_{slug}.csv", f"classificacoes_{slug}.csv"]
    sql = conversations_sql(
        "'" + report.dispatch.hsm_name.replace("'", "''") + "'",
        f"'{report.dispatch.sent_on.isoformat()}'",
        "(" + ", ".join(f"'{source}'" for source in SOURCES) + ")",
    )
    document = build_docx(report, categories, sql, csv_names, datetime.now(SP_TZ))
    client = await to_thread(drive.connect)
    await to_thread(drive.confirm_root, client, root_id)
    folder_id = await to_thread(drive.hsm_folder_id, client, root_id, report.dispatch.hsm_name)
    await to_thread(drive.upload, client, folder_id, f"{slug}.docx", document, DOCX_MIME)
    await to_thread(drive.upload, client, folder_id, csv_names[0], categories_csv(categories), "text/csv")
    await to_thread(drive.upload, client, folder_id, csv_names[1], detail_csv(report.items), "text/csv")
    logger.info("Report published: hsm=%s reference=%s", report.dispatch.hsm_name, report.reference)
