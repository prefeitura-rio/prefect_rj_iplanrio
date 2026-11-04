"""Tasks of rj_crm__relatorio_engajamento_hsm."""

from asyncio import to_thread
from datetime import date, datetime

from prefect import task
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
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers import bq
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.conversations import build_conversations
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.drive import report_exists
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.llm import classify_batches, discover_catalog, judge_batches
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.report import (
    build_categories,
    build_docx,
    categories_csv,
    detail_csv,
    report_slug,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.steps import (
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
from utils.bifrost import Bifrost
from utils.drive import confirm_root, connect, ensure_folder, upload
from utils.utils import render_query


@task(retries=2, retry_delay_seconds=30)
async def read_dispatches_task(reference: date) -> list[Dispatch]:
    """Return the dispatches that asked for a report on a day.

    Each has its campaign and start date.
    """
    sql = render_query(__file__, "pending_dispatches", dispatches_table=DISPATCHES_TABLE)
    dispatches = await to_thread(bq.read_dispatches, sql, reference)
    logger.info("Dispatches with a report for %s: %d", reference, len(dispatches))

    return dispatches


@task
async def report_exists_task(hsm_name: str, reference: date, root_id: str) -> bool:
    """Tell whether the HSM folder has the report of the day.

    A repeat then adds nothing.
    """
    return await to_thread(lambda: report_exists(connect(), root_id, hsm_name, reference))


def conversations_sql(hsm_name: str, start: str, sources: str) -> str:
    """Render the conversations query with the name, date and source expressions."""
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
    """Read the messages of the sessions that got the HSM, and build conversations.

    It reads since the start date of the HSM.

    Only the sessions where the citizen answered make a conversation, and the totals
    count them all.
    """
    sql = conversations_sql("@nome_hsm", "@data_inicio", "unnest(@fontes)")
    messages = await to_thread(bq.read_messages, sql, dispatch.hsm_name, dispatch.sent_on)
    extraction = build_conversations(messages)
    logger.info(
        "Extracted: hsm=%s dispatches=%d engaged=%d", dispatch.hsm_name, extraction.total_dispatches, extraction.engaged
    )

    return extraction


@task
async def classify_task(
    conversations: list[Conversation], model: str, limits: Limits
) -> tuple[list[Classification], dict[str, str]]:
    """Discover the categories of the dispatch from a random sample, then classify all.

    The discovery is the only step that adds a category. The classification only picks
    one that exists. Return the classifications and the catalog.
    """
    catalog = Catalog()
    sample = sample_conversations(conversations, limits.get("discovery_cap", DEFAULT_DISCOVERY_CAP), DISCOVERY_SEED)

    async with Bifrost.connect(model) as bifrost:
        await discover_catalog(bifrost, sample, catalog, limits.get("discovery_batch", DEFAULT_DISCOVERY_BATCH))
        results, failures = await classify_batches(
            bifrost, conversations, catalog, limits.get("classification_batch", DEFAULT_CLASSIFICATION_BATCH)
        )

    logger.info("Classified: %d, failures: %s", len(results), dict(failures))

    return results, catalog.as_dict()


@task
async def judge_task(
    items: list[Classification], catalog: dict[str, str], model: str, limits: Limits
) -> list[Classification]:
    """Have a second model call audit a sample of the classifications.

    A category under `judge_all_below` conversations is judged in full, a larger one in
    `large_category_pct` percent. Return all of them with the verdicts.
    """
    below = limits.get("judge_all_below", DEFAULT_JUDGE_ALL_BELOW)
    pct = limits.get("large_category_pct", DEFAULT_LARGE_CATEGORY_PCT)
    selected = select_for_judge(items, below, pct, JUDGE_SEED)

    if not selected:
        logger.info("Nothing to judge")
        return items

    async with Bifrost.connect(model) as bifrost:
        judged, failed = await judge_batches(bifrost, selected, catalog, limits.get("judge_batch", DEFAULT_JUDGE_BATCH))

    logger.info("Judged: %d, failed: %d", len(judged), failed)

    return [judged.get(item.conversation.session_id, item) for item in items]


@task(retries=2, retry_delay_seconds=30)
async def publish_task(report: Report, root_id: str, examples_per_category: int = DEFAULT_EXAMPLES) -> None:
    """Build the Word report and two CSV files of a dispatch, and publish them.

    They go in the folder of its HSM.

    The files are `<slug>.docx`, `categorias_<slug>.csv` and
    `classificacoes_<slug>.csv`.
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
    client = await to_thread(connect)
    await to_thread(confirm_root, client, root_id)
    folder_id = await to_thread(ensure_folder, client, root_id, report.dispatch.hsm_name)
    await to_thread(upload, client, folder_id, f"{slug}.docx", document, DOCX_MIME)
    await to_thread(upload, client, folder_id, csv_names[0], categories_csv(categories), "text/csv")
    await to_thread(upload, client, folder_id, csv_names[1], detail_csv(report.items), "text/csv")
    logger.info("Report published: hsm=%s reference=%s", report.dispatch.hsm_name, report.reference)
