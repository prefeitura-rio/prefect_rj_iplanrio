"""Tasks of rj_crm__relatorio_engajamento_hsm."""

from asyncio import to_thread
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import polars as pl
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    CHATBOT_DATASET,
    CHATBOT_TABLE,
    DEFAULT_CEILING,
    DEFAULT_CLASSIFICATION_BATCH,
    DEFAULT_CONCURRENCY,
    DEFAULT_DISCOVERY_BATCH,
    DEFAULT_DISCOVERY_CAP,
    DEFAULT_EXAMPLES,
    DEFAULT_JUDGE_ALL_BELOW,
    DEFAULT_JUDGE_BATCH,
    DEFAULT_JUDGE_CEILING,
    DEFAULT_LARGE_CATEGORY_PCT,
    DEFAULT_RAG_EXAMPLES,
    DISCOVERY_SEED,
    DISPATCHES_TABLE,
    EXAMPLES_CLOUD,
    EXAMPLES_TABLE,
    FORM_COLUMNS,
    FORM_LINK,
    FORM_PROCESSED_AT,
    FORM_SHEET_ID,
    FORM_STATUS,
    FORM_TAB,
    JUDGE_SEED,
    PROCESSED_AT_FORMAT,
    PROJECT_ID,
    SOURCES,
    STATUS_DONE,
    STATUS_PROCESSING,
    TYPE_QUICK,
    TYPE_VALIDATION,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers import bq
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers import drive as drive_helpers
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers import sheets as sheets_helpers
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import ValidCatalog, catalog_of
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.conversations import build_conversations
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.examples import (
    campaign_state,
    drive_state,
    fresh_rows,
    load_store,
    merge_store,
    parse_reviews,
    plan_sync,
    retrieve,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.llm import (
    classify_batches,
    classify_each,
    discover_catalog,
    judge_batches,
    suggest_categories,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.prompts import format_examples
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.report import (
    auto_catalog_frame,
    build_categories,
    build_docx,
    conversations_frame,
    publish_round,
    round_name,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.requests import (
    RequestError,
    error_status,
    gate_conversations,
    job_from_dispatch,
    job_from_request,
    plan_pending,
    skipped_status,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.steps import (
    cap_selection,
    sample_conversations,
    select_for_judge,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.text import citizen_text
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.validation import (
    plan_validation,
    publish_material,
    read_validated_catalog,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.types import (
    Classification,
    Conversation,
    Discovery,
    Dispatch,
    ExampleStore,
    Extraction,
    Job,
    Limits,
    Outcome,
    Prepared,
    Report,
    RoundFiles,
    Run,
    Window,
)
from utils.bifrost import Bifrost
from utils.constants import EMBEDDING_MODEL
from utils.drive import connect as connect_drive
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.utils import render_query

FORM = (FORM_SHEET_ID, FORM_TAB)


def sheet_time(moment: datetime) -> str:
    """Return a moment as the text that the response sheet holds, in São Paulo time."""
    return moment.strftime(PROCESSED_AT_FORMAT)


@task(retries=2, retry_delay_seconds=30)
async def read_form_jobs_task(today: date, released: list[str], run_id: str | None, now: datetime) -> list[Job]:
    """Read the response sheet and return the requests that run now.

    Stalled and invalid rows close with an error in the sheet. Each request that runs
    is marked as processing before it starts, so a second run does not take it too.
    """
    sheets = await to_thread(sheets_helpers.connect)
    frame = await to_thread(sheets_helpers.read_sheet, sheets, FORM_SHEET_ID, FORM_TAB)
    missing = [column for column in FORM_COLUMNS if column not in frame.columns]

    if missing:
        raise ValueError(f"The response sheet has no column {missing}: check the Form and the status columns")

    pending = plan_pending(frame, today, {name.strip() for name in released}, now.replace(tzinfo=None), run_id)
    stamp = sheet_time(now)

    for rejection in pending.rejections:
        values = {FORM_STATUS: rejection.status, FORM_LINK: "", FORM_PROCESSED_AT: stamp}
        await to_thread(sheets_helpers.update_request_row, sheets, FORM, frame, rejection.key, values)

    for request in pending.requests:
        values = {FORM_STATUS: STATUS_PROCESSING, FORM_LINK: "", FORM_PROCESSED_AT: stamp}
        await to_thread(
            sheets_helpers.update_request_row, sheets, FORM, frame, (request.timestamp, request.email), values
        )

    logger.info("Requests: run=%d closed=%d rows=%d", len(pending.requests), len(pending.rejections), frame.height)

    return [job_from_request(request) for request in pending.requests]


@task(retries=2, retry_delay_seconds=30)
async def read_dispatch_jobs_task(reference: date) -> list[Job]:
    """Return the jobs of the dispatches that asked for a report on a day.

    Each is a quick report by HSM name, since the dispatch day.
    """
    sql = render_query(__file__, "pending_dispatches", dispatches_table=DISPATCHES_TABLE)
    dispatches: list[Dispatch] = await to_thread(bq.read_dispatches, sql, reference)
    logger.info("Dispatches with a report for %s: %d", reference, len(dispatches))

    return [job_from_dispatch(dispatch, reference) for dispatch in dispatches]


@task
async def mark_status_task(key: tuple[str, str], status: str, link: str, now: datetime) -> bool:
    """Write the status, the link and the time on the row of a request.

    It never raises, because a status that fails to write must not stop the other
    requests of the run. The failure stays in the log.
    """
    values = {FORM_STATUS: status, FORM_LINK: link, FORM_PROCESSED_AT: sheet_time(now)}

    try:
        sheets = await to_thread(sheets_helpers.connect)
        frame = await to_thread(sheets_helpers.read_sheet, sheets, FORM_SHEET_ID, FORM_TAB)
        await to_thread(sheets_helpers.update_request_row, sheets, FORM, frame, key, values)
    except Exception as error:  # one status that fails to write must not stop the other requests
        logger.warning("The status was not written: key=%s status=%s error=%s", key, status, error)
        return False

    return True


@task(retries=2, retry_delay_seconds=30)
async def prepare_task(job: Job, root_id: str, folder_name: str) -> Prepared:
    """Make the folder of the campaign, and see if the round or the `Catalog` exists."""
    return await to_thread(lambda: drive_helpers.prepare(connect_drive(), job, root_id, folder_name))


@task
async def catalog_task(prepared: Prepared) -> ValidCatalog:
    """Read the `Catalog` that the secretariat validated.

    Raise `RequestError` if there is none or it has no decision. It does not retry,
    because no retry fixes a catalog.
    """
    return await to_thread(lambda: read_validated_catalog(sheets_helpers.connect(), prepared))


def conversations_sql(window: Window, *, literal: bool) -> str:
    """Render the conversations query for a window.

    The query that runs has parameters. The literal one is only for the report, so
    that anyone can run it again.
    """
    if literal:
        value = "'" + window.name.replace("'", "''") + "'"
        start, end = f"'{window.start.isoformat()}'", f"'{window.end.isoformat()}'"
        sources = "(" + ", ".join(f"'{source}'" for source in SOURCES) + ")"
    else:
        value, start, end, sources = "@value", "@start", "@end", "unnest(@sources)"

    return render_query(
        __file__,
        "extract_conversations",
        project=PROJECT_ID,
        dataset=CHATBOT_DATASET,
        table=CHATBOT_TABLE,
        filter_field=window.field,
        value=value,
        start_date=start,
        end_date=end,
        sources=sources,
    )


@task(retries=2, retry_delay_seconds=30)
async def extract_task(window: Window) -> Extraction:
    """Read the messages of the sessions of a window, and build the conversations.

    Only the sessions where the citizen answered make a conversation, and the totals
    count them all.
    """
    messages = await to_thread(bq.read_messages, conversations_sql(window, literal=False), window)
    extraction = build_conversations(messages)
    logger.info(
        "Extracted: name=%s dispatches=%d engaged=%d", window.name, extraction.total_dispatches, extraction.engaged
    )

    return extraction


@task
async def discover_task(conversations: list[Conversation], model: str, limits: Limits) -> Discovery:
    """Discover the categories from a random sample, and keep the examples of each.

    The discovery is the only step that adds a category.
    """
    catalog = catalog_of({})
    sample = sample_conversations(conversations, limits.get("discovery_cap", DEFAULT_DISCOVERY_CAP), DISCOVERY_SEED)

    async with Bifrost.connect(model) as bifrost:
        examples = await discover_catalog(
            bifrost, sample, catalog, limits.get("discovery_batch", DEFAULT_DISCOVERY_BATCH)
        )

    if not catalog.entries:
        raise RuntimeError("The discovery found no category: did every batch of the model fail?")

    return Discovery(catalog.as_dict(), examples)


@task(retries=2, retry_delay_seconds=30)
async def publish_material_task(folder_id: str, discovery: Discovery, conversations: list[Conversation]) -> str:
    """Publish the `Catalog` and the unclassified examples, and return the link."""

    def publish() -> str:
        return publish_material(connect_drive(), sheets_helpers.connect(), folder_id, discovery, conversations)

    return await to_thread(publish)


@task
async def classify_task(
    conversations: list[Conversation], catalog: dict[str, str], model: str, limits: Limits
) -> list[Classification]:
    """Classify the conversations in batches into the categories of the catalog.

    This is the zero-shot way, for a quick report. The classification only picks a
    category that exists.
    """
    async with Bifrost.connect(model) as bifrost:
        results, failures = await classify_batches(
            bifrost,
            conversations,
            catalog_of(catalog),
            limits.get("classification_batch", DEFAULT_CLASSIFICATION_BATCH),
        )

    logger.info("Classified: %d, failures: %s", len(results), dict(failures))

    return results


async def write_store(table: pl.DataFrame) -> None:
    """Replace the whole table of examples: write the files and publish the table.

    The flow runs one at a time, so no other write races with this one.
    """
    with TemporaryDirectory() as directory:
        table.write_parquet(Path(directory) / "examples.parquet")
        await upload_files_to_gcs.fn(
            Path(directory), EXAMPLES_TABLE, bucket=EXAMPLES_CLOUD["bucket"], dump_mode="overwrite"
        )

    await publish_table_in_bq.fn(EXAMPLES_TABLE, EXAMPLES_CLOUD)


@task
async def sync_examples_task(campaign: str, folder_id: str, catalog: ValidCatalog, model: str) -> ExampleStore:
    """Bring the example table up to date with the reviewed sheets of the campaign.

    A file that is new, changed or embedded with another model is read again. A file
    that is gone loses its examples. A file that did not change is not even read.
    Return the valid examples of the campaign, with their vectors.
    """
    drive = await to_thread(connect_drive)
    sheets = await to_thread(sheets_helpers.connect)
    files = await to_thread(drive_helpers.find_reviewed_sheets, drive, folder_id)
    table_name = f"{EXAMPLES_CLOUD['project']}.{EXAMPLES_TABLE['dataset_id']}.{EXAMPLES_TABLE['table_id']}"
    table = await to_thread(bq.read_examples, render_query(__file__, "stored_examples", examples_table=table_name))
    plan = plan_sync(drive_state(files), campaign_state(table, campaign), EMBEDDING_MODEL)
    by_id = {file["id"]: file for file in files}
    read: list[tuple[dict[str, str], pl.DataFrame]] = []

    for file_id in plan.read:
        frame = await to_thread(sheets_helpers.read_sheet, sheets, file_id)
        examples, discarded = parse_reviews(frame, catalog)

        for reason in discarded:
            logger.warning("A review row was discarded: file=%s reason=%s", by_id[file_id]["name"], reason)

        read.append((by_id[file_id], examples))

    texts = [citizen_text(text) for _, examples in read for text in examples["conversation"]]

    async with Bifrost.connect(model) as bifrost:
        vectors = await bifrost.embed(texts, model=EMBEDDING_MODEL) if texts else []

    stamp = (EMBEDDING_MODEL, datetime.now(tz=UTC))
    position = 0
    parts: list[pl.DataFrame] = []

    for file, examples in read:
        parts.append(fresh_rows(examples, campaign, file, vectors[position : position + examples.height], stamp))
        position += examples.height

    fresh = pl.concat(parts) if parts else table.clear()
    gone = [*plan.read, *plan.remove]
    stale = table.filter((pl.col("campaign") == campaign) & pl.col("file_id").is_in(gone)).height

    if fresh.height or stale:
        table = merge_store(table, plan, campaign, fresh)
        await write_store(table)

    store = load_store(table, campaign, EMBEDDING_MODEL, catalog)
    logger.info(
        "Examples: files=%d read=%d removed=%d unchanged=%d valid=%d",
        len(files),
        len(plan.read),
        len(plan.remove),
        plan.unchanged,
        store.examples.height,
    )

    return store


@task
async def classify_reviewed_task(
    conversations: list[Conversation], catalog: ValidCatalog, store: ExampleStore, model: str, limits: Limits
) -> list[Classification]:
    """Classify each conversation with the reviewed examples that are most like it.

    Each prompt carries the nearest examples of the same campaign, found by the
    similarity of what the citizen said. With no example it is zero-shot.
    """
    count = limits.get("rag_examples", DEFAULT_RAG_EXAMPLES)

    async with Bifrost.connect(model) as bifrost:
        if store.examples.is_empty() or count <= 0:
            blocks = [format_examples(store.examples.clear())] * len(conversations)
        else:
            vectors = await bifrost.embed([citizen_text(item.text) for item in conversations], model=EMBEDDING_MODEL)
            blocks = [format_examples(found) for found in retrieve(store, np.array(vectors), count)]

        results, failures = await classify_each(
            bifrost,
            conversations,
            catalog.active,
            blocks,
            limits.get("concurrency", DEFAULT_CONCURRENCY),
        )

    logger.info("Classified: %d, failures: %s", len(results), dict(failures))

    return results


@task
async def suggest_task(
    conversations: list[Conversation], catalog: ValidCatalog, model: str, limits: Limits
) -> dict[str, str] | None:
    """Suggest new categories that the catalog lacks, or nothing if the call fails.

    The suggestions are an extra, so a failure never stops the report.
    """
    sample = sample_conversations(conversations, limits.get("discovery_cap", DEFAULT_DISCOVERY_CAP), DISCOVERY_SEED)

    try:
        async with Bifrost.connect(model) as bifrost:
            return await suggest_categories(
                bifrost, sample, catalog.every, limits.get("discovery_batch", DEFAULT_DISCOVERY_BATCH)
            )
    except Exception as error:  # the suggestions are an extra: a failure must not stop the report
        logger.warning("The category suggestions failed: error=%s", error)
        return None


@task
async def judge_task(
    items: list[Classification], catalog: dict[str, str], model: str, limits: Limits
) -> list[Classification]:
    """Have a second model call audit a sample of the classifications.

    A category under `judge_all_below` conversations is judged in full, a larger one in
    `large_category_pct` percent, and the whole selection stops at `judge_ceiling`.
    Return all of them with the verdicts.
    """
    below = limits.get("judge_all_below", DEFAULT_JUDGE_ALL_BELOW)
    pct = limits.get("large_category_pct", DEFAULT_LARGE_CATEGORY_PCT)
    selected = select_for_judge(items, below, pct, JUDGE_SEED)
    selected = cap_selection(selected, limits.get("judge_ceiling", DEFAULT_JUDGE_CEILING), JUDGE_SEED)

    if not selected:
        logger.info("Nothing to judge")
        return items

    async with Bifrost.connect(model) as bifrost:
        judged, failed = await judge_batches(bifrost, selected, catalog, limits.get("judge_batch", DEFAULT_JUDGE_BATCH))

    logger.info("Judged: %d, failed: %d", len(judged), failed)

    return [judged.get(item.conversation.session_id, item) for item in items]


@task(retries=2, retry_delay_seconds=30)
async def publish_task(
    report: Report, folder_id: str, folder_name: str, now: datetime, examples_per_category: int = DEFAULT_EXAMPLES
) -> str:
    """Build the report and publish it in a new folder of the campaign.

    The folder holds the Google Doc and the sheet of classified conversations, and a
    quick report adds its automatic catalog. Return the link of the folder.
    """
    categories = build_categories(report.items, report.catalog, examples_per_category)
    document = build_docx(report, categories, conversations_sql(report.window, literal=True), now)
    files = RoundFiles(
        folder_name,
        document,
        conversations_frame(report.items, with_review=not report.quick),
        auto_catalog_frame(report.catalog, report.auto_examples, categories) if report.quick else None,
    )
    link = await to_thread(lambda: publish_round(connect_drive(), sheets_helpers.connect(), folder_id, files))
    logger.info("Report published: campaign=%s folder=%s", report.dispatch.hsm_name, folder_name)

    return link


async def run_validation(job: Job, prepared: Prepared, run: Run) -> Outcome:
    """Make the validation material: the `Catalog` to decide on, and the examples."""
    if skip := plan_validation(bool(prepared.catalog_id), 1):
        return Outcome(skip, prepared.catalog_link or drive_helpers.folder_link(prepared.folder_id))

    extraction = await extract_task(job.window)

    if skip := plan_validation(False, len(extraction.conversations)):
        return Outcome(skip)

    discovery = await discover_task(extraction.conversations, run.model, run.limits)
    link = await publish_material_task(prepared.folder_id, discovery, extraction.conversations)

    return Outcome(STATUS_DONE, link)


async def run_report(job: Job, prepared: Prepared, name: str, run: Run) -> Outcome:
    """Make an engagement report, or a quick one, in a new folder of the campaign."""
    limits = run.limits
    quick = job.request_type == TYPE_QUICK
    catalog = None if quick else await catalog_task(prepared)
    extraction = await extract_task(job.window)
    conversations = extraction.conversations

    if gate := gate_conversations(len(conversations), limits.get("ceiling", DEFAULT_CEILING), released=job.released):
        return Outcome(gate)

    notes: list[str] = []
    suggestions: dict[str, str] | None = None
    discovery = Discovery({}, {})

    if catalog is None:
        discovery = await discover_task(conversations, run.model, limits)
        items = await classify_task(conversations, discovery.catalog, run.model, limits)
        descriptions = discovery.catalog
    else:
        store = await sync_examples_task(job.campaign, prepared.folder_id, catalog, run.model)
        notes.append(f"A classificação usou {store.examples.height} exemplo(s) revisado(s) da campanha.")
        items = await classify_reviewed_task(conversations, catalog, store, run.model, limits)
        suggestions = await suggest_task(conversations, catalog, run.model, limits)
        descriptions = catalog.active.as_dict()

        if catalog.note():
            notes.append(catalog.note())

    if not items:
        return Outcome(skipped_status("nenhuma conversa foi classificada"))

    judged = await judge_task(items, descriptions, run.model, limits)
    report = Report(
        Dispatch(job.campaign, job.window.start),
        run.now.date(),
        judged,
        descriptions,
        extraction.total_dispatches,
        extraction.engaged,
        job.window,
        tuple(notes),
        suggestions,
        quick,
        discovery.examples,
    )
    examples = limits.get("examples_per_category", DEFAULT_EXAMPLES)
    link = await publish_task(report, prepared.folder_id, name, run.now, examples)
    status = STATUS_DONE if catalog is None or not catalog.note() else f"{STATUS_DONE} — {catalog.note()}"

    return Outcome(status, link)


@task
async def process_request_task(job: Job, run: Run) -> Outcome:
    """Make the report of one job, isolated from the others.

    An expected error, such as a missing catalog, ends as a status. An unexpected one
    also ends as a status, with the short run id, and is returned so that the flow
    fails after the other requests finish.
    """
    name = round_name(run.now, job.window.start, job.window.end, quick=job.request_type == TYPE_QUICK)

    try:
        prepared = await prepare_task(job, run.root_id, name)

        if prepared.outcome:
            return prepared.outcome

        if job.request_type == TYPE_VALIDATION:
            return await run_validation(job, prepared, run)

        return await run_report(job, prepared, name, run)
    except RequestError as error:
        return Outcome(error_status(str(error)))
    except Exception as error:  # one request that fails must not stop the others
        reason = f"{type(error).__name__}: {error}"
        logger.exception("The request failed: campaign=%s", job.campaign)

        return Outcome(error_status(reason, run.run_id), unexpected=f"{job.campaign}: {reason}")
