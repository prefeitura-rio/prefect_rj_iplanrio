"""Tasks of rj_crm__agentforce_classificacao_llm."""

from asyncio import Semaphore, gather, to_thread
from datetime import UTC, datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import task
from pydantic import TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_crm__agentforce_classificacao_llm.constants import (
    DEFAULT_HSM_MAX_DAYS_BEFORE,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MAX_WORKERS,
    LLM_MAX_OUTPUT_TOKENS,
    LLM_TIMEOUT,
)
from pipelines.rj_crm__agentforce_classificacao_llm.helpers import bq
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.hsm import build_button_lookup, enrich_sessions
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.labels import build_classified_row
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.schema import COLUMNS, PRIMARY_KEY
from pipelines.rj_crm__agentforce_classificacao_llm.types import (
    ButtonRow,
    Llm,
    Names,
    Prompted,
    Relevant,
    Row,
    Rule,
    Session,
    Window,
)
from utils.bifrost import Bifrost, BifrostError, parse_json_object
from utils.constants import BIFROST_MODEL
from utils.settings import NoticeSettings
from utils.tasks import notify_discord
from utils.utils import render_query

SESSIONS: TypeAdapter[list[Session]] = TypeAdapter(list[Session])
BUTTON_ROWS: TypeAdapter[list[ButtonRow]] = TypeAdapter(list[ButtonRow])
RELEVANT: TypeAdapter[list[Relevant]] = TypeAdapter(list[Relevant])
TABLE_DESCRIPTION = (
    "Classificação inicial por LLM das sessões do Agentforce (WhatsApp), uma linha por id_sessao, "
    "reescrita por MERGE se a sessão for reclassificada. Alimentada pela pipeline rj_crm__agentforce_classificacao_llm."
)
TMP_DESCRIPTION = "Tmp da pipeline rj_crm__agentforce_classificacao_llm, limpa a cada carga."


@task
async def ensure_tables_task(names: Names) -> None:
    """Check that the classification table and its tmp table exist.

    Create the one that is missing.
    """
    await to_thread(bq.ensure_tables, names.project, names.destination, names.tmp, (TABLE_DESCRIPTION, TMP_DESCRIPTION))


@task(retries=2, retry_delay_seconds=30)
async def extract_task(names: Names, window: Window) -> list[Session]:
    """Read the sessions of the last days with no class, and the HSM before each.

    There are two filters of "already classified": a cheap one on the source, that may
    be 15 minutes late, and an anti-join with the destination table, that is the
    guarantee.
    """
    sql = render_query(
        __file__,
        "extract_sessions",
        lookback_days=window.get("lookback_days", DEFAULT_LOOKBACK_DAYS),
        hsm_max_days_before=window.get("hsm_max_days_before", DEFAULT_HSM_MAX_DAYS_BEFORE),
        source_table=names.source,
        destination_table=names.destination,
    )
    sessions = SESSIONS.validate_python(await to_thread(bq.read_rows, names.project, sql))
    logger.info("Sessions to classify: count=%d", len(sessions))

    return sessions


@task(retries=2, retry_delay_seconds=30)
async def enrich_task(names: Names, sessions: list[Session]) -> list[Session]:
    """Render the HSM text of the sessions, and mark those that are a button text.

    The buttons are the only thing that comes from the catalog of HSMs by journey,
    because the text and the variables already come with each session from the dispatch
    itself.
    """
    journeys = sorted(
        {session.jornada_nome for session in sessions if session.jornada_nome and session.jornada_nome.strip()}
    )
    buttons: dict[tuple[str, str], str | None] = {}

    if journeys:
        sql = render_query(__file__, "hsm_buttons", catalog_table=names.hsm_catalog, journeys=journeys)
        buttons = build_button_lookup(BUTTON_ROWS.validate_python(await to_thread(bq.read_rows, names.project, sql)))

    enriched = enrich_sessions(sessions, buttons)
    logger.info("Late replies to a button: count=%d", sum(1 for session in enriched if session.late_button))

    return enriched


@task(retries=2, retry_delay_seconds=30)
async def read_rules_task(names: Names, stage: str) -> list[Rule]:
    """Read the active rules of a stage (`tema` or `motivo`), or none yet.

    They come from the catalog.

    The catalog is managed outside the pipeline, so a missing table means that there are
    no rules today.
    """
    sql = render_query(__file__, "rules", table=names.rules)
    parameters = [bigquery.ScalarQueryParameter("etapa", "STRING", stage)]

    try:
        rows = await to_thread(bq.read_rows, names.project, sql, parameters)
    except NotFound:
        logger.info("The catalog of rules does not exist yet: table=%s stage=%s", names.rules, stage)
        return []

    rules = [
        Rule(
            str(row["secretaria"]),
            None if row["categoria_pai"] is None else str(row["categoria_pai"]),
            str(row["nome_funcao"]),
            str(row["nome"]),
            str(row["regra_python"]),
        )
        for row in rows
    ]
    logger.info("Rules read: stage=%s count=%d", stage, len(rules))

    return rules


async def classify_one(bifrost: Bifrost, slots: Semaphore, prompted: Prompted, model: str) -> Row | None:
    """Ask the model about one session and return its row.

    A failure is logged and gives nothing.

    A session that fails stays out of the result and is tried again at the next run,
    inside the window.
    """
    async with slots:
        try:
            answer = await bifrost.answer(prompted.prompt, max_output_tokens=LLM_MAX_OUTPUT_TOKENS)
            parsed = parse_json_object(answer.text)
        except (BifrostError, ValueError) as error:
            logger.warning(
                "A session failed: id=%s error=%s: %s", prompted.session.id_sessao, type(error).__name__, error
            )
            return None

    return build_classified_row(prompted, parsed, model, answer)


@task
async def classify_task(batch: list[Prompted], llm: Llm) -> list[Row]:
    """Ask the model about each session, `max_workers` at once, and return the rows."""
    model = llm.get("model", BIFROST_MODEL)
    slots = Semaphore(llm.get("max_workers", DEFAULT_MAX_WORKERS))

    async with Bifrost.connect(model, LLM_TIMEOUT) as bifrost:
        outcomes = await gather(*(classify_one(bifrost, slots, prompted, model) for prompted in batch))

    rows = [row for row in outcomes if row is not None]
    logger.info("Batch classified: ok=%d failed=%d", len(rows), len(batch) - len(rows))

    return rows


def merge_queries(names: Names) -> tuple[str, str]:
    """Return the MERGE of the tmp table into the destination, and the tmp truncate."""
    columns = [column for column in COLUMNS if column != PRIMARY_KEY]
    merge = render_query(
        __file__,
        "merge_classifications",
        destination=names.destination,
        tmp=names.tmp,
        primary_key=PRIMARY_KEY,
        set_clause=", ".join(f"t.{column} = s.{column}" for column in columns),
        insert_cols=", ".join(COLUMNS),
        insert_vals=", ".join(f"s.{column}" for column in COLUMNS),
    )

    return merge, render_query(__file__, "truncate_table", table=names.tmp)


@task(retries=3, retry_delay_seconds=[30, 60, 120])
async def load_task(names: Names, rows: list[Row]) -> int:
    """Add the rows to the tmp table and MERGE them by `id_sessao`.

    The MERGE writes every column, so a later step that updates only some columns must
    not reuse it. Return the rows that the MERGE touched, or 0 for no row.
    """
    if not rows:
        return 0

    merge, truncate = merge_queries(names)
    touched = await to_thread(
        bq.load_and_merge,
        names.project,
        names.tmp,
        [{key: row.get(key) for key in COLUMNS} for row in rows],
        merge,
        truncate,
    )
    logger.info("Rows loaded: rows=%d merged=%d", len(rows), touched)

    return touched


@task(retries=2, retry_delay_seconds=30)
async def read_relevant_task(names: Names) -> list[Row]:
    """Read the relevant sessions and their summaries, with no model call."""
    sql = render_query(__file__, "relevant_sessions", table=names.destination)
    rows = await to_thread(bq.read_rows, names.project, sql)
    logger.info("Relevant sessions read: count=%d", len(rows))

    return [
        {
            "id_sessao": item["id_sessao"],
            "resumo": item["resumo"],
            "secretaria_principal": item["secretaria_principal"],
            "conteudo_relevante": item["conteudo_relevante"],
            "causa_nome": [*(item["causa_nome"] or [])],
            "tema_nome": [],
        }
        for item in RELEVANT.validate_python(rows)
    ]


@task(retries=3, retry_delay_seconds=[30, 60, 120])
async def update_themes_task(names: Names, rows: list[Row]) -> int:
    """Update only `tema_nome` and `causa_nome` of the sessions in the table.

    The tmp table needs `classificado_em` and `data_particao`, so they go as
    placeholders that the MERGE never reads.
    """
    if not rows:
        return 0

    now = datetime.now(UTC)
    payload = [
        {
            "id_sessao": row["id_sessao"],
            "tema_nome": row["tema_nome"],
            "causa_nome": row["causa_nome"],
            "classificado_em": now.isoformat(),
            "data_particao": now.date().isoformat(),
        }
        for row in rows
    ]
    merge = render_query(__file__, "merge_themes", destination=names.destination, tmp=names.tmp)
    truncate = render_query(__file__, "truncate_table", table=names.tmp)

    return await to_thread(bq.load_and_merge, names.project, names.tmp, payload, merge, truncate)


@task(retries=3, retry_delay_seconds=[30, 60, 120])
async def propagate_task(names: Names) -> int:
    """Copy `tema_nome` and `causa_nome` to the two dbt marts.

    They do not recompute on their own.

    The marts are incremental, so without it they keep the old theme until a full
    refresh. The two columns are fields of the struct `llm`, which BigQuery updates in
    place. Return the rows touched in both marts.
    """
    total = 0

    for chatbot in (names.chatbot_v1, names.chatbot_v2):
        sql = render_query(__file__, "propagate_chatbot", chatbot_table=chatbot, source=names.destination)
        touched = await to_thread(bq.run_merge, names.project, sql)
        logger.info("Theme and cause propagated: table=%s rows=%d", chatbot, touched)
        total += touched

    return total


@task
async def notify_task(message: str, *, errors: bool) -> None:
    """Post a message to the errors or summaries channel, if that channel is set.

    A notice never fails the run.
    """
    notices = NoticeSettings()
    webhook = (notices.DISCORD_ERRORS_WEBHOOK_URL if errors else notices.DISCORD_WEBHOOK_URL).get_secret_value()

    if not webhook:
        logger.warning("The webhook is not set, so the notice is skipped")
        return

    try:
        await notify_discord.fn(message, webhook=webhook)
    except Exception as error:
        logger.warning("The notice to Discord failed: %s", error)
