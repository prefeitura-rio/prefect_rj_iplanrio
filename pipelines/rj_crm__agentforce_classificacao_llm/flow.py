"""Flow for rj_crm__agentforce_classificacao_llm."""

from datetime import UTC, datetime

from prefect import Flow, flow
from prefect.client.schemas.objects import FlowRun, State
from toolkit.telemetry import logger

from pipelines.rj_crm__agentforce_classificacao_llm.constants import (
    DEFAULT_LOAD_BATCH,
    DEFAULT_PROJECT_ID,
    PROMPT_VERSION,
    REASON_STAGE,
    THEME_STAGE,
)
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.labels import build_rule_row, finish_rows
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.names import resolve_names
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.prompts import split_sessions
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.taxonomy import apply_rules, compile_catalog
from pipelines.rj_crm__agentforce_classificacao_llm.tasks import (
    classify_task,
    enrich_task,
    ensure_tables_task,
    extract_task,
    load_task,
    notify_task,
    propagate_task,
    read_relevant_task,
    read_rules_task,
    update_themes_task,
)
from pipelines.rj_crm__agentforce_classificacao_llm.types import Llm, Names, Row, Rule, Tables, Window


async def notify_failure(flow: Flow[..., object], flow_run: FlowRun, state: State[object]) -> None:
    """Post a failed run to the channel of the errors."""
    lines = ["🚨 **agentforce-classificacao-llm: FALHOU**", f"Flow: {flow.name}", f"Flow Run: {flow_run.name}"]
    await notify_task("\n".join([*lines, f"Mensagem: {state.message}"]), errors=True)


async def apply_catalog(names: Names, rows: list[Row], themes: list[Rule], causes: list[Rule]) -> list[Row]:
    """Fill the theme of the rows with the rules of theme, and then the cause, which needs the theme."""
    del names

    return apply_rules(apply_rules(rows, compile_catalog(themes), "tema_nome"), compile_catalog(causes), "causa_nome")


@flow(log_prints=True, on_failure=[notify_failure], flow_run_name="classificacao-llm")
async def rj_crm__agentforce_classificacao_llm(
    *,
    project_id: str = DEFAULT_PROJECT_ID,
    tables: Tables | None = None,
    window: Window | None = None,
    llm: Llm | None = None,
    recalculate_taxonomy: bool = False,
) -> None:
    """Classify the Agentforce sessions that have no classification, with a model, and load them to BigQuery.

    The flow reads the sessions of the last days (7 by default) with the HSM that came
    before each. A late reply to a button is decided by rule, and any other session goes
    to the model through Bifrost, `max_workers` at one time. After each batch of
    `load_batch` sessions (500) the flow fills the theme and the cause with the rules of
    the catalog and loads them with a MERGE by `id_sessao`, so a crash loses at most one
    batch. A session that fails stays out and is tried again at the next run. Discord
    gets a summary.

    With `recalculate_taxonomy` the flow classifies nothing: it applies the current
    rules to the summaries that are already in the table, updates only `tema_nome` and
    `causa_nome`, and copies them to the marts of dbt.
    """
    names = resolve_names(project_id, tables or {})
    themes = await read_rules_task(names, THEME_STAGE)
    causes = await read_rules_task(names, REASON_STAGE)

    if recalculate_taxonomy:
        relevant = await read_relevant_task(names)
        updated = await apply_catalog(names, relevant, themes, causes)
        in_table = await update_themes_task(names, updated)
        in_marts = await propagate_task(names)
        logger.info("Taxonomy recalculated: table=%d marts=%d", in_table, in_marts)
        return

    await ensure_tables_task(names)
    sessions = await extract_task(names, window or {})

    if not sessions:
        logger.info("No pending session")
        await notify_task(summary(0, 0, 0, 0, 0), errors=False)
        return

    split = split_sessions(await enrich_task(names, sessions))
    logger.info("Split: model=%d rule=%d dropped=%d", len(split.prompted), len(split.by_rule), split.dropped)
    loaded = await load_task(
        names,
        await apply_catalog(
            names,
            finish_rows([build_rule_row(session) for session in split.by_rule], PROMPT_VERSION, datetime.now(UTC)),
            themes,
            causes,
        ),
    )
    size = (llm or {}).get("load_batch", DEFAULT_LOAD_BATCH)
    classified = 0

    for start in range(0, len(split.prompted), size):
        rows = await classify_task(split.prompted[start : start + size], llm or {})
        classified += len(rows)
        loaded += await load_task(
            names, await apply_catalog(names, finish_rows(rows, PROMPT_VERSION, datetime.now(UTC)), themes, causes)
        )

    failed = len(split.prompted) - classified
    await notify_task(summary(len(sessions), len(split.by_rule), classified, failed, loaded), errors=False)
    logger.info("Done: classified=%d failed=%d loaded=%d", classified, failed, loaded)


def summary(extracted: int, by_rule: int, classified: int, failed: int, loaded: int) -> str:
    """Return the message of the end of a run."""
    icon = "✅" if failed == 0 else "⚠️"

    return "\n".join(
        [
            f"{icon} **agentforce-classificacao-llm: concluído**",
            f"📥 **Extraídas (pendentes):** {extracted}",
            f"📏 **Pré-classificadas por regra:** {by_rule}",
            f"🤖 **Classificadas por LLM:** {classified}",
            f"❌ **Falhas (retry automático amanhã):** {failed}",
            f"📊 **Linhas carregadas no BQ:** {loaded}",
        ]
    )
