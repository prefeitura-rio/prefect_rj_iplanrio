"""Flow for rj_crm__modelo_qualidade_telefone."""

from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__modelo_qualidade_telefone.constants import (
    DATASET_ID,
    DEFAULT_BATCH_SIZE,
    DEFAULT_RECENT_DAYS,
    DEFAULT_TRIALS,
    DRIVE_ROOT_ID,
    EVALUATION_TABLE_ID,
    MODELS_ROOT,
    SCORE_TABLE_ID,
    TIMEZONE,
)
from pipelines.rj_crm__modelo_qualidade_telefone.tasks import (
    decide_task,
    extract_events_task,
    extract_training_task,
    load_champion_task,
    publish_evaluation_task,
    publish_model_task,
    publish_report_task,
    publish_scores_task,
    score_task,
    simulate_task,
    train_task,
)
from pipelines.rj_crm__modelo_qualidade_telefone.types import Judgement, Mode, Release, RetrainOptions, Tables
from utils.hooks import notify_failure

DEFAULT_TABLES = Tables(dataset_id=DATASET_ID, score=SCORE_TABLE_ID, evaluation=EVALUATION_TABLE_ID)


@flow(log_prints=True, flow_run_name="modelo-qualidade-telefone-{mode}", on_failure=[notify_failure])
async def rj_crm__modelo_qualidade_telefone(
    *,
    mode: Mode = "score",
    tables: Tables = DEFAULT_TABLES,
    models_root: str = MODELS_ROOT,
    batch_size: int = DEFAULT_BATCH_SIZE,
    retrain: RetrainOptions | None = None,
) -> None:
    """Estimate the probability that a phone is HighDelivery on WhatsApp (LightGBM), and retrain the model each month.

    `score` runs every three days with the model that is trained. It loads the champion from GCS, scores
    every eligible phone linked to a living CPF (about 6.75 million phones and 9.4 million pairs, see
    `queries/amostra_agora.sql`) in batches of `batch_size` phones, and replaces the table of the day with one atomic
    load job. If the check fails (no row, an invalid probability, a volume far from the last run), the table of
    yesterday stays.

    `retrain` runs each month. It trains a new LightGBM (an Optuna search seeded with the hyperparameters of the
    champion), simulates the choice of a phone on recent sends for the new model, the model in production, a
    heuristic and a random choice, and decides the promotion with the gate (see `helpers/gate.py`). It publishes the
    version to GCS always, and promotes the champion only if the gate passes. It writes the numbers to the
    evaluation table, and a report (CSV files and SHAP plots) to a subfolder of the Drive named by the version.
    The version is the date and time of the training in São Paulo, because a published version is immutable and
    a retry on the same day needs another name.

    `retrain` takes `n_trials` (30), `recent_days` of sends for the simulation (30), `require_gate` (true: with
    false, the model is promoted although it fails the gate, and the reasons stay in the record),
    `auto_promote` (true: with false, the version is published and a person promotes it) and `drive_folder_id`,
    the root folder of the reports, which must be shared (Editor) with the service account.
    """
    if mode == "score":
        champion = await load_champion_task(models_root, required=True)

        if champion is None:
            raise RuntimeError("The champion is required in the score mode")

        path, stats = await score_task(champion, batch_size)
        await publish_scores_task(path, stats, tables)
        logger.info("Scoring published: version=%s rows=%d", champion.version, stats.n_rows)
        return

    options = retrain or {}
    now = datetime.now(ZoneInfo(TIMEZONE))
    version = now.strftime("%Y-%m-%d-%H%M%S")
    champion = await load_champion_task(models_root, required=False)
    frame = await extract_training_task()
    training = await train_task(frame, options.get("n_trials", DEFAULT_TRIALS), champion)
    events = await extract_events_task(options.get("recent_days", DEFAULT_RECENT_DAYS))
    simulation = await simulate_task(events, training, champion)
    gate = await decide_task(training, simulation, require_gate=options.get("require_gate", True))
    release = Release(version, now.date().isoformat())
    judgement = Judgement(simulation, gate)
    await publish_model_task(training, gate, models_root, release, auto_promote=options.get("auto_promote", True))
    await publish_evaluation_task(training, judgement, release, tables)
    await publish_report_task(training, judgement, release, options.get("drive_folder_id", DRIVE_ROOT_ID), frame)
    logger.info("Retrain %s done: promoted=%s reasons=%s", version, gate.promoted, gate.reasons)
