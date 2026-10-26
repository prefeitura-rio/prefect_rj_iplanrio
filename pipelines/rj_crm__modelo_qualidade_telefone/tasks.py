"""Tasks of rj_crm__modelo_qualidade_telefone."""

from asyncio import to_thread
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
from google.cloud.storage import Client as StorageClient
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_crm__modelo_qualidade_telefone.constants import PROJECT_ID, QUERIES_ANCHOR
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.bigquery import (
    append_evaluation,
    connect,
    count_rows,
    read_batches,
    replace_table,
)
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.evaluation import build_evaluation
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.features import features_sql, measure_coverage
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.gate import build_metadata, decide, describe_reasons
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.model_store import (
    load_champion,
    promote_version,
    publish_version,
)
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.report import (
    beeswarm_plot,
    coverage_csv,
    gate_text,
    importance_csv,
    metrics_csv,
    ranking_plot,
    simulation_csv,
)
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.scoring import write_scores
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.simulation import simulate
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.training import train
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.validation import (
    validate_events,
    validate_scores,
    validate_training,
)
from pipelines.rj_crm__modelo_qualidade_telefone.types import (
    Champion,
    Gate,
    Judgement,
    Release,
    ScoreStats,
    Simulation,
    Tables,
    Training,
)
from utils.drive import confirm_root, ensure_folder, upload
from utils.drive import connect as connect_drive
from utils.tasks import read_bq
from utils.utils import load_credentials, render_query


@task
async def load_champion_task(models_root: str, *, required: bool) -> Champion | None:
    """Load the champion from GCS.

    With `required`, raise `RuntimeError` if there is none. The daily scoring cannot run
    with no model, and the first retrain runs with none.
    """
    client = StorageClient(credentials=load_credentials(), project=PROJECT_ID)
    champion = await to_thread(load_champion, models_root, client)

    if champion is None and required:
        raise RuntimeError(f"No champion in {models_root}: publish a model before the first scoring")

    logger.info("Champion: version=%s", champion.version if champion else None)

    return champion


@task
async def score_task(champion: Champion, batch_size: int) -> tuple[Path, ScoreStats]:
    """Read the phone features, score them with the champion and write Parquet.

    The file is in a temporary folder of the run, and `publish_scores_task` loads it.
    """
    folder = Path(TemporaryDirectory(prefix="telefone_scores_", delete=False).name)
    path = folder / "scores.parquet"

    def run() -> ScoreStats:
        """Score the batches of the query."""
        return write_scores(read_batches(features_sql("now"), batch_size), champion.booster, champion.version, path)

    stats = await to_thread(run)
    logger.info("Scoring done: version=%s stats=%s", champion.version, stats)

    return path, stats


@task
async def publish_scores_task(path: Path, stats: ScoreStats, tables: Tables) -> None:
    """Check the scoring and replace the table of the day, in one atomic load job.

    If the check fails, the table of yesterday stays, and the task raises `ValueError`.
    """
    client = await to_thread(connect)
    previous = await to_thread(count_rows, client, tables["dataset_id"], tables["score"])
    validate_scores(stats, previous)
    await to_thread(replace_table, client, path, tables["dataset_id"], tables["score"])
    path.unlink()


@task
async def extract_training_task() -> pl.DataFrame:
    """Run the query of the training and check the result.

    Return one row for each phone: `telefone`, `data_corte`, `high_delivery` and the
    FEATURES.
    """
    frame = await read_bq.fn(features_sql("training"), project=PROJECT_ID)
    validate_training(frame)
    logger.info(
        "Training extracted: rows=%d high_delivery=%.1f%%",
        frame.height,
        100 * float(frame["high_delivery"].to_numpy().mean()),
    )
    logger.info("Coverage of the features (training):\n%s", measure_coverage(frame))

    return frame


@task
async def train_task(frame: pl.DataFrame, n_trials: int, champion: Champion | None) -> Training:
    """Search the hyperparameters, measure the held-out set and fit the final model."""
    return await to_thread(train, frame, n_trials, champion.metadata.get("hiperparametros") if champion else None)  # pyright: ignore[reportArgumentType]


@task
async def extract_events_task(recent_days: int) -> pl.DataFrame:
    """Run the query of the candidate events and check the result.

    Return one row for each pair of a send and a candidate phone.
    """
    sql = render_query(QUERIES_ANCHOR, "eventos_candidatos", janela_recente_dias=recent_days)
    frame = await read_bq.fn(sql, project=PROJECT_ID)
    validate_events(frame)
    logger.info("Events extracted: rows=%d sends=%d", frame.height, frame["id_interacao"].n_unique())

    return frame


@task
async def simulate_task(events: pl.DataFrame, training: Training, champion: Champion | None) -> Simulation:
    """Run the simulation: clean the pool, rank the algorithms and test the gaps."""
    simulation = await to_thread(
        simulate, events, training.phones, training.booster, champion.booster if champion else None
    )
    logger.info("Simulation done: events=%d dropped=%.1f%%", simulation.n_events, 100 * simulation.dropped_fraction)

    return simulation


@task
async def decide_task(training: Training, simulation: Simulation, *, require_gate: bool) -> Gate:
    """Apply the gate and return the decision."""
    return decide(training, simulation, require_gate=require_gate)


@task
async def publish_model_task(
    training: Training, gate: Gate, models_root: str, release: Release, *, auto_promote: bool
) -> None:
    """Publish the version in GCS, and make it the champion if the gate passes.

    It promotes only if `auto_promote` is on.
    """
    client = StorageClient(credentials=load_credentials(), project=PROJECT_ID)
    metadata = build_metadata(training, gate)
    version = release.version
    await to_thread(publish_version, models_root, version, training.booster, metadata, client)

    if gate.promoted and auto_promote:
        await to_thread(promote_version, models_root, version, client)
    elif gate.promoted:
        logger.info("Version %s passed the gate, but auto_promote is off: published, not promoted", version)
    else:
        logger.warning("Version %s not promoted: %s", version, describe_reasons(gate.reasons))


@task
async def publish_evaluation_task(training: Training, judgement: Judgement, release: Release, tables: Tables) -> None:
    """Delete the rows of this version from the evaluation table, then append."""
    rows = build_evaluation(training, judgement.simulation, judgement.gate, release.version, release.date)
    client = await to_thread(connect)
    await to_thread(append_evaluation, client, rows, tables["dataset_id"], tables["evaluation"], QUERIES_ANCHOR)


@task
async def publish_report_task(
    training: Training, judgement: Judgement, release: Release, root_id: str, frame: pl.DataFrame
) -> None:
    """Upload the retrain report to the `version` subfolder of the Drive root.

    Raise `RuntimeError` if the root folder does not exist or is not shared with the
    service account.
    """
    version = release.version
    simulation = judgement.simulation
    gate = judgement.gate
    drive = await to_thread(connect_drive)
    await to_thread(confirm_root, drive, root_id)
    folder_id = await to_thread(ensure_folder, drive, root_id, version)
    files = {
        "metricas_treino.csv": (metrics_csv(training), "text/csv"),
        "importancia_shap.csv": (importance_csv(training), "text/csv"),
        "shap_ranking.png": (await to_thread(ranking_plot, training, version), "image/png"),
        "shap_beeswarm.png": (await to_thread(beeswarm_plot, training, version), "image/png"),
        "simulacao.csv": (simulation_csv(simulation), "text/csv"),
        "gate.txt": (gate_text(gate), "text/plain"),
        "cobertura.csv": (coverage_csv(frame), "text/csv"),
    }

    for name, (content, mime_type) in files.items():
        await to_thread(upload, drive, folder_id, name, content, mime_type)

    logger.info("Report published: version=%s folder=%s", version, folder_id)
