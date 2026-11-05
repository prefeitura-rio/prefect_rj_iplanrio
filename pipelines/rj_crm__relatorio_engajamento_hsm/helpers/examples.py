"""Pure selection and similarity rules for reviewed examples."""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime

import numpy as np
import polars as pl

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    CATEGORY_SEPARATOR,
    CONV_CATEGORY,
    CONV_CONVERSATION,
    CONV_SESSION,
    CORRECT_CATEGORIES,
    EXAMPLES_SCHEMA,
    EXAMPLES_SHEET_COLUMNS,
    REVIEW,
    REVIEW_ALIASES,
    REVIEW_MARK_DATE,
    REVIEW_NOTE,
    REVIEW_RIGHT,
    SHEET_ROW,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import ValidCatalog
from pipelines.rj_crm__relatorio_engajamento_hsm.types import DriveFile, ExampleStore

REVIEW_DATE = re.compile(REVIEW_MARK_DATE)


@dataclass(frozen=True)
class SyncPlan:
    """Drive files to read, remove, or keep."""

    read: list[str]
    remove: list[str]
    unchanged: int


def plan_sync(drive: pl.DataFrame, stored: pl.DataFrame, model: str) -> SyncPlan:
    """Compare Drive files with the stored file state."""
    known = {row["file_id"]: (row["modified_at"], row["model"]) for row in stored.iter_rows(named=True)}
    current = {row["file_id"] for row in drive.iter_rows(named=True)}
    read = [
        row["file_id"]
        for row in drive.iter_rows(named=True)
        if known.get(row["file_id"]) != (row["modified_at"], model)
    ]
    return SyncPlan(read, sorted(set(known) - current), drive.height - len(read))


def review_date(file_name: str, modified_at: str) -> date | None:
    """Return the date of a review: the one in the file name, or the Drive time.

    A name date that does not exist falls back to the Drive time.
    """
    found = REVIEW_DATE.search(file_name)

    if found:
        try:
            return date.fromisoformat(found.group(1))
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(modified_at.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def parse_reviews(frame: pl.DataFrame, catalog: ValidCatalog) -> tuple[pl.DataFrame, list[str]]:
    """Turn the reviewed rows of one sheet into examples.

    Return the examples and the reason of each discarded row, for the log. A row with no
    review is not reviewed yet, so it is ignored with no reason. A category that is not
    active in the catalog discards the whole row.
    """
    required = (CONV_SESSION, CONV_CATEGORY, CONV_CONVERSATION, REVIEW, CORRECT_CATEGORIES)
    missing = [column for column in required if column not in frame.columns]
    rows: list[dict[str, object]] = []
    discarded: list[str] = []

    if missing:
        return pl.DataFrame(schema=EXAMPLES_SHEET_COLUMNS), [f"the sheet has no column {missing}, so it is ignored"]

    for row in frame.iter_rows(named=True):
        label = f"row {row.get(SHEET_ROW, '?')}"
        raw = str(row[REVIEW]).strip()

        if not raw:
            continue

        review = REVIEW_ALIASES.get(raw.casefold())

        if review is None:
            discarded.append(f"{label}: review {raw!r} is not Right or Wrong")
            continue

        session_id = str(row[CONV_SESSION]).strip()
        conversation = str(row[CONV_CONVERSATION]).strip()

        if not session_id or not conversation:
            discarded.append(f"{label}: no session id or no conversation")
            continue

        column = CONV_CATEGORY if review == REVIEW_RIGHT else CORRECT_CATEGORIES
        names = [name.strip() for name in str(row[column]).split(CATEGORY_SEPARATOR) if name.strip()]

        if not names:
            discarded.append(f"{label}: {review} with no category in {column!r}")
            continue

        found = [catalog.active.find(name) for name in names]
        inactive = [name for name, canonical in zip(names, found, strict=True) if canonical is None]

        if inactive:
            discarded.append(f"{label}: category {inactive} not active in the catalog")
            continue

        rows.append(
            {
                "session_id": session_id,
                "conversation": conversation,
                "categories": list(dict.fromkeys(name for name in found if name)),
                "review": review,
                "review_note": str(row.get(REVIEW_NOTE, "")).strip(),
            }
        )

    return pl.DataFrame(rows, schema=EXAMPLES_SHEET_COLUMNS), discarded


def select_examples(frame: pl.DataFrame, catalog: ValidCatalog) -> pl.DataFrame:
    """Return the examples that count today.

    Every category must be active in the current catalog, and it comes back under the
    name of that catalog. A conversation that was reviewed more than once keeps only its
    latest review.
    """
    if frame.is_empty():
        return frame

    renamed = [[catalog.active.find(name) for name in names] for names in frame["categories"]]
    valid = [bool(names) and all(name is not None for name in names) for names in renamed]

    return (
        frame.with_columns(pl.Series("categories", renamed, dtype=frame.schema["categories"]))
        .filter(pl.Series(valid, dtype=pl.Boolean))
        .sort(["revision_date", "ingested_at"], descending=True, nulls_last=True)
        .unique("session_id", keep="first", maintain_order=True)
    )


def top_k(query: np.ndarray, examples: np.ndarray, count: int) -> tuple[np.ndarray, np.ndarray]:
    """Return the indices and cosine scores of the nearest unit vectors."""
    count = min(count, len(examples))
    if count == 0 or len(query) == 0:
        empty = np.empty((len(query), 0))
        return empty.astype(int), empty
    scores = query @ examples.T
    candidates = np.argpartition(-scores, count - 1, axis=1)[:, :count]
    values = np.take_along_axis(scores, candidates, axis=1)
    order = np.argsort(-values, axis=1)
    return np.take_along_axis(candidates, order, axis=1), np.take_along_axis(values, order, axis=1)


def drive_state(files: Sequence[DriveFile]) -> pl.DataFrame:
    """Return the id and the Drive time of the reviewed sheets for the plan."""
    return pl.DataFrame(
        {
            "file_id": [file["id"] for file in files],
            "modified_at": [file.get("modifiedTime", "") for file in files],
        },
        schema={"file_id": pl.String, "modified_at": pl.String},
    )


def campaign_state(table: pl.DataFrame, campaign: str) -> pl.DataFrame:
    """Return the files, Drive times and models that the table has for a campaign."""
    return (
        table.filter(pl.col("campaign") == campaign)
        .select("file_id", "modified_at", "model")
        .unique(maintain_order=True)
    )


def fresh_rows(
    examples: pl.DataFrame,
    campaign: str,
    file: DriveFile,
    vectors: Sequence[Sequence[float]],
    stamp: tuple[str, datetime],
) -> pl.DataFrame:
    """Return the table rows of the examples of one reviewed file, with their vectors.

    `stamp` is the embedding model and the time of the ingestion.

    Each row keeps the Drive file, its time and the date of the review, so a later
    run reads the file again only when it changed.
    """
    model, ingested_at = stamp
    modified_at = file.get("modifiedTime", "")
    reviewed = review_date(file["name"], modified_at)

    return examples.with_columns(
        pl.lit(campaign).alias("campaign"),
        pl.lit(file["id"]).alias("file_id"),
        pl.lit(file["name"]).alias("file_name"),
        pl.lit(modified_at).alias("modified_at"),
        pl.lit(reviewed, dtype=pl.Date).alias("revision_date"),
        pl.Series("embedding", [list(map(float, vector)) for vector in vectors], dtype=pl.List(pl.Float64)),
        pl.lit(model).alias("model"),
        pl.lit(ingested_at, dtype=pl.Datetime("us", "UTC")).alias("ingested_at"),
    ).select(list(EXAMPLES_SCHEMA))


def merge_store(table: pl.DataFrame, plan: SyncPlan, campaign: str, fresh: pl.DataFrame) -> pl.DataFrame:
    """Return the whole table as it must be after a sync of one campaign.

    Other campaigns and the unchanged files stay. The files that were read again or
    removed leave, and the fresh rows enter.
    """
    gone = [*plan.read, *plan.remove]
    kept = table.filter((pl.col("campaign") != campaign) | ~pl.col("file_id").is_in(gone))

    return pl.concat([kept, fresh.select(list(EXAMPLES_SCHEMA))])


def load_store(table: pl.DataFrame, campaign: str, model: str, catalog: ValidCatalog) -> ExampleStore:
    """Return the valid examples of a campaign for the model, with their vectors.

    A file that another model embedded is out, because vectors of two models do not
    compare.
    """
    mine = table.filter((pl.col("campaign") == campaign) & (pl.col("model") == model))
    valid = select_examples(mine, catalog)

    if valid.is_empty():
        return ExampleStore(valid.drop("embedding"), np.empty((0, 0)))

    vectors = np.array(valid["embedding"].to_list(), dtype=np.float32)

    return ExampleStore(valid.drop("embedding"), vectors)


def retrieve(store: ExampleStore, queries: np.ndarray, count: int) -> list[pl.DataFrame]:
    """Return, for each query vector, its `count` nearest examples, nearest first."""
    if store.examples.is_empty() or count <= 0:
        return [store.examples.clear() for _ in range(len(queries))]

    indices, _ = top_k(queries, store.vectors, count)

    return [store.examples[row.tolist()] for row in indices]
