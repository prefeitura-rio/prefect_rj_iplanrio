"""The frame of the people to message: columns, filters, retries, the file and the log."""

from collections.abc import Collection, Sequence
from datetime import date, datetime
from json import dumps
from typing import cast

import polars as pl
from pydantic import BaseModel, JsonValue, TypeAdapter, ValidationError, field_validator
from toolkit.telemetry import logger

from pipelines.rj_crm__disparo_template.constants import CPF_ALIASES, REQUIRED_COLUMNS, VALIDATION_SAMPLE
from pipelines.rj_crm__disparo_template.helpers.clock import today
from pipelines.rj_crm__disparo_template.types import Audience, Csv

FIXED_COLUMNS = frozenset({"dispatch_date", "campaign_name", "SubscriberKey", "telefone"})
ROWS: TypeAdapter[list[dict[str, JsonValue]]] = TypeAdapter(list[dict[str, JsonValue]])


class LogLine(BaseModel):
    """The columns that every line of the log needs: the dispatch, the campaign, the CPF and the phone."""

    dispatch_date: str
    campaign_name: str
    cpf: str
    telefone: str

    @field_validator("campaign_name", "cpf", "telefone")
    @classmethod
    def strip_text(cls, value: str) -> str:
        """Drop the spaces around the text, and refuse a text that is empty."""
        stripped = value.strip()

        if not stripped:
            raise ValueError("The value cannot be empty")

        return stripped


def normalize_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Add `LOCALE` as `BR` if the frame lacks it, and name the CPF column `cpf` if it has an old name."""
    if "LOCALE" not in frame.columns:
        frame = frame.with_columns(pl.lit("BR").alias("LOCALE"))

    for alias in CPF_ALIASES:
        if alias in frame.columns and "cpf" not in frame.columns:
            logger.info("Column renamed: %s to cpf", alias)
            frame = frame.rename({alias: "cpf"})

    return frame


def validate_frame(frame: pl.DataFrame, campaign: str) -> None:
    """Check the columns that the log needs, and the first rows of the frame.

    Raise `ValueError` if `cpf` or `telefone` is missing, if the campaign has no name,
    or if one of the first rows has an empty value in them.
    """
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]

    if missing:
        raise ValueError(f"Columns missing for the log: {missing}. The query must give 'cpf' and 'telefone'")

    if not campaign.strip():
        raise ValueError("The campaign name cannot be empty")

    problems: list[str] = []

    for number, row in enumerate(frame.head(VALIDATION_SAMPLE).iter_rows(named=True)):
        try:
            LogLine(
                dispatch_date="2000-01-01",
                campaign_name=campaign,
                cpf=str(row["cpf"] or ""),
                telefone=str(row["telefone"] or ""),
            )
        except ValidationError as error:
            problems += [f"row {number}: {item['loc'][0]} - {item['msg']}" for item in error.errors()]

    if problems:
        raise ValueError("Invalid data in the columns of the log:\n" + "\n".join(problems))


def drop_duplicates(frame: pl.DataFrame, column: str, *, enabled: bool) -> pl.DataFrame:
    """Keep the first row of each value of the column, when `enabled`."""
    if not enabled or column not in frame.columns:
        return frame

    unique = frame.unique(subset=[column], keep="first", maintain_order=True)
    logger.info("Duplicates dropped: column=%s dropped=%d kept=%d", column, frame.height - unique.height, unique.height)

    return unique


def quarantine(frame: pl.DataFrame, failed_phones: Collection[str], *, retries: bool) -> pl.DataFrame:
    """Handle the phones that failed in the last dispatch.

    With `retries`, their phone goes empty, so the retry takes the first of `others`.
    Without them, the row is dropped.
    """
    if not failed_phones:
        return frame

    members = pl.Series(sorted(failed_phones), dtype=pl.String)
    failed = pl.col("telefone").cast(pl.String).is_in(members.implode())

    if retries:
        return frame.with_columns(pl.when(failed).then(None).otherwise(pl.col("telefone")).alias("telefone"))

    return frame.filter(~failed)


def fill_phones(frame: pl.DataFrame) -> pl.DataFrame:
    """Give a row with no phone the first of `others`, and drop the rows that still have none."""
    if "others" in frame.columns:
        first = pl.col("others").list.get(0, null_on_oob=True)
        frame = frame.with_columns(pl.coalesce("telefone", first).alias("telefone"))

    return frame.drop_nulls("telefone")


def has_alternatives(frame: pl.DataFrame, attempt: int) -> bool:
    """Tell whether any row has at least `attempt` alternative phones, or whether the frame has no `others` column."""
    if "others" not in frame.columns:
        return True

    return frame.select((pl.col("others").list.len() >= attempt).any()).to_series().to_list() == [True]


def retry_frame(frame: pl.DataFrame, failed_cpfs: Collection[str], attempt: int) -> pl.DataFrame:
    """Return the rows of the CPFs that failed, with the phone number `attempt` of `others`, and drop those with none.

    Raise `ValueError` if the frame has no `others` column.
    """
    if "others" not in frame.columns:
        raise ValueError("The retry needs the column 'others'")

    members = pl.Series(sorted(failed_cpfs), dtype=pl.String)
    chosen = frame.filter(pl.col("cpf").cast(pl.String).is_in(members.implode()))

    return chosen.with_columns(pl.col("others").list.get(attempt - 1, null_on_oob=True).alias("telefone")).drop_nulls(
        "telefone"
    )


def of_campaign(dispatched: pl.DataFrame, campaign: str) -> pl.DataFrame:
    """Return the dispatches of a campaign."""
    return dispatched.filter(pl.col("nome_campanha") == campaign)


def of_day(dispatched: pl.DataFrame, day: date) -> pl.DataFrame:
    """Return the dispatches of a day, by `data_particao`."""
    return dispatched.filter(pl.col("data_particao").cast(pl.String).str.slice(0, 10).str.to_date() == day)


def drop_dispatched(frame: pl.DataFrame, dispatched: pl.DataFrame, column: str) -> pl.DataFrame:
    """Drop the rows whose CPF or phone is in the dispatches.

    `column` is `cpf`, `telefone` or `SubscriberKey`.

    A frame or a dispatch table that lacks the column is left as it is, with a warning.
    """
    key = "cpf" if column == "SubscriberKey" else column

    if frame.is_empty() or dispatched.is_empty():
        return frame

    if key not in frame.columns or key not in dispatched.columns:
        logger.warning("The filter of the dispatched is skipped, a column is missing: column=%s", key)

        return frame

    members = dispatched.get_column(key).drop_nulls().cast(pl.String)
    kept = frame.filter(~pl.col(key).cast(pl.String).is_in(members.implode()))
    logger.info(
        "Already dispatched dropped: column=%s dropped=%d kept=%d", key, frame.height - kept.height, kept.height
    )

    return kept


def split_frame(frame: pl.DataFrame, size: int | None) -> list[pl.DataFrame]:
    """Cut the frame in batches of `size` rows, or keep it whole with no size."""
    if not size:
        return [frame]

    return [frame.slice(start, size) for start in range(0, frame.height, size)]


def nested_to_text(frame: pl.DataFrame) -> pl.DataFrame:
    """Write the columns that hold lists or structs as JSON text."""
    nested = [name for name, dtype in frame.schema.items() if isinstance(dtype, pl.List | pl.Struct | pl.Array)]
    texts = [
        pl.Series(
            name,
            [dump_value(value) for value in cast("list[object]", frame.get_column(name).to_list())],
            dtype=pl.String,
        )
        for name in nested
    ]

    return frame.with_columns(texts)


def dump_value(value: object) -> str | None:
    """Return a value as JSON text, or nothing for a null."""
    return None if value is None else dumps(value, ensure_ascii=False, default=str)


def build_csv(frame: pl.DataFrame, stem: str, columns: Sequence[str] | None, separator: str, now: datetime) -> Csv:
    """Return the file of a batch for the SFTP.

    With `columns`, the file has `telefone`, `SubscriberKey` (the CPF) and those
    columns, and the others are dropped, including `others`. Without them, only `others`
    goes away. `LOCALE` is `BR`, and `NOME_ARQUIVO`, if asked for, is the name of the
    file. The name is `<stem>_<time>.csv`.
    """
    named = frame.rename({"cpf": "SubscriberKey"}) if "cpf" in frame.columns else frame
    name = f"{stem}_{now:%Y%m%d%H%M%S}.csv"

    if columns is not None:
        keep = [column for column in dict.fromkeys(["telefone", "SubscriberKey", *columns]) if column in named.columns]
        named = named.select(keep)
    else:
        named = named.drop("others", strict=False)

    named = named.with_columns(pl.lit("BR").alias("LOCALE"))

    if columns is not None and "NOME_ARQUIVO" in columns:
        named = named.with_columns(pl.lit(name).alias("NOME_ARQUIVO"))

    text = nested_to_text(named).write_csv(separator=separator)

    return Csv(name=name, text=text, dispatch_date=f"{now:%Y-%m-%d %H:%M:%S}")


def build_log(frame: pl.DataFrame, dispatch_date: str, campaign: str) -> list[dict[str, JsonValue]]:
    """Return the rows of the log of a batch, with the partition columns.

    Each row has the dispatch time, the campaign, the CPF as `SubscriberKey`, the phone
    and `data`, the JSON of the other columns. A row whose CPF or phone is empty is left
    out. Raise `ValueError` if no row is left.
    """
    named = frame.rename({"cpf": "SubscriberKey"}) if "cpf" in frame.columns else frame
    extra = [column for column in named.columns if column not in FIXED_COLUMNS]
    day = dispatch_date[:10]
    rows: list[dict[str, JsonValue]] = []
    discarded = 0

    for number, row in enumerate(named.iter_rows(named=True)):
        try:
            line = LogLine(
                dispatch_date=dispatch_date,
                campaign_name=campaign,
                cpf=str(row.get("SubscriberKey") or ""),
                telefone=str(row.get("telefone") or ""),
            )
        except ValidationError:
            discarded += 1
            logger.info("Row left out of the log: number=%d", number)

            continue

        rows.append(
            {
                "dispatch_date": line.dispatch_date,
                "campaign_name": line.campaign_name,
                "SubscriberKey": line.cpf,
                "telefone": line.telefone,
                "data": dumps({column: row[column] for column in extra}, ensure_ascii=False, default=str),
                "ano_particao": day[:4],
                "mes_particao": day[5:7],
                "data_particao": day,
            }
        )

    if not rows:
        raise ValueError(f"No valid row for the log: {discarded} rows left out")

    return ROWS.validate_python(rows)


def apply_filters(
    frame: pl.DataFrame, dispatched: pl.DataFrame, audience: Audience, campaign: str, attempt: int
) -> pl.DataFrame:
    """Drop who must not get the message of the try `attempt`, by what was already sent.

    First, who got this campaign in the last `interval_days` days: by CPF in the first
    try, and by phone in a retry, so the same person can try another phone. Then who got
    any campaign today: by the column of `audience.dispatched`, except that a retry
    never filters by CPF, because its phone may have got another campaign.
    """
    if dispatched.is_empty():
        return frame

    frame = drop_dispatched(frame, of_campaign(dispatched, campaign), "cpf" if attempt == 0 else "telefone")
    mode = None if attempt > 0 and audience.dispatched == "cpf" else audience.dispatched

    if mode:
        frame = drop_dispatched(frame, of_day(dispatched, today()), mode)

    return frame
