"""Types of rj_smas__cadunico."""

from pathlib import Path
from typing import NamedTuple, TypedDict

type Record = dict[str, str | None]
"""A row of the layout, by column. Every value is text, or nothing."""

type Cells = list[list[str | None]]
"""The cells of a sheet, by row. A cell with no value is nothing."""


class RawFolders(TypedDict):
    """Where the raw files are."""

    bucket: str
    records_prefix: str
    layout_prefix: str


class Tables(TypedDict):
    """The tables of the pipeline, in one dataset."""

    dataset_id: str
    records: str
    layout: str


class Partition(NamedTuple):
    """The Parquet files of one ZIP, ready to load."""

    folder: Path
    date: str
    layout_version: str
    extracted: str


class Field(TypedDict):
    """How a model reads one column of the text line."""

    kind: str
    column: str
    source: str
    alias: str
    type: str
    date_format: str
    divisor: str
    mapped_alias: str
    mapping: list[dict[str, str]]


class ModelVersion(TypedDict):
    """The columns of one layout version of a type of record."""

    version: str
    fields: list[Field]


class SchemaColumn(TypedDict):
    """A column of a dbt schema file."""

    name: str
    description: str


class Model(TypedDict):
    """The model of one type of record."""

    name: str
    number: str
    versions: list[ModelVersion]
    columns: list[SchemaColumn]
