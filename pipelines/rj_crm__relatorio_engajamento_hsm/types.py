"""Types of rj_crm__relatorio_engajamento_hsm."""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal, TypedDict

import numpy as np
import polars as pl

type Source = Literal["form", "dispatches"]
type Color = tuple[float, float, float]
type DriveFile = dict[str, str]


class Message(TypedDict):
    """A message of a session, as the chatbot table holds it."""

    id_sessao_48h: str
    cpf: str | None
    telefone: str | None
    nome_campanha: str | None
    nome_eixo: str | None
    msg_data: datetime | None
    msg_texto: str | None
    msg_fonte: str


@dataclass(frozen=True)
class Tab:
    """The grid id and the title of a tab of a spreadsheet."""

    grid_id: int
    title: str


@dataclass(frozen=True)
class Request:
    """One report request read from the Google Form response sheet."""

    row: int
    timestamp: str
    email: str
    campaign: str
    request_type: str
    start: date
    end: date
    generation_date: date
    status: str = ""
    result_url: str = ""
    released: bool = False


@dataclass(frozen=True)
class Rejection:
    """A row of the response sheet that the run closes with a status, at once.

    It is a row that has stalled or that is invalid. `key` is the timestamp and the
    email, which find the row.
    """

    key: tuple[str, str]
    status: str


@dataclass(frozen=True)
class Pending:
    """The requests to process in this run, and the rows to close with a status."""

    requests: list[Request]
    rejections: list[Rejection]


@dataclass(frozen=True)
class Window:
    """What to extract: a campaign or an HSM, and the days of its dispatches."""

    name: str
    field: Literal["nome_campanha", "nome_hsm"]
    start: date
    end: date


@dataclass(frozen=True)
class Discovery:
    """The categories that the discovery found, with descriptions and examples."""

    catalog: dict[str, str]
    examples: dict[str, list[str]]


@dataclass(frozen=True)
class Job:
    """One report to make, from a Form request or from a dispatch.

    `released` lets it pass the conversation ceiling. `key` finds its row in the
    response sheet, and a dispatch has none.
    """

    campaign: str
    request_type: str
    window: Window
    released: bool = False
    key: tuple[str, str] | None = None


@dataclass(frozen=True)
class Run:
    """What all the requests of a run share: folder, time, model, limits and run id."""

    root_id: str
    now: datetime
    model: str
    limits: Limits
    run_id: str | None = None


@dataclass(frozen=True)
class Outcome:
    """How a request ended: the status for the sheet, a link, and an unexpected error.

    `unexpected` is empty for a request that ended on purpose or with an expected error.
    """

    status: str
    link: str = ""
    unexpected: str = ""


class Limits(TypedDict, total=False):
    """The sizes of a run: ceilings, samples, concurrency, batches and examples.

    `ceiling` is the most conversations that one report sends to the model without a
    release. `judge_ceiling` is the most that the judge looks at. `discovery_cap` is the
    sample of the discovery. `rag_examples` is the reviewed examples in each prompt.
    """

    ceiling: int
    judge_ceiling: int
    concurrency: int
    rag_examples: int
    classification_batch: int
    discovery_batch: int
    judge_batch: int
    discovery_cap: int
    judge_all_below: int
    large_category_pct: float
    examples_per_category: int


@dataclass(frozen=True)
class Dispatch:
    """A dispatch of an HSM that asked for a report: the HSM name and the day."""

    hsm_name: str
    sent_on: date


@dataclass(frozen=True)
class Conversation:
    """The talk of a citizen with the agent in one session, without the HSM text.

    All the conversations share that text.
    """

    session_id: str
    cpf: str | None
    phone: str | None
    campaign: str | None
    axis: str | None
    hsm_text: str
    text: str


@dataclass(frozen=True)
class Extraction:
    """The conversations with a citizen answer.

    Also how many sessions the dispatch had and how many engaged.
    """

    conversations: list[Conversation]
    total_dispatches: int
    engaged: int


@dataclass(frozen=True)
class Classification:
    """A conversation with its category and summary, and the judge verdict."""

    conversation: Conversation
    category: str
    justification: str
    summary: str
    verdict: str = ""
    expected_category: str = ""
    judge_note: str = ""


@dataclass(frozen=True)
class Example:
    """A conversation of a category, with its summary."""

    conversation: str
    summary: str


@dataclass(frozen=True)
class Category:
    """A category of the report: its size, how many were judged and right, examples."""

    name: str
    description: str
    count: int
    pct: float
    judged: int
    correct: int
    examples: list[Example] = field(default_factory=list)


@dataclass(frozen=True)
class Report:
    """What a report holds: the dispatch, classifications, catalog and totals."""

    dispatch: Dispatch
    reference: date
    items: list[Classification]
    catalog: dict[str, str]
    total_dispatches: int
    engaged: int
    window: Window
    notes: tuple[str, ...] = ()
    suggestions: dict[str, str] | None = None
    quick: bool = False
    auto_examples: dict[str, list[str]] = field(default_factory=dict)


@dataclass(frozen=True)
class RoundFiles:
    """What one generation publishes: the folder name, the document and the sheets.

    `auto_catalog` is only for a quick report.
    """

    folder_name: str
    document: bytes
    conversations: pl.DataFrame
    auto_catalog: pl.DataFrame | None = None


@dataclass(frozen=True)
class ExampleStore:
    """The valid reviewed examples of a campaign, with their unit vectors.

    Row `i` of `examples` matches row `i` of `vectors`. An empty store makes the
    classification zero-shot.
    """

    examples: pl.DataFrame
    vectors: np.ndarray


@dataclass(frozen=True)
class Prepared:
    """What a report needs before it starts: the campaign folder and what is in it.

    `outcome` is set when the request ends here, with a skip. `catalog_id` and
    `catalog_link` name the `Catalog` sheet of the campaign, or are empty.
    """

    folder_id: str
    outcome: Outcome | None = None
    catalog_id: str = ""
    catalog_link: str = ""
