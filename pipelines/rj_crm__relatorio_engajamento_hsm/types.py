"""Types of rj_crm__relatorio_engajamento_hsm."""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TypedDict


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


class Limits(TypedDict, total=False):
    """The sizes of a run: the batches for the model, the samples and the examples."""

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
