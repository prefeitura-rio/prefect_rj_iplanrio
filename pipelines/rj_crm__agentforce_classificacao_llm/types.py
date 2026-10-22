"""Types of rj_crm__agentforce_classificacao_llm."""

from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict

from pydantic import JsonValue

type Row = dict[str, JsonValue]


@dataclass(frozen=True)
class Session:
    """A session of the citizen with the agent that waits to be classified, with the HSM that came before it.

    `hsm_variables` are the values that fill the placeholders of `hsm_text`. `hsm_buttons` and
    `late_button` come from the catalog of HSMs: the buttons of the HSM, and whether the only message of
    the citizen is the text of one of them, which means a late answer to a button.
    """

    id_sessao: str
    telefone: str | None
    cpf: str | None
    nome_cidadao: str | None
    sessao_inicio_datahora: datetime | None
    sessao_fim_datahora: datetime | None
    qtd_mensagens_usuario: int
    mensagens_usuario_concatenadas: str | None
    conversa_completa: str | None
    id_disparo_hsm: str | None
    jornada_nome: str | None
    id_jornada: str | None
    hsm_envio_datahora: datetime | None
    hsm_nome: str | None
    hsm_texto: str | None
    hsm_variaveis_json: str | dict[str, JsonValue] | None = None
    hsm_botoes_json: str | None = None
    late_button: bool = False


class ButtonRow(TypedDict):
    """A line of the catalog of HSMs: the HSM of a journey, and its buttons."""

    jornada_nome: str
    atividade_nome: str | None
    hsm_nome: str
    hsm_botoes_json: str | None
    template_pos_versao_indicador: bool | None


@dataclass(frozen=True)
class Prompted:
    """A session with the prompt for the model, which is `com_hsm` or `sem_hsm`."""

    session: Session
    kind: str
    prompt: str


@dataclass(frozen=True)
class Split:
    """The sessions of a run by what happens to each: a prompt, a decision by rule, or nothing."""

    prompted: list[Prompted]
    by_rule: list[Session]
    dropped: int


@dataclass(frozen=True)
class Rule:
    """A rule of the catalog: a Python function that tells whether a summary belongs to a category."""

    secretariat: str
    parent: str | None
    function_name: str
    name: str
    code: str


class Tables(TypedDict, total=False):
    """The tables of the run. The defaults are the ones of production."""

    dataset_id: str
    table_id: str
    tmp_table_id: str
    rules_table_id: str
    source_table: str
    hsm_catalog_table: str


class Window(TypedDict, total=False):
    """How far back a run looks for sessions, and how far back before a session its HSM can be."""

    lookback_days: int
    hsm_max_days_before: int


class Llm(TypedDict, total=False):
    """How a run asks the model: which one, how many questions at one time, and the sessions per load."""

    model: str
    max_workers: int
    load_batch: int


@dataclass(frozen=True)
class Names:
    """The full names, with the project, of the tables that a run touches."""

    project: str
    destination: str
    tmp: str
    rules: str
    source: str
    hsm_catalog: str
    chatbot_v1: str
    chatbot_v2: str


class Relevant(TypedDict):
    """What the rules need of a classified session: its summary, its secretariat, and the cause it already has."""

    id_sessao: str
    resumo: str | None
    secretaria_principal: str | None
    conteudo_relevante: bool | None
    causa_nome: list[str] | None
