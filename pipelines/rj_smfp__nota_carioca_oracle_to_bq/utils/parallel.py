"""Lógica pura da carga paralela: parâmetros dos filhos, decisão sobre estados, exclusão mútua e prova de validação.

O pai lança um flow run filho por tabela. O filho carrega e valida a tabela temporária e a marca com
labels que provam a validação; o pai só publica depois de conferir essas marcas.
"""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum, auto

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import (
    CHILD_TAG,
    LABEL_ROWS,
    LABEL_RUN_ID,
    LABEL_SCN,
    LABEL_VALUE_MAX_LENGTH,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import Snapshot

STATE_COMPLETED = "COMPLETED"
FAILED_STATES = frozenset({"FAILED", "CRASHED", "CANCELLED", "CANCELLING"})
TERMINAL_STATES = frozenset({"COMPLETED", "FAILED", "CRASHED", "CANCELLED"})
ACTIVE_STATES = frozenset({"RUNNING", "PENDING"})
LABEL_UNSAFE = re.compile(r"[^a-z0-9_-]")


class ParallelRunError(RuntimeError):
    """A carga paralela não pode seguir: filho com falha, execução concorrente ou prova de validação inválida."""


class Verdict(StrEnum):
    """Decisão do pai sobre o conjunto de filhos."""

    RUNNING = auto()
    ALL_COMPLETED = auto()
    FAILED = auto()


@dataclass(frozen=True)
class RunInfo:
    """Visão mínima de um flow run, lida do Prefect.

    :param run_id: Id do flow run.
    :param name: Nome do flow run.
    :param state_type: Tipo do estado (``RUNNING``, ``COMPLETED``...), em maiúsculas.
    :param state_name: Nome do estado, como aparece na UI.
    :param tags: Tags do flow run.
    :param parameters: Parâmetros do flow run.
    :param elapsed_seconds: Tempo desde o início da execução; ``None`` se ainda não começou.
    """

    run_id: str
    name: str
    state_type: str
    state_name: str
    tags: tuple[str, ...]
    parameters: Mapping[str, object]
    elapsed_seconds: float | None = None

    @property
    def is_child(self) -> bool:
        """Indica se é uma execução de uma só tabela (filho), lançada pelo pai ou à mão."""
        return CHILD_TAG in self.tags or self.parameters.get("table_id") is not None


@dataclass(frozen=True)
class ChildContext:
    """Dados que o pai entrega ao filho para ele reconstruir a execução sem tirar nova foto.

    :param table_id: Tabela do filho.
    :param snapshot: Foto (SCN e horário) tirada pelo pai.
    :param parent_run_id: Flow run do pai; isola GCS e tarefa de chunking e é a prova gravada na tabela.
    """

    table_id: str
    snapshot: Snapshot
    parent_run_id: str


@dataclass(frozen=True)
class TableRunContext:
    """O que extração, load e validação de uma tabela precisam saber, igual no modo sequencial e no filho.

    :param infisical_secret_path: Pasta do segredo do Oracle.
    :param project: Projeto do BigQuery e do GCS.
    :param dataset_id: Dataset de destino.
    :param bucket: Bucket dos Parquet.
    :param run_id: Flow run que isola os arquivos (o pai, no modo paralelo).
    :param snapshot: Foto consistente de leitura.
    :param options: Parâmetros de desempenho da extração.
    """

    infisical_secret_path: str
    project: str
    dataset_id: str
    bucket: str
    run_id: str
    snapshot: Snapshot
    options: ExtractOptions


@dataclass(frozen=True)
class TableProof:
    """Marca gravada pelo filho na tabela temporária validada.

    :param labels: Labels da tabela.
    :param num_rows: Linhas da tabela segundo os metadados do BigQuery.
    """

    labels: Mapping[str, str]
    num_rows: int


def parse_child_context(
    table_id: str | None, scn: int | None, taken_at: str | None, parent_run_id: str | None
) -> ChildContext | None:
    """Decide o modo do flow: filho se ``table_id`` vier, pai se nenhum parâmetro de filho vier.

    :param table_id: Tabela do filho, ou ``None`` no pai.
    :param scn: SCN da foto do pai.
    :param taken_at: Horário da foto, em ISO 8601.
    :param parent_run_id: Flow run do pai.
    :returns: O contexto do filho, ou ``None`` no modo pai.
    :raises ValueError: Se faltar algum parâmetro do filho, ou se vierem parâmetros de filho sem ``table_id``.
    """
    if table_id is None:
        if any(value is not None for value in (scn, taken_at, parent_run_id)):
            raise ValueError("scn, snapshot_taken_at e parent_run_id só fazem sentido com table_id (modo filho).")
        return None
    if scn is None or taken_at is None or parent_run_id is None:
        raise ValueError(f"Modo filho ({table_id}) exige scn, snapshot_taken_at e parent_run_id.")
    parsed = datetime.fromisoformat(taken_at)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return ChildContext(table_id=table_id, snapshot=Snapshot(scn=scn, taken_at=parsed), parent_run_id=parent_run_id)


def build_child_parameters(
    table_id: str, snapshot: Snapshot, parent_run_id: str, passthrough: Mapping[str, object]
) -> dict[str, object]:
    """Monta os parâmetros do flow run filho de uma tabela.

    :param table_id: Tabela do filho.
    :param snapshot: Foto tirada pelo pai.
    :param parent_run_id: Flow run do pai.
    :param passthrough: Parâmetros repassados sem mudança (dataset, projeto, bucket, opções de extração...).
    :returns: Parâmetros JSON-serializáveis do flow.
    """
    return {
        **passthrough,
        "table_id": table_id,
        "scn": snapshot.scn,
        "snapshot_taken_at": snapshot.taken_at.isoformat(),
        "parent_run_id": parent_run_id,
    }


def decide(children: Sequence[RunInfo]) -> Verdict:
    """Resume o estado dos filhos numa decisão.

    :param children: Estado atual de cada filho.
    :returns: ``FAILED`` se algum filho terminou sem sucesso (ou está sendo cancelado), ``ALL_COMPLETED`` se todos
        concluíram, senão ``RUNNING``.
    """
    if any(child.state_type in FAILED_STATES for child in children):
        return Verdict.FAILED
    if all(child.state_type == STATE_COMPLETED for child in children):
        return Verdict.ALL_COMPLETED
    return Verdict.RUNNING


def runs_to_cancel(children: Sequence[RunInfo]) -> list[RunInfo]:
    """Seleciona os filhos que ainda não terminaram e nem estão sendo cancelados.

    :param children: Estado atual de cada filho.
    :returns: Filhos a cancelar.
    """
    return [child for child in children if child.state_type not in TERMINAL_STATES | {"CANCELLING"}]


def all_terminal(children: Sequence[RunInfo]) -> bool:
    """Indica se todos os filhos já terminaram.

    :param children: Estado atual de cada filho.
    :returns: ``True`` se nenhum filho está ativo.
    """
    return all(child.state_type in TERMINAL_STATES for child in children)


def format_child_status(table_id: str, child: RunInfo) -> str:
    """Monta a linha de log de um filho.

    :param table_id: Tabela do filho.
    :param child: Estado do filho.
    :returns: Linha com tabela, flow run, estado e tempo.
    """
    elapsed = "não iniciado" if child.elapsed_seconds is None else f"{int(child.elapsed_seconds)}s"
    return f"{table_id}: filho '{child.name}' ({child.run_id}) {child.state_name}, {elapsed}"


def find_conflicts(runs: Sequence[RunInfo], own_run_id: str) -> list[RunInfo]:
    """Seleciona execuções ativas do mesmo deployment que impedem esta de começar.

    Pais vêm primeiro. Filhos ativos entram porque, no início do pai, nenhum filho dele existe ainda: são de
    outro pai que morreu ou de uma execução manual de uma tabela, e disputariam as mesmas tabelas temporárias.

    :param runs: Execuções do deployment.
    :param own_run_id: Flow run atual, ignorado.
    :returns: Execuções em ``RUNNING`` ou ``PENDING``, pais antes de filhos.
    """
    active = [run for run in runs if run.run_id != own_run_id and run.state_type in ACTIVE_STATES]
    return sorted(active, key=lambda run: run.is_child)


def describe_conflicts(conflicts: Sequence[RunInfo]) -> str:
    """Monta a mensagem de recusa que nomeia as execuções em conflito.

    :param conflicts: Resultado de :func:`find_conflicts`, não vazio.
    :returns: Mensagem com nome, id e estado de cada uma e como proceder.
    """
    listed = "; ".join(
        f"{'filho' if run.is_child else 'pai'} '{run.name}' ({run.run_id}, {run.state_name})" for run in conflicts
    )
    return (
        f"Já existe carga em andamento neste deployment: {listed}. Duas cargas disputariam as mesmas tabelas "
        "temporárias. Espere terminar ou, se for uma execução travada, cancele-a no Prefect e rode de novo."
    )


def encode_label_value(value: str) -> str:
    """Adapta um texto ao charset dos valores de label do BigQuery (minúsculas, dígitos, ``_`` e ``-``; até 63).

    :param value: Texto original (por exemplo, o UUID do flow run).
    :returns: Valor aceito pelo BigQuery.
    """
    return LABEL_UNSAFE.sub("_", value.lower())[:LABEL_VALUE_MAX_LENGTH]


def encode_proof(run_id: str, scn: int, rows: int) -> dict[str, str]:
    """Monta os labels que provam a validação da tabela temporária.

    :param run_id: Flow run do pai.
    :param scn: SCN da foto.
    :param rows: Linhas validadas.
    :returns: Labels para gravar na tabela.
    """
    return {LABEL_RUN_ID: encode_label_value(run_id), LABEL_SCN: str(scn), LABEL_ROWS: str(rows)}


def verify_proof(table_id: str, proof: TableProof | None, run_id: str, scn: int) -> None:
    """Confere que a tabela temporária foi validada para este pai e este SCN, com a contagem atual.

    :param table_id: Nome da tabela, para as mensagens.
    :param proof: Marca lida do BigQuery; ``None`` se a tabela temporária não existe.
    :param run_id: Flow run do pai.
    :param scn: SCN da foto.
    :raises ParallelRunError: Se a tabela não existir, não tiver a marca, ou a marca divergir em run id, SCN ou
        contagem.
    """
    if proof is None:
        raise ParallelRunError(f"{table_id}: a tabela temporária não existe; a tabela final não foi alterada.")
    expected = encode_proof(run_id, scn, proof.num_rows)
    for label, wanted in expected.items():
        found = proof.labels.get(label)
        if found != wanted:
            raise ParallelRunError(
                f"{table_id}: label {label} é {found!r}, esperado {wanted!r}; a tabela temporária não foi validada "
                "para esta execução e a tabela final não foi alterada."
            )
