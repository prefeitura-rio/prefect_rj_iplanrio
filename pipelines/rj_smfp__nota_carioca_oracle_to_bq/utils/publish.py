"""Publicação tudo ou nada das tabelas finais: conferência de todas antes do primeiro copy e desfazer por time travel.

Cada tabela final é trocada pela temporária com um copy job ``WRITE_TRUNCATE``, que é atômico por tabela mas não
entre tabelas. Por isso:

1. antes de qualquer copy, todas as temporárias e finais são relidas e conferidas (existência, marca de validação,
   partição e cluster iguais); uma falha aborta sem tocar em nada;
2. o instante imediatamente anterior ao primeiro copy é guardado como ponto de restauração;
3. se um copy falhar depois de outras tabelas terem sido publicadas, cada uma delas volta ao estado do ponto de
   restauração por time travel e o erro original é relançado.

A restauração usa um copy job ``WRITE_TRUNCATE`` a partir do decorator de snapshot ``projeto.dataset.TABELA@<ms>``,
verificado no BigQuery real com o client Python (a referência é montada com ``TableReference``, pois o parser de
strings do client recusa o ``@``). Esse caminho mantém partição e cluster da tabela. A janela de time travel do
dataset (7 dias por padrão) precisa cobrir a execução.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import bigquery
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import TableProof, verify_proof
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import TablePlan
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import TableState, assert_layouts_match
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


class TableStore(Protocol):
    """Operações de leitura e troca de tabelas que a publicação precisa."""

    def read(self, table_id: str) -> TableState | None:
        """Lê o estado da tabela; ``None`` se não existir."""
        ...

    def publish(self, temp_id: str, final_id: str) -> None:
        """Substitui a final pela temporária."""
        ...

    def restore(self, table_id: str, timestamp_ms: int) -> None:
        """Devolve a tabela ao estado de ``timestamp_ms`` (UTC, em milissegundos)."""
        ...

    def drop(self, table_id: str) -> None:
        """Apaga uma tabela final criada por esta execução."""
        ...


@dataclass(frozen=True)
class BigQueryStore:
    """:class:`TableStore` sobre o BigQuery.

    :param project: Projeto das tabelas.
    :param dataset_id: Dataset das tabelas.
    """

    project: str
    dataset_id: str

    def read(self, table_id: str) -> TableState | None:
        """Lê o estado da tabela; ``None`` se não existir."""
        return bigquery.read_table(self.project, self.dataset_id, table_id)

    def publish(self, temp_id: str, final_id: str) -> None:
        """Substitui a final pela temporária com copy job ``WRITE_TRUNCATE``."""
        bigquery.copy_over(self.project, self.dataset_id, temp_id, final_id)

    def restore(self, table_id: str, timestamp_ms: int) -> None:
        """Copia o snapshot ``TABELA@<ms>`` de volta sobre a tabela."""
        bigquery.copy_over(self.project, self.dataset_id, f"{table_id}@{timestamp_ms}", table_id)

    def drop(self, table_id: str) -> None:
        """Apaga a tabela final criada nesta execução."""
        bigquery.drop_table(self.project, self.dataset_id, table_id)


def utc_now_ms() -> int:
    """Retorna o instante atual em milissegundos desde a época (UTC)."""
    return int(datetime.now(UTC).timestamp() * 1000)


@dataclass(frozen=True)
class PublishRequest:
    """O que publicar e a prova esperada.

    :param plans: Planos das tabelas, todas já validadas.
    :param run_id: Flow run do pai, gravado nos labels das temporárias.
    :param scn: SCN da foto.
    :param now_ms: Relógio em milissegundos UTC; injetável nos testes.
    """

    plans: Sequence[TablePlan]
    run_id: str
    scn: int
    now_ms: Callable[[], int] = utc_now_ms


def preflight(store: TableStore, request: PublishRequest) -> frozenset[str]:
    """Relê todas as temporárias e finais e confere tudo antes de publicar qualquer tabela.

    :param store: Acesso às tabelas.
    :param request: Planos e prova esperada.
    :returns: Tabelas cuja final já existe (as demais serão criadas pela publicação).
    :raises ParallelRunError: Se alguma temporária faltar ou tiver marca de validação divergente.
    :raises LayoutError: Se partição ou cluster da temporária diferirem da final.
    """
    existing: set[str] = set()
    for plan in request.plans:
        temp = store.read(plan.temp_id)
        proof = None if temp is None else TableProof(labels=temp.labels, num_rows=temp.num_rows)
        verify_proof(plan.table_id, proof, request.run_id, request.scn)
        final = store.read(plan.table_id)
        if final is not None and temp is not None:
            assert_layouts_match(plan.table_id, temp.layout, final.layout)
            existing.add(plan.table_id)
    return frozenset(existing)


def rollback(store: TableStore, published: Sequence[str], existing: frozenset[str], restore_point_ms: int) -> list[str]:
    """Desfaz as tabelas já publicadas, na ordem inversa, sem parar na primeira falha.

    :param store: Acesso às tabelas.
    :param published: Tabelas cujo copy já terminou.
    :param existing: Tabelas cuja final existia antes (as outras são apagadas).
    :param restore_point_ms: Ponto de restauração, em milissegundos UTC.
    :returns: Tabelas que não puderam ser restauradas.
    """
    failed: list[str] = []
    for table_id in reversed(published):
        try:
            if table_id in existing:
                store.restore(table_id, restore_point_ms)
                logger.warning("%s restaurada ao estado de %d ms (UTC)", table_id, restore_point_ms)
            else:
                store.drop(table_id)
                logger.warning("%s não existia antes desta execução; apagada", table_id)
        except Exception:
            logger.exception("Falha ao restaurar %s", table_id)
            failed.append(table_id)
    return failed


def publish_all(store: TableStore, request: PublishRequest, report: Callable[[str], None]) -> None:
    """Publica todas as tabelas ou nenhuma: confere tudo antes e desfaz as já publicadas se um copy falhar.

    :param store: Acesso às tabelas.
    :param request: Planos e prova esperada.
    :param report: Função que publica uma linha de log (Prefect UI).
    :raises Exception: O erro do copy que falhou, relançado depois do rollback; as tabelas que o rollback não
        conseguiu restaurar vão em uma nota do erro.
    """
    existing = preflight(store, request)
    report(f"Conferência prévia ok para {[plan.table_id for plan in request.plans]}; publicando")
    restore_point_ms = request.now_ms()
    report(f"Ponto de restauração: {restore_point_ms} ms (UTC)")
    published: list[str] = []
    try:
        for plan in request.plans:
            store.publish(plan.temp_id, plan.table_id)
            published.append(plan.table_id)
    except BaseException as error:
        report(f"Falha ao publicar; desfazendo {published or 'nenhuma tabela'}: {error}")
        failed = rollback(store, published, existing, restore_point_ms)
        report(f"Restauradas: {[t for t in published if t not in failed]}; sem restaurar: {failed}")
        if failed:
            error.add_note(f"Tabelas que o rollback não conseguiu restaurar ao ponto {restore_point_ms} ms: {failed}")
        raise
