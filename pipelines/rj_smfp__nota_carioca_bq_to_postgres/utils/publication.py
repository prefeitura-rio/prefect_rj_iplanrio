"""Carga do slot inativo e publicação atômica das views, depois a limpeza do export no GCS.

Ordem que protege o export: os arquivos só são apagados depois de ``publicar_exigiveis`` confirmar; se a carga ou a
publicação falhar, os arquivos e o manifesto ficam para uma nova tentativa. Quem decide se a publicação vale é o
Postgres: ``publicar_exigiveis`` confere as contagens do slot contra as do manifesto (uma linha a menos no CSV faz a
função falhar, e as views continuam no slot anterior).
"""

from dataclasses import dataclass
from datetime import datetime

import psycopg

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.constants import SOURCE_PREFIX
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import checksum, loader, slots
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.manifest import Manifest, ManifestLocation, TableExport
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.notify import PublishNotifier, Step
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.object_store import ObjectStore
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.tables import DETAIL, REQUIRED, SUMMARY, SlotTable
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

LOCK_KEY = "nc_carregador_exigiveis"
PUBLISH_SQL = "SELECT public.publicar_exigiveis(%s, %s, %s, %s::bigint, %s::bigint, %s, %s::timestamptz)"


@dataclass(frozen=True)
class ExportRef:
    """Um export no GCS e o manifesto dele.

    :param store: Acesso ao bucket.
    :param location: Onde está o manifesto.
    :param manifest: O manifesto lido.
    """

    store: ObjectStore
    location: ManifestLocation
    manifest: Manifest


@dataclass(frozen=True)
class Publication:
    """A publicação feita.

    :param id: ``publicacoes.id``.
    :param slot: Slot publicado.
    :param flow_run_id: Flow run do ``bq_to_oracle`` que gerou o export.
    :param checksum: Checksum do slot, calculado no Postgres.
    :param snapshot_bq: Foto do BigQuery do export (a mesma do comentário BQLOAD no Oracle).
    """

    id: int
    slot: str
    flow_run_id: str
    checksum: str
    snapshot_bq: datetime


def required_exports(manifest: Manifest) -> dict[SlotTable, TableExport]:
    """Separa do manifesto as tabelas que vão para o Postgres.

    :param manifest: O manifesto do export.
    :returns: Tabela do slot e o export que a alimenta; as outras tabelas do manifesto ficam de fora.
    :raises ValueError: Se faltar uma das tabelas exigíveis no manifesto.
    """
    missing = [table.bq_name for table in REQUIRED if table.bq_name not in manifest.tables]
    if missing:
        raise ValueError(f"O manifesto não traz as tabelas {missing}; tem {sorted(manifest.tables)}.")
    return {table: manifest.tables[table.bq_name] for table in REQUIRED}


def load_and_publish(connection: psycopg.Connection, export: ExportRef, notifier: PublishNotifier) -> Publication:
    """Carrega o slot inativo, calcula o checksum e publica.

    Uma trava consultiva da sessão impede duas cargas ao mesmo tempo no mesmo slot.

    :param connection: Conexão em autocommit de um login em ``nc_carregador``.
    :param export: Export a publicar.
    :param notifier: Notificador do Discord.
    :returns: A publicação registrada.
    :raises RuntimeError: Se outra carga estiver em andamento.
    :raises psycopg.errors.DataException: Se o Postgres reprovar as contagens do slot contra as do manifesto.
    """
    if not connection.execute("SELECT pg_try_advisory_lock(hashtext(%s))", [LOCK_KEY]).fetchall()[0][0]:
        raise RuntimeError("Outra carga das exigíveis está em andamento no Postgres; nada foi alterado.")
    try:
        manifest, exports = export.manifest, required_exports(export.manifest)
        slot = slots.inactive_slot(connection)
        logger.info("Slot %s recebe a carga; as views continuam no outro até a publicação", slot)
        notifier.enter(Step.LOAD)
        request = loader.LoadRequest(export.store, export.location.bucket, slot, exports)
        loader.load_slot(connection, request, on_table=lambda table, rows: notifier.table_loaded(table.bq_name, rows))
        notifier.enter(Step.PUBLISH)
        digest = checksum.slot_checksum(connection, slot)
        published_id = connection.execute(
            PUBLISH_SQL,
            [
                slot,
                manifest.flow_run_id,
                f"{SOURCE_PREFIX}:{export.location.dataset_id}",
                exports[SUMMARY].rows,
                exports[DETAIL].rows,
                digest,
                manifest.snapshot_modified,
            ],
        ).fetchall()[0][0]
    finally:
        connection.execute("SELECT pg_advisory_unlock(hashtext(%s))", [LOCK_KEY])
    notifier.published(slot, digest, manifest.snapshot_modified)
    logger.info("Publicação %d: slot %s, flow run %s, checksum %s", published_id, slot, manifest.flow_run_id, digest)
    return Publication(published_id, slot, manifest.flow_run_id, digest, manifest.snapshot_modified)


def delete_export(export: ExportRef) -> int:
    """Apaga do GCS os arquivos do manifesto e, por último, o próprio manifesto.

    :param export: Export já publicado.
    :returns: Quantidade de objetos removidos, o manifesto incluído.
    """
    names = [exported.name for table in export.manifest.tables.values() for exported in table.files]
    export.store.delete(export.location.bucket, names)
    export.store.delete(export.location.bucket, [export.location.name])
    return len(names) + 1
