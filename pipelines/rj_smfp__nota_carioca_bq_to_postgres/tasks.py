"""Tasks do carregador das exigíveis no Postgres: ler o manifesto, carregar e publicar, apagar o export."""

from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import manifest as manifest_module
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import notify, object_store, postgres, publication
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.manifest import Manifest


@task(cache_policy=NO_CACHE)
def read_manifest_task(manifesto_uri: str, project: str) -> Manifest:
    """Lê o manifesto do export e confere que ele traz as duas tabelas exigíveis.

    :raises ValueError: Se o URI não for de um manifesto ou se faltar uma tabela no manifesto.
    """
    location = manifest_module.parse_manifest_uri(manifesto_uri)
    text = object_store.open_store(project).read_text(location.bucket, location.name)
    parsed = manifest_module.parse_manifest(text)
    publication.required_exports(parsed)
    log(
        f"Manifesto de {manifesto_uri}: flow run {parsed.flow_run_id}, foto do BigQuery de "
        f"{parsed.snapshot_modified:%Y-%m-%d %H:%M:%S} UTC, tabelas {sorted(parsed.tables)}"
    )
    return parsed


@task(cache_policy=NO_CACHE)
def load_and_publish_task(
    manifesto_uri: str, manifest: Manifest, project: str, pg_dsn_env: str
) -> publication.Publication:
    """Carrega o slot inativo com o export e publica as views para ele, numa só conexão.

    :raises psycopg.errors.DataException: Se a contagem do slot divergir da do manifesto.
    """
    export = publication.ExportRef(
        store=object_store.open_store(project),
        location=manifest_module.parse_manifest_uri(manifesto_uri),
        manifest=manifest,
    )
    with postgres.connect(postgres.read_dsn(pg_dsn_env), "bq_to_postgres") as connection:
        published = publication.load_and_publish(connection, export, notify.current())
    log(f"Publicação {published.id}: slot {published.slot} servindo o flow run {published.flow_run_id}")
    return published


@task(cache_policy=NO_CACHE)
def delete_export_task(manifesto_uri: str, manifest: Manifest, project: str) -> None:
    """Apaga do GCS os arquivos do export já publicado e o manifesto."""
    export = publication.ExportRef(
        store=object_store.open_store(project),
        location=manifest_module.parse_manifest_uri(manifesto_uri),
        manifest=manifest,
    )
    log(f"Removidos {publication.delete_export(export)} objetos de gs://{export.location.bucket}")
