"""Flow for rj_smfp__nota_carioca_oracle_to_bq."""

from prefect import flow

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.tasks import check_dfen_extraction_task
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.types import Dfen
from utils.constants import DEFAULT_CLOUD
from utils.flows import dump_database
from utils.hooks import notify_failure
from utils.types import Cloud, Database, DatabaseDumpOptions, DumpMode, Read, Table

DEFAULT_DFEN = Dfen()


@flow(flow_run_name="{table[table_id]}", log_prints=True, on_failure=[notify_failure])
async def rj_smfp__nota_carioca_oracle_to_bq(
    *,
    database: Database,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    read: Read,
    dump_mode: DumpMode = "append",
    options: DatabaseDumpOptions | None = None,
    dfen: Dfen = DEFAULT_DFEN,
) -> None:
    """Dump a table of the Nota Carioca Oracle, unless a DFEN extraction runs.

    It is `dump_database`, after a check of `NFSE_OWNER.TAREFAS`: the run stops before
    it reads anything while the application extracts the national notes to the DFEN
    schema (see `Dfen`). A child run of a dump in shards skips the check, because its
    parent made it.
    """
    if "shard" not in (options or {}):
        await check_dfen_extraction_task(database, dfen)

    await dump_database.fn(database=database, cloud=cloud, table=table, read=read, dump_mode=dump_mode, options=options)
