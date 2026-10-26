"""Flow for rj_sms__sisreg_disparo_lembretes."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_sms__sisreg_disparo_lembretes.constants import DEFAULT_TABLE
from pipelines.rj_sms__sisreg_disparo_lembretes.tasks import read_recipients_task, send_batches_task
from pipelines.rj_sms__sisreg_disparo_lembretes.types import Dispatch
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.hsm import Recipient
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options

DEFAULT_DISPATCH = Dispatch()


@flow(log_prints=True, flow_run_name="sisreg-{table[table_id]}", on_failure=[notify_failure])
async def rj_sms__sisreg_disparo_lembretes(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table = DEFAULT_TABLE,
    dispatch: Dispatch = DEFAULT_DISPATCH,
    dump_mode: DumpMode = "append",
    destinations: list[Recipient] | None = None,
) -> None:
    """Send an SMS reminder to each patient with an appointment tomorrow, through the Wetalkie API.

    The query of `queries/appointments.sql` gives one recipient for each appointment,
    with the phone and the `vars` of the template. A phone that comes twice gets one
    message. With `destinations`, the query is skipped and these are the recipients.
    They go out in batches of `dispatch.chunk_size`, and every batch that goes out is
    recorded in `table`, even if a later batch fails. Raise `RuntimeError` if the API
    does not answer or a batch fails.
    """
    to_send = await read_recipients_task(dispatch, destinations)

    if not to_send:
        logger.info("No recipient: nothing was sent")
        return

    outcome = await send_batches_task(dispatch, to_send)

    if outcome.rows:
        with TemporaryDirectory() as directory:
            folder = await write_records(outcome.rows, Path(directory))
            await upload_files_to_gcs(
                folder,
                table,
                bucket=cloud["bucket"],
                dump_mode=dump_mode,
                source_format="json",
                options=capture_options(dump_mode),
            )

        await publish_table_in_bq(table, cloud)

    if outcome.error:
        raise RuntimeError(outcome.error)
