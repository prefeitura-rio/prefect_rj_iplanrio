"""
BigQuery I/O for the NF pipeline: find pending files, write run summaries.

The pipeline no longer maintains a separate status-control table
(``controle_processamento``) — it doesn't track declarations at all anymore
(that matching moved to BigQuery post-processing, see ``matching/README.md``
history). "Already processed" is now derived straight from
``extracao_pagina``, the pipeline's own per-page output table: a file is
considered done when every page already known for it (from any prior run)
has a row at the *current* pipeline version (git commit) — ``ok`` or
``erro_processamento`` both count, since there's no automatic cross-run
retry anymore (an errored page that exhausted its internal retries within
one run is a final result for that version; a stuck page is cleared
manually in BigQuery if needed, not by the pipeline).

``PageStatusReader`` does NOT subclass ``BigQueryClient``: it deliberately
builds its own raw ``bigquery.Client``, for the same reason ``BQInputReader``
used to — it queries one fully-qualified table whose project/dataset may not
match the client's fixed ``dataset_id``.
"""

from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account
from iplanrio_agent_toolkit.bigquery import BigQueryClient

from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)
# TODO(Trick): logger da iplanrio não exibe logs de nível INFO no Prefect
# (bug em investigação). Workaround temporário: usamos logger.warning()
# nos lugares que logicamente seriam logger.info() abaixo. Reverter para
# logger.info() quando o bug for corrigido.


class PageStatusReader:
    """Reads ``extracao_pagina`` to determine which candidate files still need processing."""

    def __init__(
        self,
        project_id: str | None = None,
        credentials_path: Path | None = None,
    ):
        if credentials_path and Path(credentials_path).exists():
            credentials = service_account.Credentials.from_service_account_file(str(credentials_path))
            self.client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        else:
            self.client = bigquery.Client(project=project_id)

    def find_pending_files(
        self,
        candidate_filenames: set[str],
        extracao_pagina_table: str,
        current_commit: str,
    ) -> set[str]:
        """
        Return the subset of ``candidate_filenames`` that still need (re)processing.

        A file is "done" (excluded from the result) when every page already
        known for it in ``extracao_pagina`` (``MAX(pagina)`` across any past
        run/version) also has a row at ``current_commit``. Files never seen
        before in ``extracao_pagina`` are always pending.

        :param candidate_filenames: ``nome_arquivo`` candidates, already
            confirmed to exist in GCS (see ``GCSDownloader.get_available_pdf_filenames``).
        :param extracao_pagina_table: Full BQ table ID, e.g.
            ``'project.dataset.extracao_pagina'``.
        :param current_commit: Short git commit hash identifying this run's
            pipeline code version (see ``processing.metadata.get_git_info``).
        :returns: Subset of ``candidate_filenames`` still pending.
        """
        if not candidate_filenames:
            return set()

        # Filenames are internally-controlled (GCS blob listing), but strip
        # any stray quote defensively before splicing into the query text —
        # this module uses plain string.Template substitution, not real
        # BigQuery query parameters (see prefect_rj_iplanrio.sql.load_query).
        quoted = ", ".join("'" + name.replace("'", "") + "'" for name in candidate_filenames)

        query = load_query(
            __file__,
            "pending_files",
            extracao_pagina_table=extracao_pagina_table,
            candidate_filenames=quoted,
            current_commit=current_commit.replace("'", ""),
        )
        df = self.client.query(query).to_dataframe()
        done = set(df["nome_arquivo"]) if "nome_arquivo" in df.columns and not df.empty else set()
        pending = candidate_filenames - done

        logger.warning(
            "extracao_pagina: %d/%d candidate files already done at commit %s — %d pending",
            len(done),
            len(candidate_filenames),
            current_commit,
            len(pending),
        )
        return pending


class BigQueryWriter(BigQueryClient):
    """Write per-run summary metrics to BigQuery (``pipeline_runs``)."""

    def write_run_summary(self, pipeline_runs_table: str, row: dict) -> None:
        """
        Append one row to the pipeline_runs tracking table via streaming insert.

        The table must exist. Create it once with:
            CREATE TABLE `<pipeline_runs_table>` (
                session_id        STRING,
                flow_run_id       STRING,
                started_at        TIMESTAMP,
                finished_at       TIMESTAMP,
                duration_seconds  FLOAT64,
                pdfs_processed    INT64,
                pdfs_failed       INT64,
                pending_after     INT64,
                avg_sec_per_pdf   FLOAT64,
                batch_size        INT64,
                workers           INT64,
                requests_per_minute INT64,
                max_concurrent    INT64
            );
        """
        self.insert_row(pipeline_runs_table, row)
