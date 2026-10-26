"""Constantes do passo opcional de dbt antes da carga."""

from copy import deepcopy

DBT_FLOW_NAME = "rj-iplanrio--run-dbt"
"""The name of the flow in Prefect, `rj_iplanrio__run_dbt` with `_` as `-`."""

DBT_DEPLOYMENT = f"{DBT_FLOW_NAME}/rj-iplanrio--run-dbt--prod"
"""Deployment do flow de dbt, no formato ``<flow>/<deployment>``."""

DBT_POLL_SECONDS = 30.0
"""Intervalo entre as consultas ao estado do run de dbt."""

DBT_DEFAULT_PARAMETERS: dict[str, object] = {
    "command": "build",
    "select": "tag:nota_carioca",
    "send_discord_report": False,
    "github_repo": "https://github.com/prefeitura-rio/queries-rj-iplanrio.git",
    "bigquery_project": "rj-iplanrio",
    "target": "prod",
    "gcs_buckets": {"prod": "rj-iplanrio_dbt", "dev": "rj-iplanrio-dev_dbt"},
}


def default_dbt_parameters() -> dict[str, object]:
    """Return a copy of the default parameters of the dbt run.

    They are the parameters of the flow `rj_iplanrio__run_dbt`, and the caller may
    change them.
    """
    return deepcopy(DBT_DEFAULT_PARAMETERS)
