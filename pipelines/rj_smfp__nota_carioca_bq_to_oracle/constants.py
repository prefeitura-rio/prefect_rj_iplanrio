"""Constantes do passo opcional de dbt antes da carga."""

import copy

DBT_FLOW_NAME = "rj-iplanrio--run-dbt"
"""Nome do flow no Prefect: o nome da função ``rj_iplanrio__run_dbt`` com ``_`` trocado por ``-``."""

DBT_DEPLOYMENT = f"{DBT_FLOW_NAME}/rj-iplanrio--run_dbt--prod"
"""Deployment do flow de dbt, no formato ``<flow>/<deployment>``."""

DBT_POLL_SECONDS = 30.0
"""Intervalo entre as consultas ao estado do run de dbt."""

_DBT_DEFAULT_PARAMETERS: dict[str, object] = {
    "command": "build",
    "select": "tag:nota_carioca",
    "send_discord_report": False,
    "github_repo": "https://github.com/prefeitura-rio/queries-rj-iplanrio.git",
    "bigquery_project": "rj-iplanrio",
    "target": "prod",
    "gcs_buckets": {"prod": "rj-iplanrio_dbt", "dev": "rj-iplanrio-dev_dbt"},
}


def default_dbt_parameters() -> dict[str, object]:
    """Retorna uma cópia dos parâmetros padrão do run de dbt.

    :returns: Parâmetros do flow ``rj_iplanrio__run_dbt``, que o chamador pode alterar.
    """
    return copy.deepcopy(_DBT_DEFAULT_PARAMETERS)
