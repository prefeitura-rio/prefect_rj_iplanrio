"""Tasks for rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}."""

from typing import TypedDict

from prefect import task
from prefect_rj_iplanrio.sql import load_query


class QueryParam(TypedDict):
    """Nome do arquivo em ``queries/`` e os valores dos placeholders ``$variable``."""

    name: str
    replacements: dict[str, object]


@task
def load_query_task(query: QueryParam) -> str:
    """Renderiza o arquivo ``queries/<name>.sql``."""
    return load_query(__file__, query["name"], **query["replacements"])
