"""Shared SQL loading utility for all pipelines in this workspace.

Place SQL files in a ``queries/`` directory next to the calling module, then
load and render them with :func:`load_query`::

    from prefect_rj_iplanrio.sql import load_query

    query = load_query(
        __file__,
        "get_last_update",
        project="rj-iplanrio",
        dataset_id=dataset_id,
        table_id=table_id,
    )

SQL files use ``string.Template`` syntax (``$variable`` or ``${variable}``).
This avoids the collision between ``str.format()`` placeholders and BigQuery
``STRUCT<field>`` / ``UNNEST([{}])`` constructs.
"""

from pathlib import Path
from string import Template


def load_query(caller_file: str, name: str, **params: object) -> str:
    """Load and render a SQL file from the pipeline's ``queries/`` directory.

    :param caller_file: Pass ``__file__`` from the calling module. Used to
        resolve the ``queries/`` directory relative to the pipeline package.
    :param name: SQL filename without the ``.sql`` extension.
    :param params: Values substituted into the template using
        ``string.Template`` (``$variable`` / ``${variable}`` syntax).
    :returns: The rendered SQL string with all placeholders replaced.
    :raises FileNotFoundError: If ``queries/<name>.sql`` does not exist.
    :raises KeyError: If a ``$variable`` referenced in the template is absent
        from ``params``.
    """
    sql_file = Path(caller_file).parent / "queries" / f"{name}.sql"
    return Template(sql_file.read_text(encoding="utf-8")).substitute(**params)
