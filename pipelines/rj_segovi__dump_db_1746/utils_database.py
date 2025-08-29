# -*- coding: utf-8 -*-
# ruff: noqa

from typing import Dict, List, Optional

from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.database_sql import (
    Database,
    MySql,
    Oracle,
    Postgres,
    SqlServer,
)
from iplanrio.pipelines_utils.io import remove_tabs_from_query
from iplanrio.pipelines_utils.logging import log


def parse_comma_separated_string_to_list(text: Optional[str]) -> List[str]:
    """
    Parses a comma separated string to a list.

    Args:
        text: The text to parse.

    Returns:
        A list of strings.
    """
    if text is None or not text:
        return []
    # Remove extras.
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\t", "")
    while ",," in text:
        text = text.replace(",,", ",")
    while text.endswith(","):
        text = text[:-1]
    result = [x.strip() for x in text.split(",")]
    result = [item for item in result if item != "" and item is not None]
    return result


def database_get_db(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    charset: str = NOT_SET,
) -> Database:
    """
    Returns a database object.

    Args:
        database_type: The type of the database.
        hostname: The hostname of the database.
        port: The port of the database.
        user: The username of the database.
        password: The password of the database.
        database: The database name.

    Returns:
        A database object.
    """

    DATABASE_MAPPING: Dict[str, type[Database]] = {
        "mysql": MySql,
        "oracle": Oracle,
        "postgres": Postgres,
        "sql_server": SqlServer,
    }

    if database_type not in DATABASE_MAPPING:
        raise ValueError(f"Unknown database type: {database_type}")
    return DATABASE_MAPPING[database_type](
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset if charset != NOT_SET else None,
    )


def database_execute(
    database,
    query: str,
) -> None:
    """
    Executes a query on the database.

    Args:
        database: The database object.
        query: The query to execute.
    """
    # log(f"Query parsed: {query}")
    query = remove_tabs_from_query(query)
    log(f"Executing query line: {query}")
    database.execute_query(query)
