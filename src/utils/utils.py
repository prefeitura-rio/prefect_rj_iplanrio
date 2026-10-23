"""Helpers of the shared tasks: where a table lives, and the steps around it."""

from collections.abc import Sequence
from datetime import date, datetime
from http import HTTPStatus
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from google.auth import default
from google.auth.credentials import Credentials
from httpx2 import AsyncClient, HTTPStatusError, TransportError
from prefect.runtime import flow_run
from pydantic import ValidationError
from toolkit.constants import TIMEZONE
from toolkit.dbt import DbtRun
from toolkit.executor.render import render_template
from toolkit.types import TemplateValue

from .constants import (
    CREDENTIAL_SCOPES,
    DBT_FAILED,
    DBT_WARNED,
    DISCORD_LIMIT,
)
from .records import dig, read_json
from .types import DbtNode, DbtOutcome, DumpMode, DumpOptions


def is_transient(error: object) -> bool:
    """Tell if a failed request is worth sending again.

    A timeout, a network error, a 429 and a 5xx are. A 4xx is not, because the same
    request would fail the same way.
    """
    match error:
        case HTTPStatusError():
            status = error.response.status_code
            return status == HTTPStatus.TOO_MANY_REQUESTS or status >= HTTPStatus.INTERNAL_SERVER_ERROR
        case TransportError():
            return True
        case _:
            return False


def failure_message(flow_name: str, run_name: str, reason: str, url: str | None) -> str:
    """Return the Discord message of a failed flow run, short enough to send."""
    link = f"\n{url}" if url else ""
    return f":red_circle: **{flow_name}** `{run_name}` failed: {reason}{link}"[:DISCORD_LIMIT]


def read_nodes(result: object) -> list[DbtNode]:
    """Return the nodes of a dbt result that failed or warned, in the order of dbt.

    A command with no result per node, such as `debug`, has none.
    """
    items = cast("Sequence[object]", getattr(getattr(result, "result", None), "results", ()))
    nodes: list[DbtNode] = []

    for item in items:
        status = str(getattr(item, "status", ""))
        if status in (*DBT_FAILED, *DBT_WARNED):
            node = getattr(item, "node", item)
            name = getattr(node, "name", None) or getattr(node, "unique_id", "")
            nodes.append(DbtNode(name=str(name), status=status, message=str(getattr(item, "message", None) or "")))

    return nodes


def summarize_dbt(request: DbtRun, outcome: DbtOutcome) -> str:
    """Return the Discord message of a dbt run, cut to the limit of Discord.

    The failed and warned nodes are listed only if there are some. The repository shows
    by its name, never by its URL, because the URL can hold a token.
    """
    name = request.repository.rsplit("/", 1)[-1].removesuffix(".git")
    mark = ":white_check_mark:" if outcome["success"] else ":x:"
    ending = "without errors" if outcome["success"] else "with errors"
    lines = [f"{mark} **[{name}]** `dbt {request.command}` finished {ending}", f"- Target: `{request.target}`"]
    lines += [
        f"- {label}: `{value}`" for label, value in (("Select", request.select), ("Exclude", request.exclude)) if value
    ]

    for node in outcome["nodes"]:
        icon = ":warning:" if node["status"] in DBT_WARNED else ":red_circle:"
        lines.append(f"{icon} `{node['name']}` {node['status']}: {node['message']}")

    return "\n".join(lines)[:DISCORD_LIMIT]


def render_query(anchor: str, name: str, **values: TemplateValue | float | date | None) -> str:
    """Render `queries/<name>.sql`, next to the file `anchor`, as a strict Jinja template.

    A value that is a float, a date or `None` is written as its text. Raise
    `UndefinedError` for a variable that `values` lacks.

    ```python
    render_query(__file__, "last_update", table="p.d.t")  # SELECT max(x) FROM p.d.t
    ```
    """
    plain: dict[str, TemplateValue] = {
        key: str(value) if isinstance(value, float | date | None) else value for key, value in values.items()
    }

    return render_template(name, plain, roots=(Path(anchor).parent / "queries",))


def load_credentials(scopes: Sequence[str] = CREDENTIAL_SCOPES) -> Credentials:
    """Return the ADC credentials (workload identity) for the scopes.

    Raise `DefaultCredentialsError` if the environment has no credentials.
    """
    return cast("Credentials", default(scopes=list(scopes))[0])


def capture_options(dump_mode: DumpMode) -> DumpOptions:
    """Return the options that make an `append` run safe to send again, or none.

    An `append` of a run adds its rows under the capture time of the run, so a retry of
    the same run adds nothing. An `overwrite` replaces the table and needs none.
    """
    return DumpOptions(capture=capture_time()) if dump_mode == "append" else DumpOptions()


def capture_time() -> datetime:
    """Return the scheduled start of the flow run in São Paulo.

    A retry of the same flow run gives the same time, so its capture is the same.
    """
    return cast("datetime", flow_run.scheduled_start_time).astimezone(ZoneInfo(TIMEZONE))


async def post_discord(webhook: str, content: str) -> str | None:
    """Post a message to a Discord webhook, cut to the limit of Discord, and return its id.

    The id is nothing when Discord answers without the message.
    Raise `HTTPStatusError` if Discord refuses it.
    """
    async with AsyncClient() as client:
        response = await client.post(webhook, params={"wait": "true"}, json={"content": content[:DISCORD_LIMIT]})
        response.raise_for_status()

    try:
        identifier = dig(read_json(response), "id")
    except ValidationError:
        return None

    return identifier if isinstance(identifier, str) else None
