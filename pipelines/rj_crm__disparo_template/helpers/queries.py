"""The query of the destinations of a run."""

from datetime import date
from pathlib import Path

from pipelines.rj_crm__disparo_template.types import Audience
from utils.dispatch import format_query


def build_query(audience: Audience, folder: Path, today: date) -> str | None:
    """Return the query of the run, or nothing if today is no day to run.

    Its processor decides.

    The text is `audience.query_file`, a file of `folder`, or else `audience.query`.
    With `replacements`, the placeholders are filled, through the processor of
    `audience` if it has one. Raise `ValueError` if there is no query.
    """
    text = (folder / audience.query_file).read_text(encoding="utf-8") if audience.query_file else audience.query

    if not text:
        raise ValueError("The audience needs a query or a query_file")

    if audience.replacements:
        return format_query(text, audience.replacements, audience.processor, today)

    return text
