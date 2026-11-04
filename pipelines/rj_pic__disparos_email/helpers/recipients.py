"""Turn the rows of the recipients into mails."""

from ast import literal_eval
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from jinja2 import Environment, FileSystemLoader
from pydantic import JsonValue, TypeAdapter, ValidationError
from toolkit.telemetry import logger

from pipelines.rj_pic__disparos_email.constants import EMAIL_COLUMN, NAME_COLUMN, TEMPLATE_NAME
from pipelines.rj_pic__disparos_email.types import Mail

JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
STUDENTS: TypeAdapter[list[dict[str, JsonValue]]] = TypeAdapter(list[dict[str, JsonValue]])


def parse_filter(text: str) -> list[str]:
    """Return the addresses of the filter, with a quote escaped, or none.

    None come back if the text has no address.

    ```python
    parse_filter("a@x.org, b@x.org")  # ["a@x.org", "b@x.org"]
    parse_filter("")  # []
    ```
    """
    if "@" not in text:
        return []

    return [item.strip().replace("'", "\\'") for item in text.split(",")]


def parse_students(text: str) -> list[dict[str, JsonValue]]:
    """Return the students of the field `dados`.

    It is JSON or the text of a Python list.

    A single object becomes a list of one, and a text that is neither gives no student.
    """
    if not text:
        return []

    try:
        value: object = JSON.validate_json(text)
    except ValidationError:
        try:
            value = cast("object", literal_eval(text))
        except ValueError, SyntaxError:
            logger.warning("The field `dados` is not JSON nor a Python literal: %s", text[:200])

            return []

    items = value if isinstance(value, list) else [value] if value else []

    try:
        return STUDENTS.validate_python(items)
    except ValidationError:
        logger.warning("The field `dados` has an item that is not an object: %s", text[:200])

        return []


def display_name(row: Mapping[str, str]) -> str:
    """Return the recipient name, or the part of the address before `@`.

    The address part is the fallback for a row with no name.
    """
    email = row.get(EMAIL_COLUMN, "").strip()

    return (row.get(NAME_COLUMN, "").strip() or email.split("@")[0]) if email else ""


def render_body(template_dir: Path, variables: Mapping[str, object]) -> str:
    """Render the HTML of the mail, with the escape of HTML on."""
    environment = Environment(loader=FileSystemLoader(template_dir), autoescape=True)

    return environment.get_template(TEMPLATE_NAME).render(**variables)


def build_mail(row: Mapping[str, str], subject: str, template_dir: Path) -> Mail | None:
    """Return the mail of a row, or nothing if the row has no address."""
    email = row.get(EMAIL_COLUMN, "").strip()

    if not email:
        return None

    name = display_name(row)
    students = parse_students(row.get("dados", "[]"))
    body = render_body(
        template_dir,
        {
            "recipiente_nome": name,
            "recipiente_email": email,
            "alunos": students,
            "data_atualizacao": row.get("data_atualizacao", ""),
            "total_alunos": len(students),
        },
    )

    return Mail(to=email, name=name, subject=subject, body=body)
