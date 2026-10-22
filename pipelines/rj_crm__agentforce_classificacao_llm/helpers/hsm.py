"""The HSM of a session: its text with the variables in, and its buttons."""

from collections.abc import Mapping, Sequence
from dataclasses import replace
from re import Match, sub
from re import compile as regex
from unicodedata import normalize

from pydantic import JsonValue, TypeAdapter, ValidationError
from toolkit.telemetry import logger

from pipelines.rj_crm__agentforce_classificacao_llm.constants import LCS_RATIO
from pipelines.rj_crm__agentforce_classificacao_llm.types import ButtonRow, Session

JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
PLACEHOLDER = regex(r"\$\{(\w+)\}")


def normalize_text(text: object) -> str:
    """Return a text in lower case, without accents, with the runs of spaces made one.

    Used to compare what the citizen wrote with the title of a button.
    """
    plain = normalize("NFKD", str(text).strip().lower()).encode("ascii", "ignore").decode("ascii")

    return sub(r"\s+", " ", plain)


def button_titles(buttons_json: str | None) -> list[str]:
    """Return the titles of the buttons of an HSM from its JSON, or no title if it is empty or not JSON."""
    if not buttons_json or not buttons_json.strip():
        return []

    try:
        buttons = JSON.validate_json(buttons_json)
    except ValidationError:
        return []

    if not isinstance(buttons, dict):
        return []

    return [str(button.get("title", "")) for button in buttons.values() if isinstance(button, dict)]


def choose_button_row(rows: Sequence[ButtonRow]) -> ButtonRow:
    """Choose one row of the catalog for a journey and an HSM that has many, such as a republished template.

    It prefers the version that does not come after a change of the template version, and breaks a tie by
    the name of the activity.
    """
    current = [row for row in rows if row["template_pos_versao_indicador"] is False]

    return sorted(current or rows, key=lambda row: row["atividade_nome"] or "")[0]


def build_button_lookup(rows: Sequence[ButtonRow]) -> dict[tuple[str, str], str | None]:
    """Return the buttons JSON of each journey and HSM, from the rows of the catalog.

    Rows that repeat a journey, an HSM and the buttons count once.
    """
    seen: set[tuple[str, str, str | None]] = set()
    groups: dict[tuple[str, str], list[ButtonRow]] = {}

    for row in rows:
        key = (row["jornada_nome"], row["hsm_nome"], row["hsm_botoes_json"])

        if key not in seen:
            seen.add(key)
            groups.setdefault(key[:2], []).append(row)

    return {key: choose_button_row(group)["hsm_botoes_json"] for key, group in groups.items()}


def lcs_length(first: str, second: str) -> int:
    """Return the length of the longest common subsequence of two texts."""
    previous = [0] * (len(second) + 1)

    for left in first:
        current = [0]

        for index, right in enumerate(second, start=1):
            current.append(previous[index - 1] + 1 if left == right else max(previous[index], current[index - 1]))

        previous = current

    return previous[-1]


def find_fuzzy(placeholder: str, variables: Mapping[str, JsonValue]) -> JsonValue:
    """Return the value of the key of the variables that looks most like a placeholder, or nothing.

    The keys often have a prefix of the system, such as `cc_wt_solicitacao` for `${solicitacao}`. The key with the
    longest common subsequence wins (capitals do not count), and it must cover 70% of the placeholder.
    """
    target = placeholder.lower()
    best, score = None, 0

    for key in variables:
        current = lcs_length(target, key.lower())

        if current > score:
            best, score = key, current

    return variables[best] if best is not None and score >= LCS_RATIO * len(target) else None


def read_variables(variables: str | Mapping[str, JsonValue] | None) -> Mapping[str, JsonValue] | None:
    """Return the variables of an HSM as an object, or nothing if there are none or they are not an object."""
    parsed: JsonValue | Mapping[str, JsonValue] | None = variables

    if isinstance(variables, str):
        try:
            parsed = JSON.validate_json(variables)
        except ValidationError:
            return None

    return parsed if isinstance(parsed, dict) else None


def render_hsm(text: str | None, variables: str | Mapping[str, JsonValue] | None) -> str | None:
    """Put the values of the variables in the placeholders `${name}` of the text of an HSM.

    A placeholder takes the value of its own key, or else of the key that looks most like it. One that
    finds none stays as it is, because a visible placeholder is better than a wrong value. The text of a
    Session, which has no placeholder, and a text with no usable variables come back as they are.
    """
    values = read_variables(variables)

    if text is None or values is None:
        return text

    def fill(found: Match[str]) -> str:
        key = found.group(1)
        value = values[key] if key in values else find_fuzzy(key, values)

        if value is None:
            logger.info("The placeholder of the HSM is not resolved: %s", key)

        return found.group(0) if value is None else str(value)

    return PLACEHOLDER.sub(fill, text)


def is_late_button_reply(session: Session) -> bool:
    """Tell whether the only message of the citizen is the text of a button of the HSM.

    It means that the answer to a button came after the 24 hours, and was not taken as a click. Only a
    session of one message counts, so a long talk that has the text of a button is not taken for one.
    """
    if session.qtd_mensagens_usuario != 1:
        return False

    titles = button_titles(session.hsm_botoes_json)
    message = normalize_text(session.mensagens_usuario_concatenadas or "")

    return any(normalize_text(title) == message for title in titles)


def enrich_sessions(sessions: Sequence[Session], buttons: Mapping[tuple[str, str], str | None]) -> list[Session]:
    """Return the sessions with the HSM text rendered, the buttons of the HSM, and whether they are a late reply.

    `buttons` has the buttons JSON of each journey and HSM. The variables of the HSM are no longer needed.
    """
    enriched: list[Session] = []

    for session in sessions:
        key = (session.jornada_nome or "", session.hsm_nome or "")
        updated = replace(
            session,
            hsm_texto=render_hsm(session.hsm_texto, session.hsm_variaveis_json),
            hsm_variaveis_json=None,
            hsm_botoes_json=buttons.get(key),
        )
        enriched.append(replace(updated, late_button=is_late_button_reply(updated)))

    return enriched
