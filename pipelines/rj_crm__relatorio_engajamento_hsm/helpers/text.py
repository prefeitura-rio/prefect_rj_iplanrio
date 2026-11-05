"""Text helpers."""

from re import sub
from unicodedata import normalize

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import CITIZEN_SOURCE, SOURCE_LABELS


def normalize_label(text: object) -> str:
    """Return a category name to compare: no accents, no capitals, one space.

    An underscore counts as a space.
    """
    flat = str(text).strip().lower().replace("_", " ")
    plain = normalize("NFKD", flat).encode("ascii", "ignore").decode("ascii")

    return sub(r"\s+", " ", plain)


def citizen_text(conversation: str) -> str:
    """Return only the citizen lines of a conversation: the text that is embedded.

    The category comes from what the citizen said. The agent answers are long and alike,
    so embedding the whole conversation would blur the match. A line with no label
    continues the message above. A conversation with no citizen line comes back whole.
    """
    prefixes = tuple(f"{label}:" for label in SOURCE_LABELS.values())
    citizen = f"{SOURCE_LABELS[CITIZEN_SOURCE]}:"
    lines: list[str] = []
    speaking = False

    for line in conversation.split("\n"):
        content = line

        if line.startswith(prefixes):
            speaking = line.startswith(citizen)
            content = line.split(":", 1)[1]

        if speaking and content.strip():
            lines.append(content.strip())

    return "\n".join(lines) if lines else conversation
