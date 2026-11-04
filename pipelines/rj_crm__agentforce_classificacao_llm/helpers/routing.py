"""Render and validate the macrotheme and secretariat routing prompts."""

from functools import cache
from pathlib import Path

from pydantic import JsonValue, TypeAdapter

from pipelines.rj_crm__agentforce_classificacao_llm.helpers.hsm import normalize_text
from pipelines.rj_crm__agentforce_classificacao_llm.types import MacrothemeCatalog, SecretariatCatalog

ROOT = Path(__file__).parent.parent
MACROTHEME_CATALOG = TypeAdapter(list[MacrothemeCatalog])
SECRETARIAT_CATALOG = TypeAdapter(list[SecretariatCatalog])
OUTSIDE = "OUTRA"
NO_HSM = "(Nenhum HSM identificado para esta sessão.)"
FULL_OUTSIDE_RULE = "3. Se o assunto não for responsabilidade de nenhum órgão da lista, os dois campos devem ser null."
FILTERED_OUTSIDE_RULE = (
    "3. Se o assunto for de um órgão da Prefeitura que não está na lista, "
    f'responda "{OUTSIDE}" nos dois campos. Se não for responsabilidade da '
    "Prefeitura, os dois campos devem ser null."
)


MACROTHEMES = MACROTHEME_CATALOG.validate_json((ROOT / "pipelines" / "macrotemas.json").read_bytes())
SECRETARIATS = SECRETARIAT_CATALOG.validate_json((ROOT / "pipelines" / "secretarias.json").read_bytes())
MACROTHEME_LABELS = tuple(str(item["nome"]) for item in MACROTHEMES)
SECRETARIAT_LABELS = tuple(str(item["sigla"]) for item in SECRETARIATS)
SECRETARIATS_BY_MACROTHEME = {
    str(item["nome"]): tuple(item.get("secretarias", [])) for item in MACROTHEMES if item.get("secretarias")
}

if comma_labels := [label for label in (*MACROTHEME_LABELS, *SECRETARIAT_LABELS) if "," in label]:
    raise ValueError(f"Routing labels cannot contain commas: {comma_labels}")

if unknown := {value for values in SECRETARIATS_BY_MACROTHEME.values() for value in values} - set(SECRETARIAT_LABELS):
    raise ValueError(f"Macrothemes contain unknown secretariats: {sorted(unknown)}")


def fill(template: str, hsm: str | None, conversation: str | None) -> str:
    """Put the HSM and conversation in a routing prompt."""
    return template.replace("<<HSM_TEXTO>>", hsm or NO_HSM).replace("<<CONVERSA_COMPLETA>>", conversation or "")


@cache
def macrotheme_template() -> str:
    """Return the macrotheme template with its stable catalog prefix."""
    catalog = "\n".join(f"- {item['nome']}: {item['descricao']}" for item in MACROTHEMES)
    return (ROOT / "prompts" / "classificacao_macrotema.txt").read_text().replace("<<MACROTEMAS>>", catalog)


def macrotheme_prompt(hsm: str | None, conversation: str | None) -> str:
    """Render the macrotheme prompt for one session."""
    return fill(macrotheme_template(), hsm, conversation)


def candidate_secretariats(related: str | None) -> tuple[str, ...] | None:
    """Return candidates for related macrothemes, or nothing for the full list."""
    if not related:
        return None

    found: set[str] = set()
    for name in related.split(", "):
        if name not in SECRETARIATS_BY_MACROTHEME:
            return None
        found.update(SECRETARIATS_BY_MACROTHEME[name])

    return tuple(sorted(found))


@cache
def secretariat_template(candidates: tuple[str, ...] | None) -> str:
    """Return a secretariat template with a stable filtered or full prefix."""
    rows = SECRETARIATS if candidates is None else [item for item in SECRETARIATS if item["sigla"] in candidates]
    catalog = "\n".join(f"- {item['sigla']} - {item['nome']}: {item['descricao']}" for item in rows)
    rule = FULL_OUTSIDE_RULE if candidates is None else FILTERED_OUTSIDE_RULE
    return (
        (ROOT / "prompts" / "classificacao_secretaria.txt")
        .read_text()
        .replace("<<SECRETARIAS>>", catalog)
        .replace("<<REGRA_FORA_DA_LISTA>>", rule)
    )


def secretariat_prompt(candidates: tuple[str, ...] | None, hsm: str | None, conversation: str | None) -> str:
    """Render the secretariat prompt for one session."""
    return fill(secretariat_template(candidates), hsm, conversation)


def canonical(value: JsonValue, labels: tuple[str, ...] | list[str]) -> str | None:
    """Return the catalog spelling of one label, or nothing for an unknown label."""
    if not isinstance(value, str) or not value.strip():
        return None
    return {normalize_text(label): label for label in labels}.get(normalize_text(value))


def related(value: JsonValue, labels: tuple[str, ...] | list[str]) -> str | None:
    """Return unique valid comma-separated labels with catalog spelling."""
    if not isinstance(value, str):
        return None
    found = dict.fromkeys(label for part in value.split(",") if (label := canonical(part, labels)))
    return ", ".join(found) if found else None


def check_pair(
    principal_value: JsonValue,
    related_value: JsonValue,
    labels: tuple[str, ...] | list[str],
) -> tuple[str | None, str | None]:
    """Validate principal and related labels and include the principal in both."""
    principal = canonical(principal_value, labels)
    related_text = related(related_value, labels)
    values = related_text.split(", ") if related_text else []

    if principal and principal not in values:
        values.insert(0, principal)

    return principal, ", ".join(values) if values else None
