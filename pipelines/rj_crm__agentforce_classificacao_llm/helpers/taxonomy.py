"""The rules of theme and cause, applied to the summary of each session.

The rules are Python functions of the catalog `taxonomia_regras`. They were made by a
model over a sample, and measured for precision, and a person put them in the catalog.
Here they only run, so no model is called. The same table has the themes (`etapa =
tema`, with no parent) and the causes (`etapa = motivo`, each with the theme that it
belongs to).
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__agentforce_classificacao_llm.constants import NO_SECRETARIAT
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.hsm import normalize_text
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.sandbox import compile_rules
from pipelines.rj_crm__agentforce_classificacao_llm.types import Row, Rule


@dataclass(frozen=True)
class CompiledRule:
    """A category and the function that tells whether a summary belongs to it."""

    category: str
    parent: str | None
    matches: Callable[[str], bool]


def compile_catalog(rules: Sequence[Rule]) -> dict[str, list[CompiledRule]]:
    """Compile the rules of the catalog and group them by secretariat.

    A rule that the sandbox refuses is skipped and logged, and never stops the others.
    """
    index: dict[str, list[CompiledRule]] = {}

    for rule in rules:
        try:
            functions = compile_rules(rule.code)
        except ValueError as error:
            logger.warning("A rule is refused by the sandbox: %s (%s): %s", rule.function_name, rule.secretariat, error)
            continue

        index.setdefault(rule.secretariat, []).extend(
            CompiledRule(rule.name, rule.parent, function) for function in functions.values()
        )

    return index


def match_row(row: Mapping[str, JsonValue], rules: Sequence[CompiledRule]) -> list[str]:
    """Return the categories whose rule matches the summary of a session.

    A rule with a parent runs only for a session that already has that theme in
    `tema_nome`: a cause belongs inside a theme, and not to the whole secretariat. A
    rule that fails on a session is skipped and logged.
    """
    summary = row.get("resumo")
    raw_themes = row.get("tema_nome")
    themes = {theme for theme in raw_themes if isinstance(theme, str)} if isinstance(raw_themes, list) else set()
    text = normalize_text(summary) if isinstance(summary, str) else ""
    found: list[str] = []

    for rule in rules:
        if rule.parent is not None and rule.parent not in themes:
            continue

        try:
            if rule.matches(text):
                found.append(rule.category)
        except Exception as error:
            logger.warning(
                "A rule failed on a session: rule=%s session=%s error=%s: %s",
                rule.category,
                row.get("id_sessao"),
                type(error).__name__,
                error,
            )

    return found


def apply_rules(rows: Sequence[Row], index: Mapping[str, Sequence[CompiledRule]], column: str) -> list[Row]:
    """Return the rows with `column` filled with the categories that the rules found, none if nothing matches.

    Only a session with relevant content and a summary gets rules, those of its
    secretariat or, with none, of `Sem secretaria identificada`. For the others the
    column is empty. With no rule at all, the rows come back as they are.
    """
    if not index:
        return list(rows)

    result: list[Row] = []

    for row in rows:
        summary = row.get("resumo")
        secretariat = row.get("secretaria_principal")
        rules = index.get(secretariat if isinstance(secretariat, str) and secretariat else NO_SECRETARIAT, [])
        relevant = row.get("conteudo_relevante") is True and isinstance(summary, str) and bool(summary)
        found: list[JsonValue] = [*match_row(row, rules)] if relevant else []
        result.append({**row, column: found})

    logger.info("Rules applied: column=%s with_category=%d", column, sum(1 for row in result if row.get(column)))

    return result
