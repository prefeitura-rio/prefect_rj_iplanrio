"""The catalog of categories of one dispatch.

It lives in memory for the processing of one dispatch. The discovery is the only step
that adds a category, and the classification only looks one up.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field

import polars as pl
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    ACTIVE_DECISIONS,
    CATALOG_CATEGORY,
    CATALOG_DECIDED_ON,
    CATALOG_DECISION,
    CATALOG_DESCRIPTION,
    DECISION_ALIASES,
    EXCLUDED_DECISION,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.text import normalize_label


class Catalog:
    """The categories found for a dispatch, each with a description.

    They are looked up by a normalized name.
    """

    def __init__(self) -> None:
        """Start with no category."""
        self.entries: dict[str, tuple[str, str]] = {}

    def register(self, name: str, description: str = "") -> str:
        """Add a category if it is new, and return its canonical name.

        That name is the one that goes in the result.

        A name that differs only in accents, capitals or spaces is the same category.
        """
        key = normalize_label(name)

        return self.entries.setdefault(key, (name.strip(), description.strip()))[0]

    def find(self, name: str) -> str | None:
        """Return the canonical name of a category that exists, or nothing.

        It never adds one.
        """
        entry = self.entries.get(normalize_label(name))

        return entry[0] if entry else None

    def as_dict(self) -> dict[str, str]:
        """Return the descriptions by canonical name."""
        return dict(self.entries.values())

    def as_prompt(self) -> str:
        """Return the catalog as prompt lines in alphabetical order.

        An empty catalog gives a note.
        """
        if not self.entries:
            return "(nenhuma categoria criada ainda — você pode criar as primeiras)"

        return "\n".join(sorted(f"- {name}: {description}" for name, description in self.entries.values()))


@dataclass
class ValidCatalog:
    """The `Catalog` sheet that the secretariat validated.

    `active` has the categories decided Keep or New, the only ones that a
    classification and a reviewed example can use. `every` has all the categories of the
    sheet, so a suggestion never repeats one that the secretariat excluded or left
    undecided. `pending` lists the categories with no valid decision.
    """

    active: Catalog = field(default_factory=Catalog)
    every: Catalog = field(default_factory=Catalog)
    excluded: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    undated: list[str] = field(default_factory=list)
    url: str = ""

    @property
    def empty(self) -> bool:
        """Tell whether no category is active."""
        return not self.active.entries

    def summary(self) -> str:
        """Return the counts of the decisions, for the log."""
        return (
            f"{len(self.active.entries)} ativa(s), {len(self.excluded)} excluída(s), "
            f"{len(self.pending)} sem decisão, {len(self.undated)} ativa(s) sem data da decisão"
        )

    def note(self) -> str:
        """Return the warning for the status of a half-validated catalog, or nothing."""
        if not self.pending:
            return ""

        return f"{len(self.pending)} categoria(s) sem decisão no Catálogo foram ignoradas"


def interpret_catalog(frame: pl.DataFrame) -> ValidCatalog:
    """Return the validated catalog of a sheet read as text.

    The decision ignores capital letters. A blank or unknown decision keeps the category
    out of the classification and logs a warning. The decision date is only for audit.

    Raise `ValueError` if a required column is missing, because a catalog with a changed
    header cannot be read halfway.
    """
    required = (CATALOG_CATEGORY, CATALOG_DESCRIPTION, CATALOG_DECISION)
    missing = [column for column in required if column not in frame.columns]

    if missing:
        raise ValueError(f"The Catalog has no column {missing}")

    catalog = ValidCatalog()

    for row in frame.iter_rows(named=True):
        name = str(row[CATALOG_CATEGORY]).strip()

        if not name:
            continue

        description = str(row[CATALOG_DESCRIPTION])
        catalog.every.register(name, description)
        decision = DECISION_ALIASES.get(str(row[CATALOG_DECISION]).strip().casefold())

        if decision in ACTIVE_DECISIONS:
            catalog.active.register(name, description)

            if not str(row.get(CATALOG_DECIDED_ON, "")).strip():
                catalog.undated.append(name)
        elif decision == EXCLUDED_DECISION:
            catalog.excluded.append(name)
        else:
            catalog.pending.append(name)
            logger.warning("A category has no decision and is ignored: name=%s", name)

    return catalog


def catalog_of(descriptions: Mapping[str, str]) -> Catalog:
    """Return a catalog with these categories and descriptions."""
    catalog = Catalog()

    for name, description in descriptions.items():
        catalog.register(name, description)

    return catalog
