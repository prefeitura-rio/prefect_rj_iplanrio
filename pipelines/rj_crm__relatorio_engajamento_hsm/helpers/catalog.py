"""The catalog of categories of one dispatch.

It lives in memory for the processing of one dispatch. The discovery is the only step
that adds a category, and the classification only looks one up.
"""

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
