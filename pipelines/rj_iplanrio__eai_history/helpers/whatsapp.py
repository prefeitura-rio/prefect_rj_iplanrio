"""Markdown to the formatting of WhatsApp."""

from re import DOTALL, MULTILINE, Match, match, sub
from re import compile as regex

from pipelines.rj_iplanrio__eai_history.constants import CODE_PLACEHOLDER, LIST_PLACEHOLDER

FOOTNOTE_BLOCK = regex(r"(?:\n^\[\^[^\]]+\]:.*$)+", MULTILINE)
FOOTNOTE_LINE = regex(r"^\[\^([^\]]+)\]:\s*(.*)$")
TABLE = regex(r"(?:^\|.*\|\n?)+", MULTILINE)


def preserve_code(text: str) -> tuple[str, list[str]]:
    """Replace each code block and inline code with a placeholder, and return the text with the codes."""
    codes: list[str] = []

    def keep(found: Match[str]) -> str:
        codes.append(found.group(0))

        return f"{CODE_PLACEHOLDER}{len(codes) - 1}\u00a4"

    return sub(r"```[\s\S]*?```|`[^`\n]+`", keep, text), codes


def collect_footnotes(text: str) -> tuple[str, dict[str, str]]:
    """Take the footnote definitions out of the text, and return the text with them by key."""
    definitions: dict[str, str] = {}

    for block in FOOTNOTE_BLOCK.finditer(text):
        for line in block.group(0).strip().split("\n"):
            if found := FOOTNOTE_LINE.match(line):
                definitions[found.group(1)] = found.group(2).strip()

    return FOOTNOTE_BLOCK.sub("", text), definitions


def convert_inline(text: str) -> str:
    """Convert images, links, strikethrough, bold and italics, the longest markers first."""
    text = sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"[Image: \1]", text)
    text = sub(r"\[([^\]]*)\]\(([^)]+)\)", r"\2", text)
    text = sub(r"~~(.*?)~~", r"<s>\1</s>", text, flags=DOTALL)
    text = sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", text, flags=DOTALL)
    text = sub(r"__(.*?)__", r"<b>\1</b>", text, flags=DOTALL)
    text = sub(r"(?<!\*)\*([^\*\n].*?)\*(?!\*)", r"<i>\1</i>", text)
    text = sub(r"(?<!_)_([^\_\n].*?)_(?!_)", r"<i>\1</i>", text)

    return (
        text.replace("<s>", "~")
        .replace("</s>", "~")
        .replace("<b>", "*")
        .replace("</b>", "*")
        .replace("<i>", "_")
        .replace("</i>", "_")
    )


def format_table(found: Match[str]) -> str:
    """Return a Markdown table as aligned text in a code block, with a blank line around it."""
    lines = [line.strip() for line in found.group(0).strip().split("\n")]
    rows = [
        [cell.strip() for cell in line.split("|") if cell.strip()]
        for line in lines
        if "|" in line and not match(r"^[\s|: -]+$", line)
    ]

    if not rows:
        return ""

    columns = max(len(row) for row in rows)
    widths = [max((len(row[index]) for row in rows if index < len(row)), default=0) for index in range(columns)]
    body = "".join(
        " | ".join((row[index] if index < len(row) else "").ljust(widths[index]) for index in range(columns)) + "\n"
        for row in rows
    )

    return f"\n\n```{body.strip()}```\n\n"


def convert_blocks(text: str) -> str:
    """Convert headers to bold, rules to a break, tables to code, and drop the checkbox of a list item."""
    text = sub(r"^\s*#+\s+(.+?)\s*#*$", r"*\1*\n", text, flags=MULTILINE)
    text = sub(r"^\s*[-*_]{3,}\s*$", "\n", text, flags=MULTILINE)
    text = TABLE.sub(format_table, text)

    return sub(rf"^(\s*[-+]|{LIST_PLACEHOLDER})\s*\[[xX ]\]\s+", r"\1 ", text, flags=MULTILINE)


def number_footnotes(text: str, definitions: dict[str, str]) -> tuple[str, dict[str, int]]:
    """Replace the markers of the defined footnotes with numbers in order of appearance, and drop the others."""
    numbers: dict[str, int] = {}

    def number(found: Match[str]) -> str:
        key = found.group(1)

        if key not in definitions:
            return ""

        return f"[{numbers.setdefault(key, len(numbers) + 1)}]"

    return sub(r"\[\^([^\]]+)\]", number, text), numbers


def markdown_to_whatsapp(text: str) -> str:
    """Convert Markdown to the formatting of WhatsApp.

    Code stays as it is, a list marker `*` is not an italic, a table becomes aligned text in a code
    block, a header becomes bold, and the footnotes move to a numbered list at the end.
    """
    converted, codes = preserve_code(text.replace("\r\n", "\n"))
    converted = sub(r"^(\s*)\*\s", rf"\1{LIST_PLACEHOLDER} ", converted, flags=MULTILINE)
    converted, definitions = collect_footnotes(converted)
    converted = convert_blocks(convert_inline(converted))
    converted, numbers = number_footnotes(converted, definitions)
    converted = converted.replace(f"{LIST_PLACEHOLDER} ", "* ")

    for index, code in enumerate(codes):
        converted = converted.replace(f"{CODE_PLACEHOLDER}{index}\u00a4", code)

    converted = sub(r"\{r'\\\"(.*?)\\\"\'\}", r'"\1"', converted)
    converted = sub(r"\n{3,}", "\n\n", converted).strip()

    if numbers:
        notes = "".join(
            f"[{number}] {definitions[key]}\n" for key, number in sorted(numbers.items(), key=lambda item: item[1])
        )
        converted += "\n\n---\n_Notas:_\n" + notes.rstrip()

    return converted
