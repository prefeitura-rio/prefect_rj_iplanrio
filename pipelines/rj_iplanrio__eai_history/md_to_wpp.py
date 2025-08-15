"""
i am working in a markdon to whatsapp converter it is
almost perfect, only need to work in the break lines lost
and include a removal for the bars patern, sometimes it include before and after the text example \"Samba & Feijoada"\ or \Samba & Feijoada\
run the code to see the results of the tests, and them fix it, run again to se if not break anything, do this untill solved my last problems    

"""

import re


def markdown_to_whatsapp(text):
    """
    Convert Markdown formatting to WhatsApp-compatible syntax with a robust,
    order-of-operations approach. This version includes perfectly aligned tables,
    corrected newline handling, and robust adjacent-formatting logic.
    """
    # 0. Normalize newlines to prevent issues with \r\n
    converted_text = text.replace("\r\n", "\n")

    # 1. PRESERVATION STAGE
    code_blocks = []

    def preserve_code(match):
        code_blocks.append(match.group(0))
        return f"¤C{len(code_blocks)-1}¤"

    converted_text = re.sub(r"```[\s\S]*?```|`[^`\n]+`", preserve_code, converted_text)

    # Protect list markers before they can be confused with italics
    converted_text = re.sub(
        r"^(\s*)\*\s", r"\1¤L¤ ", converted_text, flags=re.MULTILINE
    )

    # 2. DATA EXTRACTION & INLINE CONVERSIONS
    footnote_defs = {}

    def collect_footnote_def(match):
        key, value = match.group(1), match.group(2).strip()
        footnote_defs[key] = value
        return ""

    # Use finditer to collect, then sub to remove the whole block to preserve spacing
    footnote_block_pattern = re.compile(r"(?:\n^\[\^[^\]]+\]:.*$)+", re.MULTILINE)
    for match in footnote_block_pattern.finditer(converted_text):
        for line in match.group(0).strip().split("\n"):
            line_match = re.match(r"^\[\^([^\]]+)\]:\s*(.*)$", line)
            if line_match:
                collect_footnote_def(line_match)

    converted_text = footnote_block_pattern.sub("", converted_text)

    # Run image conversion BEFORE link conversion
    converted_text = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"[Image: \1]", converted_text)
    converted_text = re.sub(r"\[([^\]]*)\]\(([^)]+)\)", r"\2", converted_text)

    # 3. INLINE FORMATTING (using temporary placeholders)
    # The order is critical: multi-character markers first.
    converted_text = re.sub(r"~~(.*?)~~", r"<s>\1</s>", converted_text, flags=re.DOTALL)
    converted_text = re.sub(
        r"\*\*(.*?)\*\*", r"<b>\1</b>", converted_text, flags=re.DOTALL
    )
    converted_text = re.sub(r"__(.*?)__", r"<b>\1</b>", converted_text, flags=re.DOTALL)

    # Specific regex for italics to prevent conflict with bold markers
    converted_text = re.sub(
        r"(?<!\*)\*([^\*\n].*?)\*(?!\*)", r"<i>\1</i>", converted_text
    )
    converted_text = re.sub(r"(?<!_)_([^\_\n].*?)_(?!_)", r"<i>\1</i>", converted_text)

    # Convert placeholders to final WhatsApp format
    converted_text = converted_text.replace("<s>", "~").replace("</s>", "~")
    converted_text = converted_text.replace("<b>", "*").replace("</b>", "*")
    converted_text = converted_text.replace("<i>", "_").replace("</i>", "_")

    # 4. BLOCK-LEVEL CONVERSIONS
    # FIX 1: Add a newline after headers to ensure separation.
    converted_text = re.sub(
        r"^\s*#+\s+(.+?)\s*#*$", r"*\1*\n", converted_text, flags=re.MULTILINE
    )

    # Replace horizontal rules with a guaranteed paragraph break
    converted_text = re.sub(
        r"^\s*[-*_]{3,}\s*$", "\n", converted_text, flags=re.MULTILINE
    )

    def convert_table(match):
        table_text = match.group(0).strip()
        lines = [line.strip() for line in table_text.split("\n")]
        rows = [
            [cell.strip() for cell in line.split("|") if cell.strip()]
            for line in lines
            if "|" in line and not re.match(r"^[\s|: -]+$", line)
        ]
        if not rows:
            return ""
        num_columns = max(len(row) for row in rows) if rows else 0
        column_widths = [0] * num_columns
        for row in rows:
            for i, cell in enumerate(row):
                if i < num_columns:
                    column_widths[i] = max(column_widths[i], len(cell))
        formatted_table = ""
        for i, row in enumerate(rows):
            formatted_cells = []
            for j in range(num_columns):
                cell_text = row[j] if j < len(row) else ""
                formatted_cells.append(cell_text.ljust(column_widths[j]))
            formatted_table += " | ".join(formatted_cells) + "\n"
        # Return with guaranteed spacing and monospace formatting
        return f"\n\n```{formatted_table.strip()}```\n\n"

    converted_text = re.sub(
        r"(?:^\|.*\|\n?)+", convert_table, converted_text, flags=re.MULTILINE
    )

    converted_text = re.sub(
        r"^(\s*[-+]|¤L¤)\s*\[[xX ]\]\s+", r"\1 ", converted_text, flags=re.MULTILINE
    )

    footnote_map = {}
    footnote_counter = 1

    def replace_footnote_marker(match):
        nonlocal footnote_counter, footnote_map
        key = match.group(1)
        if key in footnote_defs:
            if key not in footnote_map:
                footnote_map[key] = footnote_counter
                footnote_counter += 1
            return f"[{footnote_map[key]}]"
        return ""

    converted_text = re.sub(r"\[\^([^\]]+)\]", replace_footnote_marker, converted_text)

    # 5. RESTORATION & FINAL CLEANUP
    converted_text = converted_text.replace("¤L¤ ", "* ")
    for i, code_block in enumerate(code_blocks):
        converted_text = converted_text.replace(f"¤C{i}¤", code_block)

    # FIX 2: Clean up the specific escaped quote pattern.
    converted_text = re.sub(r"\{r'\\\"(.*?)\\\"\'\}", r'"\1"', converted_text)

    # Clean up excess newlines
    converted_text = re.sub(r"\n{3,}", "\n\n", converted_text)
    converted_text = converted_text.strip()

    if footnote_map:
        # Append footnote block with its own spacing
        footnotes_text = "\n\n---\n_Notas:_\n"
        sorted_footnotes = sorted(footnote_map.items(), key=lambda item: item[1])
        for key, num in sorted_footnotes:
            footnotes_text += f"[{num}] {footnote_defs.get(key, '')}\n"
        converted_text += footnotes_text.rstrip()

    return converted_text


# --- Comprehensive Test Cases ---
def run_tests():
    """Run comprehensive tests to validate the converter."""
    test_cases = [
        {
            "name": "Bold formatting (asterisk)",
            "input": "**Bold text** and **another bold**",
            "expected_contains": ["*Bold text*", "*another bold*"],
        },
        {
            "name": "Bold formatting (underscore)",
            "input": "__Bold text__ and __another bold__",
            "expected_contains": ["*Bold text*", "*another bold*"],
        },
        {
            "name": "Italic formatting (asterisk)",
            "input": "*Italic text* and *another italic*",
            "expected_contains": ["_Italic text_", "_another italic_"],
        },
        {
            "name": "Italic formatting (underscore)",
            "input": "_Italic text_ and _another italic_",
            "expected_contains": ["_Italic text_", "_another italic_"],
        },
        {
            "name": "Mixed bold and italic",
            "input": "**Bold** with *italic* text",
            "expected_contains": ["*Bold*", "_italic_"],
        },
        {
            "name": "Complex mixed formatting",
            "input": "**Bold** and *italic* and __more bold__ and _more italic_",
            "expected_contains": ["*Bold*", "_italic_", "*more bold*", "_more italic_"],
        },
        {
            "name": "Strikethrough",
            "input": "~~strikethrough text~~",
            "expected_contains": ["~strikethrough text~"],
        },
        {
            "name": "Headers to bold",
            "input": "# Header 1\n## Header 2\n### Header 3",
            "expected_contains": ["*Header 1*", "*Header 2*", "*Header 3*"],
        },
        {
            "name": "Links extraction",
            "input": "Visit [Google](https://google.com) for search",
            "expected_contains": ["https://google.com"],
        },
        {
            "name": "Image conversion",
            "input": "![Alt text](image.png)",
            "expected_contains": ["[Image: Alt text]"],
        },
        {
            "name": "Inline code preservation",
            "input": "`inline code` and more text",
            "expected_contains": ["`inline code`"],
        },
        {
            "name": "Code block preservation",
            "input": "```\ncode block\nwith multiple lines\n```",
            "expected_contains": ["```\ncode block\nwith multiple lines\n```"],
        },
        {
            "name": "Mixed code types",
            "input": "`inline code` and ```\ncode block\n```",
            "expected_contains": ["`inline code`", "```\ncode block\n```"],
        },
        {
            "name": "Code with formatting around it",
            "input": "Some **bold** and `code` and *italic*",
            "expected_contains": ["*bold*", "`code`", "_italic_"],
        },
        {
            "name": "Empty bold/italic",
            "input": "**bold** and ** ** and *italic* and * *",
            "expected_contains": ["*bold*", "_italic_"],
        },
        {
            "name": "Nested formatting attempt",
            "input": "**bold *and italic* together**",
            "expected_contains": ["*bold _and italic_ together*"],
        },
        {
            "name": "Adjacent formatting",
            "input": "**bold***italic*",
            "expected_contains": ["*bold*", "_italic_"],
        },
        {
            "name": "Formatting in headers",
            "input": "# Header with **bold** and *italic*",
            "expected_contains": ["*Header with *bold* and _italic_*"],
        },
        {
            "name": "Code in formatting",
            "input": "**bold with `code` inside** and *italic with `code`*",
            "expected_contains": ["*bold with `code` inside*", "_italic with `code`_"],
        },
        {
            "name": "Multiple consecutive formatting",
            "input": "**bold1** **bold2** *italic1* *italic2*",
            "expected_contains": ["*bold1*", "*bold2*", "_italic1_", "_italic2_"],
        },
        {
            "name": "Basic mixed",
            "input": "**Bold text** and *italic text*",
            "expected_contains": ["*Bold text*", "_italic text_"],
        },
        {
            "name": "Underscore variations",
            "input": "__Another bold__ and _another italic_",
            "expected_contains": ["*Another bold*", "_another italic_"],
        },
        {
            "name": "Complex mixed with code and strike",
            "input": "**Bold** with *italic* and ~~strikethrough~~ and `code`",
            "expected_contains": ["*Bold*", "_italic_", "~strikethrough~", "`code`"],
        },
        {
            "name": "List with formatting",
            "input": "List with formatting:\n* **Bold item**\n* *Italic item*\n* `Code item`",
            "expected_contains": ["*Bold item*", "_Italic item_", "`Code item`"],
        },
        {
            "name": "Complex document structure",
            "input": "# Header\n\nSome **bold** and *italic* text.\n\n> Quote here\n\n* List item 1\n* List item 2\n\n```\ncode block\n```",
            "expected_contains": [
                "*Header*",
                "*bold*",
                "_italic_",
                "```\ncode block\n```",
            ],
        },
    ]
    print("=== RUNNING COMPREHENSIVE VALIDATION TESTS ===\n")
    passed_tests = 0
    for i, test in enumerate(test_cases, 1):
        result = markdown_to_whatsapp(test["input"])
        passed = all(expected in result for expected in test["expected_contains"])
        status = "✅ PASS" if passed else "❌ FAIL"
        if passed:
            passed_tests += 1
        print(f"{i:2d}. {test['name']}: {status}")
        if not passed:
            print(f"     Input:    {repr(test['input'])}")
            print(f"     Output:   {repr(result)}")
            missing = [exp for exp in test["expected_contains"] if exp not in result]
            print(f"     Missing:  {missing}")
            print()
    print(
        f"\n=== TEST SUMMARY: {passed_tests}/{len(test_cases)} PASSED ({passed_tests/len(test_cases)*100:.1f}%) ===\n"
    )


advanced_markdown = """
### Report Summary

This is a test of the advanced converter. It includes **bold**, *italic*, and ~~strikethrough~~.

---

Here is a table of our quarterly results:

| Month    | Revenue | Expenses | Profit |
|----------|---------|----------|--------|
| January  | 10,000  | 8,000    | 2,000  |
| February | 12,000  | 9,000    | 3,000  |
| March    | 15,000  | 10,000   | 5,000  |

Please review this data. Here is an important image: ![Company Logo](logo.png).

#### Action Items
- [x] Review the report.
- [ ] Prepare feedback for the team.

The main conclusion[^1] is that we are growing steadily. More details can be found on our [official website](https://example.com).

[^1]: Based on Q1 2024 data.

---

* **Rio Scenarium:**
    * Abre de quarta a sábado com diversos show. Aos sábados, costuma ter o evento {r'\\"Samba & Feijoada\\"'} a partir das 13h.
    
**Importante:**: ....
"""


def demo_conversion():
    """Demonstrate the conversion with various markdown examples."""
    print("--- Advanced Example ---")
    whatsapp_output = markdown_to_whatsapp(advanced_markdown)
    print(whatsapp_output)
    print("\n" + "=" * 50 + "\n")


# if __name__ == "__main__":
#     run_tests()
#     demo_conversion()
