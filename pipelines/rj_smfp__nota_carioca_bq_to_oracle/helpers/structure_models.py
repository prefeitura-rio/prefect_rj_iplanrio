"""Modelos da estrutura física de uma tabela: partições, índices e plano."""

from re import compile as compile_pattern

# Identificadores entre aspas duplas, como o dicionário do Oracle grava as colunas
# dentro de uma expressão de índice.
QUOTED_IDENTIFIER_PATTERN = compile_pattern(r'"([^"]+)"')


STRING_LITERAL_PATTERN = compile_pattern(r"'(?:[^']|'')*'")


USABLE_INDEX_STATUSES = ("VALID", "N/A")


class IndexExpression(str):
    """An index key that is a SQL expression, and not a column name.

    It is a function-based index.

    The text comes from `all_ind_expressions.column_expression`, and it goes in
    `CREATE INDEX` as it is. It compares and hashes as a `str`, and the type only tells
    the expression from a column name, which gets quotes.
    """

    @property
    def columns(self) -> set[str]:
        """List the columns that the expression names, outside text literals.

        They are in double quotes.
        """
        return set(QUOTED_IDENTIFIER_PATTERN.findall(STRING_LITERAL_PATTERN.sub("''", self)))
