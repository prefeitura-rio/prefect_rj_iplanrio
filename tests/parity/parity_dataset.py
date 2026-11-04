"""The one logical dataset that every engine of the lab holds.

`orders`: 300 regular rows, three rows whose `dia` is NULL, and one row with a big id.
Only the standard library is used, so any Python can print the seed SQL.
"""

from datetime import date
from decimal import Decimal
from sys import argv, stdout
from typing import TypedDict

REGULAR = 300
BIG_ID = 3_000_000_000
NAMES = ["Ana", "João", "ação", "Zé  Maria", " cliente ", "Çedilha", "Bia"]


class Order(TypedDict):
    """One row of `orders`."""

    id: int
    nome: str
    dia: date | None
    valor: Decimal


def rows() -> list[Order]:
    """Return the rows of `orders`."""
    found: list[Order] = []

    for index in range(1, REGULAR + 1):
        found.append(
            {
                "id": index,
                "nome": f"{NAMES[index % len(NAMES)]} {index}",
                "dia": date(2024, 1 + index % 3, 1 + index % 27),
                "valor": Decimal(index) * Decimal("1.5"),
            }
        )

    for index in range(REGULAR + 1, REGULAR + 4):
        found.append({"id": index, "nome": f"sem dia {index}", "dia": None, "valor": Decimal("0.50")})

    found.append({"id": BIG_ID, "nome": "id grande", "dia": date(2024, 2, 29), "valor": Decimal("99999999.99")})

    return found


def quote(text: str) -> str:
    """Return a SQL string literal."""
    return "'" + text.replace("'", "''") + "'"


def literal(value: object, dialect: str) -> str:
    """Return a SQL literal for a value of the dataset."""
    if value is None:
        return "NULL"
    if isinstance(value, date):
        if dialect == "oracle":
            return f"DATE '{value.isoformat()}'"
        return f"'{value.isoformat()}'"
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, int):
        return str(value)
    return ("N" if dialect == "mssql" else "") + quote(str(value))


TYPES = {
    "postgres": {
        "id": "BIGINT",
        "nome": "TEXT",
        "dia": "DATE",
        "valor": "NUMERIC(10,2)",
    },
    "mysql": {
        "id": "BIGINT",
        "nome": "VARCHAR(200) CHARACTER SET utf8mb4",
        "dia": "DATE",
        "valor": "DECIMAL(10,2)",
    },
    "mssql": {
        "id": "BIGINT",
        "nome": "NVARCHAR(200)",
        "dia": "DATE",
        "valor": "DECIMAL(10,2)",
    },
    "oracle": {
        "id": "NUMBER(19)",
        "nome": "VARCHAR2(200 CHAR)",
        "dia": "DATE",
        "valor": "NUMBER(10,2)",
    },
}


def seed_sql(dialect: str, schema: str | None = None) -> str:
    """Return the SQL that creates and fills `orders` and `kinds` in a dialect.

    `kinds` holds one row of the less common types, to read the types back.
    """
    kinds = TYPES[dialect]
    table = f"{schema}.orders" if schema else "orders"
    columns = ", ".join(f"{name} {kind}" for name, kind in kinds.items())
    lines = [f"CREATE TABLE {table} ({columns}, PRIMARY KEY (id));"]

    for row in rows():
        values = ", ".join(literal(row[name], dialect) for name in kinds)
        lines.append(f"INSERT INTO {table} ({', '.join(kinds)}) VALUES ({values});")

    if dialect == "postgres":
        lines += [
            "CREATE TABLE kinds (inteiro INTEGER, longo BIGINT, decimal NUMERIC(20,6), flag BOOLEAN, texto TEXT, "
            "instante TIMESTAMP, instante_utc TIMESTAMPTZ);",
            "INSERT INTO kinds VALUES (7, 3000000000, 1.250000, TRUE, 'café', "
            "'2024-03-01 12:30:00', '2024-03-01 12:30:00+00');",
        ]
    elif dialect == "mysql":
        lines += [
            "CREATE TABLE kinds (inteiro INT, longo BIGINT, decimal_ DECIMAL(20,6), flag BOOLEAN, "
            "texto VARCHAR(100) CHARACTER SET utf8mb4, instante DATETIME, instante_utc TIMESTAMP);",
            "SET time_zone = '+00:00';",
            "INSERT INTO kinds VALUES (7, 3000000000, 1.250000, TRUE, 'café', "
            "'2024-03-01 12:30:00', '2024-03-01 12:30:00');",
        ]
    elif dialect == "mssql":
        lines += [
            "CREATE TABLE kinds (inteiro INT, longo BIGINT, decimal_ DECIMAL(20,6), flag BIT, "
            "texto NVARCHAR(100), instante DATETIME2, instante_utc DATETIMEOFFSET);",
            "INSERT INTO kinds VALUES (7, 3000000000, 1.250000, 1, N'café', "
            "'2024-03-01 12:30:00', '2024-03-01 12:30:00+00:00');",
        ]
    elif dialect == "oracle":
        lines += [
            "CREATE TABLE kinds (inteiro NUMBER(9), longo NUMBER(19), decimal_ NUMBER(20,6), "
            "texto VARCHAR2(100 CHAR), instante TIMESTAMP, instante_utc TIMESTAMP WITH TIME ZONE);",
            "INSERT INTO kinds VALUES (7, 3000000000, 1.250000, 'café', "
            "TIMESTAMP '2024-03-01 12:30:00', TIMESTAMP '2024-03-01 12:30:00 +00:00');",
        ]

    return "\n".join(lines) + ("\nCOMMIT;" if dialect == "oracle" else "\n")


if __name__ == "__main__":
    stdout.write(seed_sql(argv[1], argv[2] if len(argv) > 2 else None) + "\n")
