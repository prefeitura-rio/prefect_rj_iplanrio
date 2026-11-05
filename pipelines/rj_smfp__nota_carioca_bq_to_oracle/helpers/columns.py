"""The load plan: the columns of the original Oracle table and the CSV fields.

The fields come from BigQuery.
"""

from decimal import Decimal
from math import ceil
from re import compile as compile_pattern

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import CHARACTER_TYPES
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.types import LoaderField, LoadPlan, OracleColumn, RawTextEncoding

COLUMN_NAME_PATTERN = compile_pattern(r"^[A-Z_][A-Z0-9_$#]{0,127}$")
SIMPLE_NAME_PATTERN = compile_pattern(r"^[A-Z][A-Z0-9_$#]{0,127}$")
ROWID_TYPES = ("ROWID", "UROWID")
# Data guardada como texto no BigQuery é sempre ``AAAA-MM-DDTHH:MI:SS`` (19 caracteres,
# sem fração, fuso ou espaço). O campo ``DATE`` com máscara é convertido pelo próprio
# SQL*Loader, sem expressão SQL por linha. O ``T`` literal vai entre aspas duplas
# escapadas dentro da máscara. Valor fora desse formato é rejeitado, e como a carga usa
# ``ERRORS=0`` a carga inteira falha: nenhuma linha é truncada ou perdida em silêncio.
DATE_FIELD = 'DATE "YYYY-MM-DD\\"T\\"HH24:MI:SS"'
TIMESTAMP_TZ_FIELD = 'TIMESTAMP WITH TIME ZONE "YYYY-MM-DD HH24:MI:SS.FF TZR"'
TEXT_FIELD_MAX_BYTES = 4000
FILLER_TEXT_FIELD = f"FILLER CHAR({TEXT_FIELD_MAX_BYTES})"
UTF8_MAX_BYTES_PER_CHAR = 4
NUMBER_FIELD = "CHAR(64)"
# O BigQuery guarda os bytes do RAW como STRING em base64 padrão (4 caracteres a cada 3
# bytes, com preenchimento). O BASE64_DECODE falha com ORA-29261 em valor nulo; o CASE
# mantém nulo e vazio como nulo.
RAW_FROM_BASE64 = "CASE WHEN :{name} IS NOT NULL THEN UTL_ENCODE.BASE64_DECODE(UTL_RAW.CAST_TO_RAW(:{name})) END"
RAW_TEXT_MIN_CHARS = 64


def read_int(value: object) -> int | None:
    """Return a number that the database gave as an integer, or nothing for a null.

    Raise `TypeError` for others.
    """
    if value is None:
        return None

    if isinstance(value, int | float | Decimal | str):
        return int(value)

    raise TypeError(f"Valor não numérico: {value!r}")


def oracle_column(row: dict[str, object]) -> OracleColumn:
    """Convert a row of `all_tab_columns` to an `OracleColumn`.

    The row has `column_name`, `data_type`, `data_length`, `char_used`,
    `data_precision`, `data_scale` and `nullable`.
    """
    return OracleColumn(
        name=str(row["column_name"]),
        data_type=str(row["data_type"]),
        data_length=read_int(row["data_length"]),
        char_used=None if row["char_used"] is None else str(row["char_used"]),
        precision=read_int(row["data_precision"]),
        scale=read_int(row["data_scale"]),
        nullable=row["nullable"] == "Y",
    )


def text_field(column: OracleColumn) -> str:
    """Define the CSV text field (UTF-8) of a character column.

    The control file declares `CHARACTERSET AL32UTF8` and the session uses `NLS_LANG` in
    UTF-8, so the size of `CHAR(n)` is in bytes of the CSV. A value that fits the column
    must never be rejected for its size. With byte semantics in a UTF-8 database it
    takes at most `data_length` bytes, but in a single-byte database, or with character
    semantics, each character may take up to 4 bytes in the CSV. So the field has `4 *
    data_length` bytes, capped at 4000, the most that a `VARCHAR2` holds. Small sizes
    keep the memory that SQL*Loader reserves for each column far below the fixed 4000
    bytes.
    """
    if column.data_length is None:
        return f"CHAR({TEXT_FIELD_MAX_BYTES})"
    return f"CHAR({min(TEXT_FIELD_MAX_BYTES, UTF8_MAX_BYTES_PER_CHAR * column.data_length)})"


def loader_spec(column: OracleColumn, bq_type: str, raw_text_encoding: RawTextEncoding = "base64") -> str:
    """Define how SQL*Loader reads a CSV field into the target column.

    Dates stored as text in BigQuery (`YYYY-MM-DDTHH:MI:SS`) are read as `DATE` with a
    mask by SQL*Loader itself, and null or empty is null. A `RAW` stored as text in
    BigQuery comes in base64, which SQL decodes to the original bytes, or in
    hexadecimal, which SQL*Loader converts alone when it loads a `CHAR` field into a
    `RAW` column. In both, a null or empty value stays null. `raw_text_encoding` is the
    encoding of the text of the `RAW` columns in BigQuery.

    Raise `NotImplementedError` if the pair of types is not supported, or if a base64
    `RAW` column has a name that needs quotes, because the SQL expression of the control
    file is already inside double quotes.
    """
    ddl_type = column.ddl_type
    if column.data_type in CHARACTER_TYPES and bq_type in ("STRING", "JSON"):
        return text_field(column)
    if column.data_type == "NUMBER" and bq_type in ("NUMERIC", "INTEGER"):
        return NUMBER_FIELD
    if column.data_type == "DATE" and bq_type == "STRING":
        return DATE_FIELD
    if column.data_type == "RAW" and bq_type == "STRING":
        if raw_text_encoding == "hex":
            return f"CHAR({2 * int(column.data_length or 0)})"
        if not SIMPLE_NAME_PATTERN.match(column.name):
            raise NotImplementedError(f"Coluna {column.name}: conversão de RAW exige nome sem aspas no Oracle.")
        size = max(RAW_TEXT_MIN_CHARS, 4 * ceil(int(column.data_length or 0) / 3))
        return f'CHAR({size}) "{RAW_FROM_BASE64.format(name=column.name)}"'
    if ddl_type.endswith("WITH TIME ZONE") and bq_type == "TIMESTAMP":
        return TIMESTAMP_TZ_FIELD
    raise NotImplementedError(f"Coluna {column.name}: carga de {bq_type} do BigQuery em {ddl_type} sem suporte.")


def build_load_plan(
    bq_fields: list[dict[str, str]],
    template_columns: list[OracleColumn],
    excluded_columns: list[str] | None = None,
    raw_text_encoding: RawTextEncoding = "base64",
) -> LoadPlan:
    """Build the load plan from the BigQuery schema and the original table.

    The target table repeats the types, sizes, `NOT NULL` and order of the original
    table, except for the excluded columns. The `ROWID` and `UROWID` columns of the
    original that BigQuery lacks are excluded on their own: they hold the physical
    addresses of rows in the same database (in materialized join views, for example) and
    cannot come from another source. A BigQuery field that the target lacks is read and
    dropped (`FILLER`).

    Raise `ValueError` if BigQuery lacks a column of the original that was not excluded,
    or if a name is not a valid identifier. Raise `NotImplementedError` if a type is not
    supported.
    """
    bq_names = {field["name"].upper() for field in bq_fields}
    requested = {name.upper() for name in excluded_columns or []}
    excluded = [
        column.name
        for column in template_columns
        if column.name in requested or (column.name not in bq_names and column.data_type in ROWID_TYPES)
    ]
    columns = [column for column in template_columns if column.name not in excluded]
    missing = [column.name for column in columns if column.name not in bq_names]
    if missing:
        raise ValueError(
            f"Colunas da tabela original ausentes no BigQuery: {missing}. "
            + "Se não forem dados de negócio, informe-as no parâmetro excluded_template_columns."
        )

    by_name = {column.name: column for column in columns}
    fields, ignored = [], []
    for field in bq_fields:
        name = field["name"].upper()
        if not COLUMN_NAME_PATTERN.match(name):
            raise ValueError(f"Nome de coluna inválido para o Oracle: {field['name']!r}")
        if field["mode"] == "REPEATED" or field["type"] in ("RECORD", "STRUCT"):
            raise NotImplementedError(f"Coluna {field['name']}: tipo {field['type']} {field['mode']} sem suporte.")
        if name in by_name:
            fields.append(LoaderField(name=name, spec=loader_spec(by_name[name], field["type"], raw_text_encoding)))
        else:
            fields.append(LoaderField(name=name, spec=FILLER_TEXT_FIELD))
            ignored.append(name)
    return LoadPlan(columns=columns, fields=fields, ignored=ignored, excluded=excluded)
