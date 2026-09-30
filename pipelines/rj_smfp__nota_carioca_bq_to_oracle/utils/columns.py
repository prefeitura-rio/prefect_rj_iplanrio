"""Mapeamento de colunas do BigQuery para tipos do Oracle e campos do SQL*Loader."""

import re
from dataclasses import dataclass

ORACLE_TYPES = {
    "STRING": "VARCHAR2(4000 BYTE)",
    "JSON": "VARCHAR2(4000 BYTE)",
    "NUMERIC": "NUMBER",
    "INTEGER": "NUMBER(19)",
    "TIMESTAMP": "TIMESTAMP(6) WITH TIME ZONE",
}

LOADER_FIELDS = {
    "STRING": "CHAR(4000)",
    "JSON": "CHAR(4000)",
    "NUMERIC": "CHAR(64)",
    "INTEGER": "CHAR(32)",
    "TIMESTAMP": 'TIMESTAMP WITH TIME ZONE "YYYY-MM-DD HH24:MI:SS.FF TZR"',
}

COLUMN_NAME_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_$#]{0,127}$")


@dataclass(frozen=True)
class Column:
    """Coluna da tabela de destino no Oracle.

    :param name: Nome da coluna no Oracle, em maiúsculas.
    :param oracle_type: Tipo da coluna no ``CREATE TABLE``.
    :param loader_field: Especificação do campo no control file do SQL*Loader.
    """

    name: str
    oracle_type: str
    loader_field: str


def map_columns(bq_fields: list[dict[str, str]]) -> list[Column]:
    """Converte o schema do BigQuery em colunas do Oracle, na mesma ordem.

    A ordem precisa ser preservada porque o CSV exportado pelo BigQuery segue a
    ordem do schema.

    :param bq_fields: Campos do schema, cada um com as chaves ``name``, ``type``
        e ``mode``.
    :returns: Colunas prontas para gerar o DDL e o control file.
    :raises NotImplementedError: Se houver tipo sem suporte, campo aninhado ou
        campo ``REPEATED``.
    :raises ValueError: Se um nome de coluna não for um identificador válido no
        Oracle.
    """
    unsupported = [
        f"{field['name']} ({field['type']}, {field['mode']})"
        for field in bq_fields
        if field["type"] not in ORACLE_TYPES or field["mode"] == "REPEATED"
    ]
    if unsupported:
        raise NotImplementedError(f"Tipos do BigQuery sem suporte no carregamento para o Oracle: {unsupported}")

    columns = []
    for field in bq_fields:
        name = field["name"].upper()
        if not COLUMN_NAME_PATTERN.match(name):
            raise ValueError(f"Nome de coluna inválido para o Oracle: {field['name']!r}")
        columns.append(
            Column(name=name, oracle_type=ORACLE_TYPES[field["type"]], loader_field=LOADER_FIELDS[field["type"]])
        )
    return columns
