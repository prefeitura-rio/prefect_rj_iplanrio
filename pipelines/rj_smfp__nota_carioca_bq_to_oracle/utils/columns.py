"""Plano de carga: colunas da tabela original no Oracle e campos correspondentes no CSV do BigQuery."""

import re
from dataclasses import dataclass

COLUMN_NAME_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_$#]{0,127}$")
SIMPLE_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")
CHARACTER_TYPES = ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR")
TIMESTAMP_TYPE_PATTERN = re.compile(r"^TIMESTAMP\(\d\)( WITH TIME ZONE)?$")
DATE_FROM_TEXT = "TO_DATE(SUBSTR(REPLACE(:{name}, 'T', ' '), 1, 19), 'YYYY-MM-DD HH24:MI:SS')"
TIMESTAMP_TZ_FIELD = 'TIMESTAMP WITH TIME ZONE "YYYY-MM-DD HH24:MI:SS.FF TZR"'
TEXT_FIELD = "CHAR(4000)"
NUMBER_FIELD = "CHAR(64)"
DATE_TEXT_FIELD = "CHAR(64)"


@dataclass(frozen=True)
class OracleColumn:
    """Definição de uma coluna no Oracle, como em ``all_tab_columns``.

    :param name: Nome da coluna, em maiúsculas.
    :param data_type: Tipo base (``VARCHAR2``, ``NUMBER``, ``DATE``...).
    :param data_length: Tamanho em bytes da coluna.
    :param char_used: ``B`` para tamanho em bytes, ``C`` para caracteres.
    :param precision: Precisão de ``NUMBER``, ou ``None``.
    :param scale: Escala de ``NUMBER``, ou ``None``.
    :param nullable: Se a coluna aceita nulo.
    """

    name: str
    data_type: str
    data_length: int | None
    char_used: str | None
    precision: int | None
    scale: int | None
    nullable: bool

    @property
    def ddl_type(self) -> str:
        """Retorna o tipo no formato do ``CREATE TABLE``.

        :raises NotImplementedError: Se o tipo não tiver suporte na carga.
        """
        if self.data_type in CHARACTER_TYPES:
            unit = "CHAR" if self.char_used == "C" else "BYTE"
            return f"{self.data_type}({self.data_length} {unit})"
        if self.data_type == "NUMBER":
            if self.precision is None:
                return "NUMBER" if self.scale is None else f"NUMBER(*,{self.scale})"
            return f"NUMBER({self.precision},{self.scale or 0})"
        if self.data_type == "DATE" or TIMESTAMP_TYPE_PATTERN.match(self.data_type):
            return self.data_type
        raise NotImplementedError(f"Coluna {self.name}: tipo {self.data_type} sem suporte na carga.")

    @property
    def definition(self) -> str:
        """Retorna a definição da coluna no ``CREATE TABLE``, com ``NOT NULL`` quando houver."""
        return f'"{self.name}" {self.ddl_type}' + ("" if self.nullable else " NOT NULL")


@dataclass(frozen=True)
class LoaderField:
    """Campo do CSV no control file do SQL*Loader.

    :param name: Nome do campo, em maiúsculas; igual ao da coluna quando carregado.
    :param spec: Especificação do campo, incluindo conversão SQL ou ``FILLER``.
    """

    name: str
    spec: str


@dataclass(frozen=True)
class LoadPlan:
    """Como uma tabela do BigQuery é carregada na cópia da tabela original.

    :param columns: Colunas da tabela de destino, na ordem da tabela original.
    :param fields: Campos do CSV exportado, na ordem do schema do BigQuery.
    :param ignored: Colunas do BigQuery que não existem na tabela original.
    """

    columns: list[OracleColumn]
    fields: list[LoaderField]
    ignored: list[str]


def oracle_column(row: dict[str, object]) -> OracleColumn:
    """Converte uma linha de ``all_tab_columns`` em :class:`OracleColumn`.

    :param row: Linha com ``column_name``, ``data_type``, ``data_length``,
        ``char_used``, ``data_precision``, ``data_scale`` e ``nullable``.
    :returns: Definição da coluna.
    """
    return OracleColumn(
        name=str(row["column_name"]),
        data_type=str(row["data_type"]),
        data_length=None if row["data_length"] is None else int(row["data_length"]),
        char_used=None if row["char_used"] is None else str(row["char_used"]),
        precision=None if row["data_precision"] is None else int(row["data_precision"]),
        scale=None if row["data_scale"] is None else int(row["data_scale"]),
        nullable=row["nullable"] == "Y",
    )


def loader_spec(column: OracleColumn, bq_type: str) -> str:
    """Define como o SQL*Loader lê um campo do CSV para a coluna de destino.

    Datas guardadas como texto no BigQuery (``AAAA-MM-DDTHH:MI:SS``, com ou sem
    frações de segundo) viram ``DATE``; as frações são descartadas, como no tipo
    ``DATE`` do Oracle.

    :param column: Coluna de destino.
    :param bq_type: Tipo do campo no BigQuery.
    :returns: Especificação do campo no control file.
    :raises NotImplementedError: Se a combinação de tipos não tiver suporte, ou se
        a coluna de data tiver um nome que exige aspas (a expressão SQL do control
        file já é delimitada por aspas duplas).
    """
    ddl_type = column.ddl_type
    if column.data_type in CHARACTER_TYPES and bq_type in ("STRING", "JSON"):
        return TEXT_FIELD
    if column.data_type == "NUMBER" and bq_type in ("NUMERIC", "INTEGER"):
        return NUMBER_FIELD
    if column.data_type == "DATE" and bq_type == "STRING":
        if not SIMPLE_NAME_PATTERN.match(column.name):
            raise NotImplementedError(f"Coluna {column.name}: conversão de data exige nome sem aspas no Oracle.")
        return f'{DATE_TEXT_FIELD} "{DATE_FROM_TEXT.format(name=column.name)}"'
    if ddl_type.endswith("WITH TIME ZONE") and bq_type == "TIMESTAMP":
        return TIMESTAMP_TZ_FIELD
    raise NotImplementedError(f"Coluna {column.name}: carga de {bq_type} do BigQuery em {ddl_type} sem suporte.")


def build_load_plan(bq_fields: list[dict[str, str]], template_columns: list[OracleColumn]) -> LoadPlan:
    """Monta o plano de carga a partir do schema do BigQuery e da tabela original.

    A tabela de destino repete tipos, tamanhos, ``NOT NULL`` e ordem da tabela
    original. Campos do BigQuery que não existem na original são lidos e
    descartados (``FILLER``).

    :param bq_fields: Campos do schema do BigQuery (``name``, ``type``, ``mode``).
    :param template_columns: Colunas da tabela original, na ordem dela.
    :returns: Plano com colunas de destino, campos do CSV e colunas ignoradas.
    :raises ValueError: Se faltar no BigQuery alguma coluna da original, ou se um
        nome não for um identificador válido.
    :raises NotImplementedError: Se algum tipo não tiver suporte.
    """
    by_name = {column.name: column for column in template_columns}
    bq_names = {field["name"].upper() for field in bq_fields}
    missing = [column.name for column in template_columns if column.name not in bq_names]
    if missing:
        raise ValueError(f"Colunas da tabela original ausentes no BigQuery: {missing}")

    fields, ignored = [], []
    for field in bq_fields:
        name = field["name"].upper()
        if not COLUMN_NAME_PATTERN.match(name):
            raise ValueError(f"Nome de coluna inválido para o Oracle: {field['name']!r}")
        if field["mode"] == "REPEATED" or field["type"] in ("RECORD", "STRUCT"):
            raise NotImplementedError(f"Coluna {field['name']}: tipo {field['type']} {field['mode']} sem suporte.")
        if name in by_name:
            fields.append(LoaderField(name=name, spec=loader_spec(by_name[name], field["type"])))
        else:
            fields.append(LoaderField(name=name, spec=f"FILLER {TEXT_FIELD}"))
            ignored.append(name)
    return LoadPlan(columns=list(template_columns), fields=fields, ignored=ignored)
