"""Plano de carga: colunas da tabela original no Oracle e campos correspondentes no CSV do BigQuery."""

import re
from dataclasses import dataclass
from math import ceil
from typing import Literal

COLUMN_NAME_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_$#]{0,127}$")
SIMPLE_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")
CHARACTER_TYPES = ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR")
ROWID_TYPES = ("ROWID", "UROWID")
TIMESTAMP_TYPE_PATTERN = re.compile(r"^TIMESTAMP\(\d\)( WITH TIME ZONE)?$")
# Data guardada como texto no BigQuery é sempre ``AAAA-MM-DDTHH:MI:SS`` (19 caracteres, sem fração, fuso ou espaço).
# O campo ``DATE`` com máscara é convertido pelo próprio SQL*Loader, sem expressão SQL por linha. O ``T`` literal vai
# entre aspas duplas escapadas dentro da máscara. Valor fora desse formato é rejeitado, e como a carga usa
# ``ERRORS=0`` a carga inteira falha: nenhuma linha é truncada ou perdida em silêncio.
DATE_FIELD = 'DATE "YYYY-MM-DD\\"T\\"HH24:MI:SS"'
TIMESTAMP_TZ_FIELD = 'TIMESTAMP WITH TIME ZONE "YYYY-MM-DD HH24:MI:SS.FF TZR"'
TEXT_FIELD_MAX_BYTES = 4000
FILLER_TEXT_FIELD = f"FILLER CHAR({TEXT_FIELD_MAX_BYTES})"
UTF8_MAX_BYTES_PER_CHAR = 4
NUMBER_FIELD = "CHAR(64)"
# O BigQuery guarda os bytes do RAW como STRING em base64 padrão (4 caracteres a cada 3 bytes, com preenchimento).
# O BASE64_DECODE falha com ORA-29261 em valor nulo; o CASE mantém nulo e vazio como nulo.
RAW_FROM_BASE64 = "CASE WHEN :{name} IS NOT NULL THEN UTL_ENCODE.BASE64_DECODE(UTL_RAW.CAST_TO_RAW(:{name})) END"
RAW_TEXT_MIN_CHARS = 64
RawTextEncoding = Literal["base64", "hex"]


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
        if self.data_type == "RAW":
            return f"RAW({self.data_length})"
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
    :param excluded: Colunas da tabela original que não são criadas na de destino.
    """

    columns: list[OracleColumn]
    fields: list[LoaderField]
    ignored: list[str]
    excluded: list[str]


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


def text_field(column: OracleColumn) -> str:
    """Define o campo de texto do CSV (UTF-8) para uma coluna de caracteres.

    O control file declara ``CHARACTERSET AL32UTF8`` e a sessão usa ``NLS_LANG`` em UTF-8, então o tamanho do
    ``CHAR(n)`` é em bytes do CSV. Um valor que cabe na coluna nunca pode ser rejeitado por tamanho: com
    semântica de bytes num banco UTF-8 ele ocupa no máximo ``data_length`` bytes, mas num banco de byte único
    (ou com semântica de caracteres) cada caractere pode ocupar até 4 bytes no CSV. Por isso o campo tem
    ``4 * data_length`` bytes, limitado a 4000, que é o máximo de um ``VARCHAR2``. Tamanhos pequenos mantêm
    a reserva de memória do SQL*Loader por coluna muito menor que os 4000 bytes fixos.

    :param column: Coluna de destino do tipo caractere.
    :returns: Especificação do campo no control file.
    """
    if column.data_length is None:
        return f"CHAR({TEXT_FIELD_MAX_BYTES})"
    return f"CHAR({min(TEXT_FIELD_MAX_BYTES, UTF8_MAX_BYTES_PER_CHAR * column.data_length)})"


def loader_spec(column: OracleColumn, bq_type: str, raw_text_encoding: RawTextEncoding = "base64") -> str:
    """Define como o SQL*Loader lê um campo do CSV para a coluna de destino.

    Datas guardadas como texto no BigQuery (``AAAA-MM-DDTHH:MI:SS``) são lidas
    como ``DATE`` com máscara pelo próprio SQL*Loader; nulo ou vazio é nulo.
    ``RAW`` guardado como texto no BigQuery vem em base64, decodificado por
    SQL para os bytes originais, ou em hexadecimal, que o SQL*Loader converte
    sozinho ao carregar um campo ``CHAR`` numa coluna ``RAW``. Em ambos, valor
    nulo ou vazio continua nulo.

    :param column: Coluna de destino.
    :param bq_type: Tipo do campo no BigQuery.
    :param raw_text_encoding: Codificação do texto das colunas ``RAW`` no BigQuery.
    :returns: Especificação do campo no control file.
    :raises NotImplementedError: Se a combinação de tipos não tiver suporte, ou se
        a coluna ``RAW`` em base64 tiver um nome que exige aspas (a expressão SQL
        do control file já é delimitada por aspas duplas).
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
    """Monta o plano de carga a partir do schema do BigQuery e da tabela original.

    A tabela de destino repete tipos, tamanhos, ``NOT NULL`` e ordem da tabela
    original, exceto pelas colunas excluídas. Colunas ``ROWID``/``UROWID`` da
    original que não existem no BigQuery são excluídas automaticamente: guardam
    endereços físicos de linhas no próprio banco (por exemplo, em views
    materializadas de join) e não têm como vir de outra origem. Campos do
    BigQuery que não existem na tabela de destino são lidos e descartados (``FILLER``).

    :param bq_fields: Campos do schema do BigQuery (``name``, ``type``, ``mode``).
    :param template_columns: Colunas da tabela original, na ordem dela.
    :param excluded_columns: Outras colunas da original que não devem ser criadas.
    :param raw_text_encoding: Codificação do texto das colunas ``RAW`` no BigQuery.
    :returns: Plano com colunas de destino, campos do CSV e colunas ignoradas e
        excluídas.
    :raises ValueError: Se faltar no BigQuery alguma coluna da original que não
        foi excluída, ou se um nome não for um identificador válido.
    :raises NotImplementedError: Se algum tipo não tiver suporte.
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
            "Se não forem dados de negócio, informe-as no parâmetro excluded_template_columns."
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
