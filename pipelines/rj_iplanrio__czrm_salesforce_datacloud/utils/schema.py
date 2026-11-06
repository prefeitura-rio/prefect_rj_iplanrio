"""Schema do BigQuery derivado da própria DLO.

O usuário de integração não tem permissão de ler metadado de Data Stream
(``GET /ssot/data-streams`` retorna vazio). O caminho disponível é
``SELECT * FROM <dlo> LIMIT 0``: a resposta traz colunas e tipos sem custo.

Os nomes de coluna são preservados como vêm da API — sem snake_case nem
remoção de prefixos. O BigQuery aceita os nomes originais do Data Cloud.
"""

from __future__ import annotations

from dataclasses import dataclass

from google.cloud import bigquery

_STRING = bigquery.enums.SqlTypeNames.STRING
_FLOAT64 = bigquery.enums.SqlTypeNames.FLOAT64
_TIMESTAMP = bigquery.enums.SqlTypeNames.TIMESTAMP
_BOOLEAN = bigquery.enums.SqlTypeNames.BOOLEAN
_DATE = bigquery.enums.SqlTypeNames.DATE

TIPOS = {
    "Varchar": _STRING,
    "Text": _STRING,
    "Phone": _STRING,
    "Email": _STRING,
    "Url": _STRING,
    "Numeric": _FLOAT64,
    "Number": _FLOAT64,
    "Percent": _FLOAT64,
    "Currency": _FLOAT64,
    "Int": _FLOAT64,
    "BigInt": _FLOAT64,
    "SmallInt": _FLOAT64,
    "Long": _FLOAT64,
    "Float": _FLOAT64,
    "Double": _FLOAT64,
    "Decimal": _FLOAT64,
    "TimestampTZ": _TIMESTAMP,
    "Timestamp": _TIMESTAMP,
    "DateTime": _TIMESTAMP,
    "Bool": _BOOLEAN,
    "Boolean": _BOOLEAN,
    "Date": _DATE,
}


@dataclass(frozen=True)
class SchemaTabela:
    """Schema de uma tabela derivado da DLO."""

    campos: tuple[bigquery.SchemaField, ...]
    datas: tuple[str, ...]
    datas_date: tuple[str, ...]
    colunas: tuple[str, ...]


def montar(
    colunas_dlo: list[dict],
    booleanas: list[str] | None = None,
) -> SchemaTabela:
    """Monta o schema a partir das colunas devolvidas por ``SELECT * LIMIT 0``.

    Os nomes de coluna são usados como vieram da API, sem normalização.

    :param colunas_dlo: lista ``[{"name": ..., "type": ...}]``.
    :param booleanas: colunas que chegam como texto e devem virar BOOLEAN.
    :returns: schema com campos, listas de colunas timestamp/date e nomes.
    :raises ValueError: se aparecer um tipo de coluna não mapeado.
    """
    booleanas = booleanas or []
    campos: list[bigquery.SchemaField] = []
    datas: list[str] = []
    datas_date: list[str] = []
    vistos: set[str] = set()

    for coluna in colunas_dlo:
        nome = coluna["name"]
        if nome in vistos:
            continue
        vistos.add(nome)

        tipo = coluna.get("type", "Varchar")
        if nome in booleanas:
            tipo_bq = _BOOLEAN
        else:
            try:
                tipo_bq = TIPOS[tipo]
            except KeyError as exc:
                raise ValueError(f"tipo '{tipo}' da coluna '{nome}' não mapeado.") from exc

        if tipo_bq == _TIMESTAMP:
            datas.append(nome)
        if tipo_bq == _DATE:
            datas_date.append(nome)

        campos.append(bigquery.SchemaField(nome, tipo_bq))

    campos.append(bigquery.SchemaField("_loaded_at", _TIMESTAMP, description="Instante da carga em UTC"))

    return SchemaTabela(
        campos=tuple(campos),
        datas=tuple(datas),
        datas_date=tuple(datas_date),
        colunas=tuple(c.name for c in campos),
    )
