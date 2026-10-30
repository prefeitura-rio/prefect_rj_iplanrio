"""Schema do BigQuery derivado da própria DLO.

O usuário de integração **não** tem permissão de ler metadado de Data Stream
(``GET /ssot/data-streams`` volta vazio para ele). O caminho que funciona é
``SELECT * FROM <dlo> LIMIT 0``: a resposta traz a lista de colunas com tipo
(``Varchar``, ``Numeric``, ``TimestampTZ``) e custa uma query vazia.

Assim o schema acompanha a fonte sozinho — o Account tem 105 campos na QA e
106 em produção, e a coluna extra não quebra nada.
"""

from __future__ import annotations

import re
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
    "TimestampTZ": _TIMESTAMP,
    "DateTime": _TIMESTAMP,
    "Boolean": _BOOLEAN,
    "Date": _DATE,
}

CONTROLE = ("_loaded_at", "data_criacao", "data_atualizacao")


def _snake(name: str) -> str:
    """Converte CamelCase/PascalCase para snake_case."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower()


def nome_final(nome_dlo: str, renomear: dict[str, str] | None = None) -> str:
    """Nome da coluna no BigQuery a partir do nome na DLO.

    Remove o prefixo ``ssot__`` e o sufixo ``__c``/``__dlm``/``__dll``, aplica
    snake_case e depois o mapa ``renomear`` (que tira o ``_c`` residual que o
    Data Cloud deixa no lugar de ``__c`` em campo custom).

    :param nome_dlo: nome da coluna como vem da API (ex.: ``Data_Fechamento__c``).
    :param renomear: mapa aplicado depois da normalização.
    :returns: nome final da coluna.
    """
    limpo = re.sub(r"^ssot__", "", nome_dlo)
    limpo = re.sub(r"(__c|__dlm|__dll)$", "", limpo)
    normalizado = _snake(limpo)
    return (renomear or {}).get(normalizado, normalizado)


@dataclass(frozen=True)
class SchemaTabela:
    """Schema de uma tabela (staging e final compartilham os mesmos campos)."""

    campos: tuple[bigquery.SchemaField, ...]
    datas: tuple[str, ...]
    colunas: tuple[str, ...]
    datas_date: tuple[str, ...] = ()

    @property
    def campos_finais(self) -> list[bigquery.SchemaField]:
        """Campos da tabela final: os da staging + ``_first_loaded_at``.

        ``_first_loaded_at`` é preenchido só no INSERT do MERGE e nunca é
        tocado no UPDATE — guarda a primeira vez que o registro apareceu.
        """
        return [*self.campos, bigquery.SchemaField("_first_loaded_at", _TIMESTAMP)]


def montar(
    colunas_dlo: list[dict],
    renomear: dict[str, str] | None = None,
    booleanas: list[str] | None = None,
) -> SchemaTabela:
    """Monta o schema a partir das colunas devolvidas por ``SELECT * LIMIT 0``.

    :param colunas_dlo: lista ``[{"name": ..., "type": ...}]``.
    :param renomear: mapa de renomeação (ver :func:`nome_final`).
    :param booleanas: colunas que chegam como texto e viram BOOLEAN.
    :returns: schema pronto para criar tabela e carregar o DataFrame.
    :raises ValueError: Se aparecer um tipo de coluna não mapeado.
    """
    booleanas = booleanas or []
    campos: list[bigquery.SchemaField] = []
    datas: list[str] = []
    datas_date: list[str] = []
    vistos: set[str] = set()

    for coluna in colunas_dlo:
        nome = nome_final(coluna["name"], renomear)
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
                raise ValueError(f"tipo '{tipo}' da coluna '{coluna['name']}' não mapeado.") from exc

        if tipo_bq == _TIMESTAMP:
            datas.append(nome)
        if tipo_bq == _DATE:
            datas_date.append(nome)
        if nome in ("_loaded_at", "data_criacao", "data_atualizacao"):
            raise ValueError(f"a DLO tem uma coluna que colide com controle: '{nome}'.")
        campos.append(bigquery.SchemaField(nome, tipo_bq))

    campos.extend(
        [
            bigquery.SchemaField("_loaded_at", _TIMESTAMP, description="Última carga que tocou a linha"),
            bigquery.SchemaField("data_criacao", _DATE, description="Dia de criação do registro (São Paulo)"),
            bigquery.SchemaField("data_atualizacao", _DATE, description="Dia da última alteração (São Paulo)"),
        ]
    )

    return SchemaTabela(
        campos=tuple(campos),
        datas=tuple(datas),
        colunas=tuple(c.name for c in campos),
        datas_date=tuple(datas_date),
    )
