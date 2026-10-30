"""Tabelas raw: schemas BigQuery versionados em JSON (``schemas_bq/``), particionamento e cluster."""

import json
from dataclasses import dataclass
from functools import lru_cache

from pipelines.rj_smfp__nota_carioca_parser.constants import SCHEMAS_BQ_DIR


@dataclass(frozen=True, slots=True)
class EspecTabela:
    """Layout de uma tabela: ``particao`` é a coluna de partição diária (``None`` sem partição); ``cluster`` a chave."""

    nome: str
    cluster: tuple[str, ...]
    particao: str | None = None


ESPECIFICACOES = {
    "nfse": EspecTabela("nfse", ("NOTA_NACIONAL",), "DATA_HORA_GERACAO"),
    "dps": EspecTabela("dps", ("DPS",), "DATA_HORA_GERACAO"),
    "pessoas": EspecTabela("pessoas", ("HASH_ID",), "DATA_HORA_GERACAO"),
    "eventos": EspecTabela("eventos", ("CHAVE_ACESSO",), "DATA_HORA_GERACAO"),
    "quarentena": EspecTabela("quarentena", ("nsu",)),
    "parser_marca": EspecTabela("parser_marca", ("versao_parser",)),
}
# Ordem de carga de um lote: as tabelas de dados primeiro e a quarentena por último; a marca só depois de todas.
ORDEM_DE_CARGA = ("nfse", "dps", "pessoas", "eventos", "quarentena")


@lru_cache(maxsize=None)
def schema_bigquery(tabela: str) -> tuple[dict[str, str], ...]:
    """Campos do schema BigQuery da tabela (``name``, ``type``, ``mode``, ``description``), lidos do JSON do pacote."""
    campos = json.loads((SCHEMAS_BQ_DIR / f"{tabela}.json").read_text(encoding="utf-8"))
    return tuple(campos)


def colunas(tabela: str) -> tuple[str, ...]:
    """Nomes das colunas da tabela, na ordem do schema."""
    return tuple(campo["name"] for campo in schema_bigquery(tabela))
