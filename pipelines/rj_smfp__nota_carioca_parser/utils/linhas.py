"""Metadados das linhas raw e serialização NDJSON para o load job do BigQuery."""

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal

COLUNAS_METADADOS = ("NSU", "DATA_HORA_GERACAO", "VERSAO_PARSER", "INGERIDO_EM")


def com_metadados(
    linha: Mapping[str, object],
    nsu: int,
    data_hora_geracao: datetime | None,
    versao_parser: str,
    ingerido_em: datetime,
) -> dict[str, object]:
    """Acrescenta ``NSU``, ``DATA_HORA_GERACAO``, ``VERSAO_PARSER`` e ``INGERIDO_EM`` (uma linha raw é append-only)."""
    return {
        **linha,
        "NSU": nsu,
        "DATA_HORA_GERACAO": None if data_hora_geracao is None else data_hora_geracao.isoformat(),
        "VERSAO_PARSER": versao_parser,
        "INGERIDO_EM": ingerido_em.isoformat(),
    }


def _valor(objeto: object) -> str:
    if isinstance(objeto, Decimal):
        return format(objeto, "f")
    if isinstance(objeto, datetime):
        return objeto.isoformat()
    raise TypeError(f"Tipo sem serialização para o BigQuery: {type(objeto).__name__}")


def serializar_ndjson(linhas: Sequence[Mapping[str, object]]) -> bytes:
    """Uma linha JSON por registro (``NUMERIC`` como texto exato), em UTF-8, pronto para o load job."""
    return "\n".join(json.dumps(linha, default=_valor, ensure_ascii=False) for linha in linhas).encode("utf-8")
