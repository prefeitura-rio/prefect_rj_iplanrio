"""Porta da fonte do histórico (o Oracle da app) e o formato de uma linha lida dela."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class LinhaOracle:
    """Linha de ``MV_DOCUMENTOSFISCAISNACIONAIS`` ou da ``_QUARENTENA``.

    :param nsu: NSU de distribuição (chave primária).
    :param chave_acesso: ``CHAVE_ACESSO``.
    :param tipo_documento: ``TIPO_DOCUMENTO`` numérico (1 NFS-e, 2 evento, 3 CNC).
    :param tipo_evento: ``TIPO_EVENTO`` numérico (1 é a emissão, que o ADN não envia).
    :param data_hora_geracao: ``DATA_HORA_GERACAO``: um ``DATE`` com a hora local de São Paulo, sem fuso.
    :param arquivo_xml: ``ARQUIVO_XML`` (CLOB) como texto; sanitizado na tabela principal, como veio na quarentena.
    :param quarentena: ``True`` se a linha vem da quarentena.
    """

    nsu: int
    chave_acesso: str | None
    tipo_documento: int | None
    tipo_evento: int | None
    data_hora_geracao: datetime | None
    arquivo_xml: str | None = field(repr=False)
    quarentena: bool


class FonteOracle(Protocol):
    """Leitura somente leitura, por faixa fechada de NSU, da tabela principal e da quarentena."""

    def nsus_na_faixa(self, nsu_ini: int, nsu_fim: int) -> dict[int, bool]:
        """NSU de cada linha em ``[nsu_ini, nsu_fim]`` (sem ler o CLOB) e se é da quarentena."""
        ...

    def ler_faixa(self, nsu_ini: int, nsu_fim: int) -> list[LinhaOracle]:
        """Linhas completas em ``[nsu_ini, nsu_fim]``, por NSU crescente, dos dois lados (a principal vence)."""
        ...

    def close(self) -> None:
        """Encerra a sessão."""
        ...
