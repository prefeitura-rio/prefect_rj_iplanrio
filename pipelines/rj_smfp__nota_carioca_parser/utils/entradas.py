"""Entrada do parser: linhas do índice ``documentos`` e portas para o BigQuery e o GCS.

O índice e os objetos são escritos pelo extrator do ADN (outra trilha); aqui só se lê o contrato:
``raw_nfse_nacional.documentos(nsu, chave_acesso, tipo_documento, tipo_evento, data_hora_geracao, gcs_uri, linha,
xml_sha256, origem, ambiente, ingerido_em)`` e objetos NDJSON.gz com linhas
``{nsu, chave_acesso, tipo_documento, tipo_evento, data_hora_geracao, xml_sha256, xml}``.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class EntradaIndice:
    """Uma linha do índice ``documentos``."""

    nsu: int
    chave_acesso: str | None
    tipo_documento: str | int | None
    tipo_evento: str | int | None
    data_hora_geracao: datetime | None
    gcs_uri: str
    linha: int | None = None
    xml_sha256: str | None = None
    origem: str | None = None
    ambiente: str | None = None


class ArmazemRaw(Protocol):
    """O lado BigQuery do parser: índice, marca, quarentena e carga das tabelas raw."""

    def ultima_marca(self) -> int:
        """Maior ``ultimo_nsu`` gravado, de qualquer versão do parser (0 se nunca rodou)."""

    def listar_indice(self, apos_nsu: int, limite: int) -> Sequence[EntradaIndice]:
        """Até ``limite`` entradas do índice com NSU acima de ``apos_nsu``, em ordem de NSU."""

    def entradas_por_nsu(self, nsus: Sequence[int]) -> Sequence[EntradaIndice]:
        """Entradas do índice para os NSUs dados."""

    def buscar_nfse_por_chave(self, chave_acesso: str) -> EntradaIndice | None:
        """A entrada do índice da NFS-e com a chave de acesso (a de menor NSU), se houver."""

    def listar_quarentena_pendente(self, versao_parser: str, limite: int) -> Sequence[int]:
        """NSUs em quarentena cuja última avaliação foi de outra versão do parser, mais urgentes primeiro."""

    def carregar(self, tabela: str, lote_id: str, linhas: Sequence[Mapping[str, object]]) -> None:
        """Carrega as linhas por load job; repetir o mesmo ``lote_id`` da tabela não duplica."""

    def gravar_marca(self, versao_parser: str, ultimo_nsu: int) -> None:
        """Registra que tudo até ``ultimo_nsu`` foi processado e carregado."""


class LeitorObjetos(Protocol):
    """O lado GCS do parser: lê um objeto NDJSON.gz do extrator."""

    def ler_linhas(self, gcs_uri: str) -> Mapping[int, Mapping[str, object]]:
        """Linhas do objeto, indexadas por NSU."""
