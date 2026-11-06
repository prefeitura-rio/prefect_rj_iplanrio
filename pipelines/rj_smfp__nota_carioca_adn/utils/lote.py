"""Empacotamento de um lote de documentos em objeto NDJSON gzip e as linhas de índice que o apontam."""

import gzip
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from pipelines.rj_smfp__nota_carioca_adn.constants import EXTENSAO_LOTE, PARTICAO_SEM_DATA
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_decode import ItemAdn
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import DocumentoIlegivel
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import SEM_DATA, LinhaIndice, RegistroDocumento


@dataclass(frozen=True, slots=True)
class LotePronto:
    """Objeto do raw pronto para gravar.

    :param objeto: Nome no bucket: ``adn/{ambiente}/dt={AAAA-MM-DD}/lote_{nsu_ini:012d}_{nsu_fim:012d}.ndjson.gz``.
    :param conteudo: NDJSON em gzip, determinístico (mesmos registros, mesmos bytes).
    :param registros: Registros na ordem das linhas (por NSU crescente).
    """

    objeto: str
    conteudo: bytes
    registros: tuple[RegistroDocumento, ...]

    @property
    def nsu_ini(self) -> int:
        """Menor NSU do lote."""
        return self.registros[0].nsu

    @property
    def nsu_fim(self) -> int:
        """Maior NSU do lote."""
        return self.registros[-1].nsu


def registro_do_item(item: ItemAdn) -> RegistroDocumento:
    """Converte um item do ADN em registro do raw; o item que não decodifica é guardado como veio, com o motivo."""
    try:
        documento = item.decodificar()
    except DocumentoIlegivel as erro:
        return RegistroDocumento(
            nsu=item.nsu,
            chave_acesso=item.chave_acesso,
            tipo_documento=item.tipo_documento,
            tipo_evento=item.tipo_evento,
            data_hora_geracao=item.data_hora_geracao,
            xml=None,
            ilegivel=erro.motivo,
            arquivo_xml_original=item.arquivo_xml,
        )
    return RegistroDocumento(
        nsu=documento.nsu,
        chave_acesso=documento.chave_acesso,
        tipo_documento=documento.tipo_documento,
        tipo_evento=documento.tipo_evento,
        data_hora_geracao=documento.data_hora_geracao,
        xml=documento.xml,
    )


def _data_da_particao(registros: Sequence[RegistroDocumento]) -> str:
    """Data UTC do primeiro documento (por NSU) que tem data; estável entre tentativas do mesmo lote."""
    for registro in registros:
        if registro.data_hora_geracao is not None:
            return registro.data_hora_geracao.astimezone(UTC).strftime("%Y-%m-%d")
    return PARTICAO_SEM_DATA


def montar_lote(ambiente: str, registros: Sequence[RegistroDocumento]) -> LotePronto:
    """Ordena por NSU, serializa e comprime.

    :param ambiente: ``staging`` ou ``prod``.
    :param registros: Documentos do lote (ao menos um, sem NSU repetido).
    :raises ValueError: Se não houver registros ou houver NSU repetido.
    """
    if not registros:
        raise ValueError("Um lote precisa de ao menos um documento")
    ordenados = tuple(sorted(registros, key=lambda registro: registro.nsu))
    if len({registro.nsu for registro in ordenados}) != len(ordenados):
        raise ValueError("NSU repetido no mesmo lote")
    texto = "".join(registro.para_linha() + "\n" for registro in ordenados)
    objeto = (
        f"adn/{ambiente}/dt={_data_da_particao(ordenados)}/"
        f"lote_{ordenados[0].nsu:012d}_{ordenados[-1].nsu:012d}{EXTENSAO_LOTE}"
    )
    return LotePronto(objeto, gzip.compress(texto.encode("utf-8"), mtime=0), ordenados)


@dataclass(frozen=True, slots=True)
class Ingestao:
    """De onde e quando um lote entra no índice.

    :param origem: ``adn`` ou ``oracle_backfill``.
    :param ambiente: ``staging`` ou ``prod``.
    :param ingerido_em: Hora da ingestão.
    """

    origem: str
    ambiente: str
    ingerido_em: datetime


def linhas_do_indice(lote: LotePronto, gcs_uri: str, ingestao: Ingestao) -> list[LinhaIndice]:
    """Uma linha de índice por registro do lote; ``linha`` é a posição no objeto, a partir de 1.

    :param lote: Lote gravado.
    :param gcs_uri: ``gs://bucket/objeto`` do lote.
    :param ingestao: Origem, ambiente e hora da ingestão.
    """
    return [
        LinhaIndice(
            nsu=registro.nsu,
            chave_acesso=registro.chave_acesso,
            tipo_documento=registro.tipo_documento,
            tipo_evento=registro.tipo_evento,
            data_hora_geracao=registro.data_hora_geracao or SEM_DATA,
            gcs_uri=gcs_uri,
            linha=posicao,
            xml_sha256=registro.xml_sha256,
            origem=ingestao.origem,
            ambiente=ingestao.ambiente,
            ingerido_em=ingestao.ingerido_em,
        )
        for posicao, registro in enumerate(lote.registros, start=1)
    ]
