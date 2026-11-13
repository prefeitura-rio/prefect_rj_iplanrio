"""Item do ``LoteDFe`` do ADN e sua decodificação: base64, depois gunzip, depois UTF-8."""

import base64
import binascii
import gzip
import zlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import ContratoAdnViolado, DocumentoIlegivel

FUSO_PADRAO = ZoneInfo("America/Sao_Paulo")
"""Fuso aplicado às datas que o ADN enviar sem deslocamento."""


@dataclass(frozen=True, slots=True)
class DocumentoAdn:
    """Documento fiscal já decodificado.

    :param nsu: NSU de distribuição.
    :param chave_acesso: Chave de acesso, como veio.
    :param tipo_documento: ``NFSE``, ``EVENTO``..., como veio.
    :param tipo_evento: Tipo do evento, ou ``None`` quando o ADN não o envia (emissão).
    :param data_hora_geracao: Momento de geração, com fuso.
    :param xml: XML original decodificado, sem nenhuma sanitização (um BOM, por exemplo, permanece).
    """

    nsu: int
    chave_acesso: str | None
    tipo_documento: str | None
    tipo_evento: str | None
    data_hora_geracao: datetime
    xml: str


@dataclass(frozen=True, slots=True)
class ItemAdn:
    """Item do lote como o ADN enviou; ``decodificar`` produz o documento.

    :param nsu: NSU de distribuição.
    :param chave_acesso: Chave de acesso.
    :param tipo_documento: Tipo do documento.
    :param tipo_evento: Tipo do evento, se enviado.
    :param data_hora_geracao: Data de geração com fuso, ou ``None`` se ausente ou inválida.
    :param arquivo_xml: ``ArquivoXml`` original (base64 de gzip), sem alteração.
    """

    nsu: int
    chave_acesso: str | None
    tipo_documento: str | None
    tipo_evento: str | None
    data_hora_geracao: datetime | None
    arquivo_xml: str | None = field(repr=False)

    @classmethod
    def do_json(cls, bruto: Mapping[str, object]) -> "ItemAdn":
        """Lê um ``DistribuicaoNSU`` do swagger.

        :raises ContratoAdnViolado: Se o item não tiver NSU inteiro.
        """
        nsu = bruto.get("NSU")
        if isinstance(nsu, bool) or not isinstance(nsu, int):
            raise ContratoAdnViolado(f"Item do lote sem NSU inteiro: {nsu!r}")
        return cls(
            nsu=nsu,
            chave_acesso=_texto(bruto.get("ChaveAcesso")),
            tipo_documento=_texto(bruto.get("TipoDocumento")),
            tipo_evento=_texto(bruto.get("TipoEvento")),
            data_hora_geracao=_data(bruto.get("DataHoraGeracao")),
            arquivo_xml=_texto(bruto.get("ArquivoXml")),
        )

    def decodificar(self) -> DocumentoAdn:
        """Decodifica o ``ArquivoXml`` (base64 → gunzip → UTF-8 estrito).

        :raises DocumentoIlegivel: Se faltar o XML ou a data, ou se alguma etapa da decodificação falhar.
        """
        if self.data_hora_geracao is None:
            raise DocumentoIlegivel(self, "DataHoraGeracao ausente ou inválida")
        if not self.arquivo_xml:
            raise DocumentoIlegivel(self, "ArquivoXml ausente")
        try:
            comprimido = base64.b64decode(self.arquivo_xml, validate=True)
        except binascii.Error as erro:
            raise DocumentoIlegivel(self, "ArquivoXml não é base64 válido") from erro
        try:
            descomprimido = gzip.decompress(comprimido)
        except (OSError, EOFError, zlib.error) as erro:
            raise DocumentoIlegivel(self, "ArquivoXml não é gzip válido") from erro
        try:
            xml = descomprimido.decode("utf-8")
        except UnicodeDecodeError as erro:
            raise DocumentoIlegivel(self, "ArquivoXml não é UTF-8 válido") from erro
        return DocumentoAdn(
            self.nsu, self.chave_acesso, self.tipo_documento, self.tipo_evento, self.data_hora_geracao, xml
        )


def _texto(valor: object) -> str | None:
    return valor if isinstance(valor, str) else None


def _data(valor: object) -> datetime | None:
    if not isinstance(valor, str):
        return None
    try:
        momento = datetime.fromisoformat(valor)
    except ValueError:
        return None
    return momento if momento.tzinfo is not None else momento.replace(tzinfo=FUSO_PADRAO)
