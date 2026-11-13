"""Tipos de documento do ADN e o documento lido do índice/GCS."""

from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum

from pipelines.rj_smfp__nota_carioca_parser.utils.erros import TipoDocumentoDesconhecido


class TipoDocumento(IntEnum):
    """``eTipoDocumento`` do .NET (``DFe.vb``): o número é o gravado em ``TIPO_DOCUMENTO`` no Oracle."""

    NFSE = 1
    EVENTO = 2
    CNC = 3

    @classmethod
    def de_valor(cls, valor: object) -> "TipoDocumento":
        """Converte o valor do ADN/índice (número, ``NFSE``/``EVENTO``/``CNC`` em qualquer caixa).

        :raises TipoDocumentoDesconhecido: Se o valor não for um dos três tipos (``TODOS`` não é um documento).
        """
        if isinstance(valor, cls):
            return valor
        if isinstance(valor, bool):
            raise TipoDocumentoDesconhecido(valor)
        if isinstance(valor, int | float) and float(valor) in {1.0, 2.0, 3.0}:
            return cls(int(valor))
        if isinstance(valor, str):
            texto = valor.strip().upper()
            if texto.isdigit() and int(texto) in {1, 2, 3}:
                return cls(int(texto))
            if texto in cls.__members__:
                return cls[texto]
        raise TipoDocumentoDesconhecido(valor)


@dataclass(frozen=True, slots=True)
class Documento:
    """Um documento do ADN: uma linha do NDJSON, já casada com o índice.

    :param nsu: Número sequencial único do ADN.
    :param xml: XML original decodificado, sem sanitizar (o parser sanitiza).
    :param tipo_documento: NFSE, EVENTO ou CNC, ou ``None`` se o índice trouxe um valor desconhecido.
    :param chave_acesso: Chave de acesso da NFS-e (50 dígitos), se o ADN informou.
    :param tipo_evento: Descrição ou número do tipo de evento do ADN, como veio.
    :param data_hora_geracao: ``DataHoraGeracao`` do ADN.
    """

    nsu: int
    xml: str | None
    tipo_documento: TipoDocumento | None
    chave_acesso: str | None = None
    tipo_evento: str | None = None
    data_hora_geracao: datetime | None = None
