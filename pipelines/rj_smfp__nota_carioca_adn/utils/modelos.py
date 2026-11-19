"""Tipos do raw e do índice: as linhas do objeto NDJSON, as do BigQuery e o resultado de um run."""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from pipelines.rj_smfp__nota_carioca_adn.constants import (
    INTERVALO_ENTRE_CHAMADAS_S,
    LACUNAS_POR_OBJETO,
    MAX_LACUNAS_POR_RUN,
    VALIDADE_TRAVA_S,
)

SEM_DATA = datetime(1970, 1, 1, tzinfo=UTC)
"""Valor da coluna particionada ``data_hora_geracao`` do índice para um documento sem data."""


def sha256_do_xml(xml: str | None) -> str | None:
    """sha256 (hexadecimal) do XML em UTF-8, ou ``None`` se não há XML."""
    return None if xml is None else hashlib.sha256(xml.encode("utf-8")).hexdigest()


def _iso_utc(momento: datetime | None) -> str | None:
    return None if momento is None else momento.astimezone(UTC).isoformat()


@dataclass(frozen=True, slots=True)
class FaixaNsu:
    """Intervalo fechado de NSUs ``[ini, fim]``."""

    ini: int
    fim: int


@dataclass(frozen=True, slots=True)
class RegistroDocumento:
    """Um documento para o raw: vira uma linha do objeto NDJSON e uma do índice.

    :param nsu: NSU de distribuição.
    :param chave_acesso: Chave de acesso.
    :param tipo_documento: Tipo do documento (``NFSE``, ``EVENTO``...).
    :param tipo_evento: Tipo do evento; ``None`` quando o ADN não envia.
    :param data_hora_geracao: Momento de geração com fuso.
    :param xml: XML original, sem sanitizar; ``None`` quando o item é ilegível.
    :param ilegivel: Motivo, quando o ``ArquivoXml`` não pôde ser decodificado.
    :param arquivo_xml_original: ``ArquivoXml`` como o ADN enviou (base64 de gzip), guardado só quando ilegível.
    :param quarentena_oracle: Só no backfill: ``True`` se o documento veio da quarentena do Oracle.
    """

    nsu: int
    chave_acesso: str | None
    tipo_documento: str | None
    tipo_evento: str | None
    data_hora_geracao: datetime | None
    xml: str | None
    ilegivel: str | None = None
    arquivo_xml_original: str | None = field(default=None, repr=False)
    quarentena_oracle: bool | None = None

    @property
    def xml_sha256(self) -> str | None:
        """sha256 do XML guardado."""
        return sha256_do_xml(self.xml)

    def para_linha(self) -> str:
        """Linha NDJSON: ``{nsu, chave_acesso, tipo_documento, tipo_evento, data_hora_geracao, xml_sha256, xml}`` mais,
        só quando existem, ``ilegivel``, ``arquivo_xml_original`` e ``quarentena_oracle``."""
        campos: dict[str, object] = {
            "nsu": self.nsu,
            "chave_acesso": self.chave_acesso,
            "tipo_documento": self.tipo_documento,
            "tipo_evento": self.tipo_evento,
            "data_hora_geracao": _iso_utc(self.data_hora_geracao),
            "xml_sha256": self.xml_sha256,
            "xml": self.xml,
        }
        if self.ilegivel is not None:
            campos["ilegivel"] = self.ilegivel
            campos["arquivo_xml_original"] = self.arquivo_xml_original
        if self.quarentena_oracle is not None:
            campos["quarentena_oracle"] = self.quarentena_oracle
        return json.dumps(campos, ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class LinhaIndice:
    """Linha de ``documentos`` no dataset do ambiente; ``linha`` é a posição no objeto, a partir de 1."""

    nsu: int
    chave_acesso: str | None
    tipo_documento: str | None
    tipo_evento: str | None
    data_hora_geracao: datetime
    gcs_uri: str
    linha: int
    xml_sha256: str | None
    origem: str
    ambiente: str
    ingerido_em: datetime

    def para_json(self) -> str:
        """Linha NDJSON para o load job do BigQuery."""
        return json.dumps(
            {
                "nsu": self.nsu,
                "chave_acesso": self.chave_acesso,
                "tipo_documento": self.tipo_documento,
                "tipo_evento": self.tipo_evento,
                "data_hora_geracao": _iso_utc(self.data_hora_geracao),
                "gcs_uri": self.gcs_uri,
                "linha": self.linha,
                "xml_sha256": self.xml_sha256,
                "origem": self.origem,
                "ambiente": self.ambiente,
                "ingerido_em": _iso_utc(self.ingerido_em),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )


@dataclass(frozen=True, slots=True)
class DocumentoIndexado:
    """O que o índice sabe de um NSU: hash do XML guardado, origem e onde ele está no raw (objeto e linha)."""

    nsu: int
    xml_sha256: str | None
    origem: str
    gcs_uri: str
    linha: int


@dataclass(frozen=True, slots=True)
class ConfigExtracao:
    """Parâmetros de um run do extrator.

    :param ambiente: ``staging`` (produção restrita) ou ``prod``.
    :param intervalo_entre_chamadas_s: Intervalo mínimo entre duas chamadas ao ADN.
    :param max_lacunas_por_run: Teto de NSUs de lacuna buscados num run.
    :param nsu_inicial: Cursor do primeiro run de um ambiente (sem cursor gravado e sem ele, o run se recusa).
    :param max_lotes_por_run: Para o run depois de tantos lotes; ``None`` segue até o fim de dados.
    :param run_id: Identificação do dono da trava.
    :param lacunas_por_objeto: Documentos de lacuna por objeto gravado.
    :param validade_trava_s: Validade da trava do ambiente, renovada a cada chamada.
    """

    ambiente: str
    intervalo_entre_chamadas_s: float = INTERVALO_ENTRE_CHAMADAS_S
    max_lacunas_por_run: int = MAX_LACUNAS_POR_RUN
    nsu_inicial: int | None = None
    max_lotes_por_run: int | None = None
    run_id: str = "local"
    lacunas_por_objeto: int = LACUNAS_POR_OBJETO
    validade_trava_s: float = VALIDADE_TRAVA_S


@dataclass(frozen=True, slots=True)
class ResultadoExtracao:
    """O que um run fez.

    :param cursor_inicial: Cursor ao começar.
    :param cursor_final: Cursor ao terminar.
    :param lotes: Lotes novos gravados (a partir do cursor).
    :param documentos: Documentos gravados, inclusive os de lacunas.
    :param ilegiveis: Documentos gravados sem XML por falha de decodificação.
    :param lacunas_preenchidas: NSUs de lacuna encontrados e gravados.
    :param lacunas_inexistentes: NSUs de lacuna que o ADN não tem, registrados para não serem buscados de novo.
    :param motivo_fim: ``sem_documentos``, ``limite_de_lotes`` ou ``em_andamento`` (resultado parcial).
    """

    cursor_inicial: int
    cursor_final: int
    lotes: int
    documentos: int
    ilegiveis: int
    lacunas_preenchidas: int
    lacunas_inexistentes: int
    motivo_fim: Literal["sem_documentos", "limite_de_lotes", "em_andamento"]
