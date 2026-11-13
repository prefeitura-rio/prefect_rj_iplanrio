"""Conferência byte a byte: o XML do ADN direto (sanitizado) contra o ``ARQUIVO_XML`` do Oracle, por NSU.

Para cada NSU de origem ``adn`` do índice, compara o sha256 de ``sanitizar(xml_adn)`` com o sha256 do ``ARQUIVO_XML`` do
Oracle (na quarentena a app guarda o XML sem sanitizar, então ali compara o XML original). Só lê dentro da janela
horária e no máximo ``linhas_por_minuto``, como o backfill.
"""

import gzip
import json
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Literal

from pipelines.rj_smfp__nota_carioca_adn.constants import ORIGEM_ADN
from pipelines.rj_smfp__nota_carioca_adn.utils.armazenamento import Armazenamento
from pipelines.rj_smfp__nota_carioca_adn.utils.fonte import LinhaOracle
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import DocumentoIndexado, sha256_do_xml
from pipelines.rj_smfp__nota_carioca_adn.utils.percurso import ConfigBackfill, Percurso, ServicosBackfill
from pipelines.rj_smfp__nota_carioca_adn.utils.sanitizar import sanitizar_xml

OBJETOS_EM_MEMORIA = 4


@dataclass(frozen=True, slots=True)
class RelatorioConferencia:
    """Resultado de ``conferir`` sobre ``[nsu_inicio, nsu_fim]``.

    :param iguais: NSUs do ADN direto cujo XML confere byte a byte com o do Oracle.
    :param diferentes: NSUs do ADN direto cujo XML difere (inclusive o ilegível) do do Oracle.
    :param ausentes_oracle: NSUs do ADN direto que o Oracle não tem.
    :param ausentes_nuvem: NSUs do Oracle sem cópia de origem ``adn`` no índice.
    :param motivo_fim: ``concluido`` ou ``fora_da_janela``.
    """

    nsu_inicio: int
    nsu_fim: int
    iguais: int
    diferentes: tuple[int, ...]
    ausentes_oracle: tuple[int, ...]
    ausentes_nuvem: tuple[int, ...]
    motivo_fim: Literal["concluido", "fora_da_janela"]

    def resumo(self, amostra: int = 50) -> dict[str, object]:
        """Contagens e, de cada lista, os primeiros ``amostra`` NSUs (para log e mensagem)."""
        return {
            "faixa": [self.nsu_inicio, self.nsu_fim],
            "iguais": self.iguais,
            "diferentes": len(self.diferentes),
            "ausentes_oracle": len(self.ausentes_oracle),
            "ausentes_nuvem": len(self.ausentes_nuvem),
            "amostra_diferentes": list(self.diferentes[:amostra]),
            "motivo_fim": self.motivo_fim,
        }


@dataclass
class _RawEmMemoria:
    """Lê linhas do raw por (objeto, número da linha), mantendo só alguns objetos descompactados na memória."""

    armazenamento: Armazenamento
    _objetos: OrderedDict[str, list[str]] = field(default_factory=OrderedDict)

    def xml(self, documento: DocumentoIndexado) -> str | None:
        """XML original do documento no raw (``None`` se foi gravado ilegível)."""
        prefixo = f"gs://{self.armazenamento.bucket}/"
        if not documento.gcs_uri.startswith(prefixo):
            raise ValueError(f"URI fora do bucket do raw: {documento.gcs_uri}")
        objeto = documento.gcs_uri.removeprefix(prefixo)
        if objeto not in self._objetos:
            self._objetos[objeto] = gzip.decompress(self.armazenamento.ler(objeto)).decode("utf-8").splitlines()
            while len(self._objetos) > OBJETOS_EM_MEMORIA:
                self._objetos.popitem(last=False)
        registro = json.loads(self._objetos[objeto][documento.linha - 1])
        if registro["nsu"] != documento.nsu:
            raise ValueError(f"O índice aponta o NSU {documento.nsu} para {documento.gcs_uri}:{documento.linha}")
        return registro["xml"]


def conferir(config: ConfigBackfill, servicos: ServicosBackfill) -> RelatorioConferencia:
    """Compara byte a byte, por NSU, o XML do ADN direto (sanitizado) com o do Oracle.

    :param config: Faixa, janela e ritmo (a conferência também só lê dentro da janela).
    :param servicos: Fonte, bucket, índice e relógio.
    :returns: Relatório com ``iguais``, ``diferentes``, ``ausentes_oracle`` e ``ausentes_nuvem``.
    """
    percurso = Percurso(config, servicos.relogio)
    raw = _RawEmMemoria(servicos.armazenamento)
    iguais = 0
    diferentes: list[int] = []
    ausentes_oracle: list[int] = []
    ausentes_nuvem: list[int] = []
    for faixa in percurso.faixas_liberadas():
        do_adn = {
            documento.nsu: documento
            for documento in servicos.indice.documentos_na_faixa(config.ambiente, faixa.ini, faixa.fim)
            if documento.origem == ORIGEM_ADN
        }
        no_oracle = servicos.fonte.nsus_na_faixa(faixa.ini, faixa.fim)
        ausentes_nuvem += sorted(set(no_oracle) - set(do_adn))
        ausentes_oracle += sorted(set(do_adn) - set(no_oracle))
        comuns = sorted(set(do_adn) & set(no_oracle))
        if not comuns:
            continue
        linhas = {linha.nsu: linha for linha in servicos.fonte.ler_faixa(faixa.ini, faixa.fim)}
        percurso.linhas_da_faixa = len(linhas)
        for nsu in comuns:
            if _confere(raw.xml(do_adn[nsu]), linhas[nsu]):
                iguais += 1
            else:
                diferentes.append(nsu)
    return RelatorioConferencia(
        config.nsu_inicio,
        config.nsu_fim,
        iguais,
        tuple(diferentes),
        tuple(ausentes_oracle),
        tuple(ausentes_nuvem),
        percurso.motivo_fim,
    )


def _confere(xml_adn: str | None, linha: LinhaOracle) -> bool:
    if xml_adn is None or linha.arquivo_xml is None:
        return False
    esperado = xml_adn if linha.quarentena else sanitizar_xml(xml_adn)
    return sha256_do_xml(esperado) == sha256_do_xml(linha.arquivo_xml)
