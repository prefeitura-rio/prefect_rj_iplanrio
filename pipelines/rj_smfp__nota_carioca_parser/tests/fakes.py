"""Dublês do BigQuery e do GCS para os testes: em memória, com o mesmo contrato dos adaptadores reais.

``ArmazemLocal`` imita o que importa do BigQuery: a carga de um ``(tabela, lote_id)`` repetido não duplica (id do load
job determinístico), cada linha tem exatamente as colunas do schema JSON do pacote e uma tabela pode falhar a carga.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pipelines.rj_smfp__nota_carioca_parser.utils.entradas import EntradaIndice
from pipelines.rj_smfp__nota_carioca_parser.utils.tabelas import colunas

INSTANTE = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
GERACAO = datetime(2026, 2, 10, 13, 15, 30, tzinfo=UTC)


class FalhaDeCarga(RuntimeError):
    """Falha simulada do load job."""


@dataclass
class ArmazemLocal:
    """Índice, marca, quarentena e tabelas raw em memória."""

    entradas: list[EntradaIndice] = field(default_factory=list)
    tabelas: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    marcas: list[tuple[str, int]] = field(default_factory=list)
    cargas: list[tuple[str, str]] = field(default_factory=list)
    falhas: dict[str, int] = field(default_factory=dict)

    def ultima_marca(self) -> int:
        return max((nsu for _, nsu in self.marcas), default=0)

    def listar_indice(self, apos_nsu: int, limite: int) -> list[EntradaIndice]:
        return sorted((e for e in self.entradas if e.nsu > apos_nsu), key=lambda e: e.nsu)[:limite]

    def entradas_por_nsu(self, nsus: Sequence[int]) -> list[EntradaIndice]:
        return [e for e in self.entradas if e.nsu in set(nsus)]

    def buscar_nfse_por_chave(self, chave_acesso: str) -> EntradaIndice | None:
        candidatas = [e for e in self.entradas if e.chave_acesso == chave_acesso and str(e.tipo_documento) == "NFSE"]
        return min(candidatas, key=lambda e: e.nsu, default=None)

    def listar_quarentena_pendente(self, versao_parser: str, limite: int) -> list[int]:
        ultima: dict[int, dict[str, object]] = {}
        for linha in sorted(self.tabelas.get("quarentena", []), key=lambda r: str(r["avaliado_em"])):
            ultima[int(linha["nsu"])] = linha
        pendentes = [
            linha
            for linha in ultima.values()
            if linha["prioridade"] is not None and linha["versao_parser"] != versao_parser
        ]
        return [int(p["nsu"]) for p in sorted(pendentes, key=lambda p: (p["prioridade"], p["nsu"]))][:limite]

    def carregar(self, tabela: str, lote_id: str, linhas: Sequence[Mapping[str, object]]) -> None:
        if self.falhas.get(tabela, 0) > 0:
            self.falhas[tabela] -= 1
            raise FalhaDeCarga(f"load de {tabela} falhou")
        esperadas = set(colunas(tabela))
        for linha in linhas:
            assert set(linha) == esperadas, (tabela, set(linha) ^ esperadas)
        if (tabela, lote_id) in self.cargas:
            return
        self.cargas.append((tabela, lote_id))
        self.tabelas.setdefault(tabela, []).extend(dict(linha) for linha in linhas)

    def gravar_marca(self, versao_parser: str, ultimo_nsu: int) -> None:
        self.marcas.append((versao_parser, ultimo_nsu))

    def linhas(self, tabela: str) -> list[dict[str, object]]:
        return self.tabelas.get(tabela, [])

    def nsus(self, tabela: str) -> list[object]:
        return [linha["NSU"] for linha in self.linhas(tabela)]


@dataclass
class LeitorLocal:
    """Objetos NDJSON do extrator em memória; ``leituras`` conta quantas vezes cada objeto foi aberto."""

    objetos: dict[str, dict[int, Mapping[str, object]]] = field(default_factory=dict)
    leituras: dict[str, int] = field(default_factory=dict)

    def ler_linhas(self, gcs_uri: str) -> dict[int, Mapping[str, object]]:
        self.leituras[gcs_uri] = self.leituras.get(gcs_uri, 0) + 1
        return self.objetos[gcs_uri]


def montar(
    documentos: Sequence[tuple[int, str, str | None, str | None]],
    por_objeto: int = 3,
    sem_linha: Sequence[int] = (),
) -> tuple[ArmazemLocal, LeitorLocal]:
    """Cria o índice e os objetos para ``(nsu, tipo_documento, xml, chave_acesso)``, ``por_objeto`` por arquivo."""
    armazem, leitor = ArmazemLocal(), LeitorLocal()
    for posicao, (nsu, tipo, xml, chave) in enumerate(documentos):
        uri = f"gs://bucket/adn/producao/dt=2026-02-10/lote_{posicao // por_objeto:04d}.ndjson.gz"
        armazem.entradas.append(
            EntradaIndice(
                nsu=nsu,
                chave_acesso=chave,
                tipo_documento=tipo,
                tipo_evento=None,
                data_hora_geracao=GERACAO,
                gcs_uri=uri,
                linha=posicao % por_objeto,
                ambiente="producao",
            )
        )
        if nsu not in sem_linha:
            linha = {"nsu": nsu, "chave_acesso": chave, "tipo_documento": tipo, "xml": xml}
            leitor.objetos.setdefault(uri, {})[nsu] = linha
        else:
            leitor.objetos.setdefault(uri, {})
    return armazem, leitor
