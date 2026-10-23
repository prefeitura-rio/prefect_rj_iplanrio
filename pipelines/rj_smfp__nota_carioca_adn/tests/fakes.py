"""Implementações em memória das portas de GCS, BigQuery, relógio e notificação, só para testes.

Cada fake respeita o contrato da porta real (precondição de geração no GCS, cursor que só avança, carga que acrescenta)
e permite injetar falhas em pontos exatos para provar o que o extrator faz quando o mundo falha.
"""

import itertools
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta

from pipelines.rj_smfp__nota_carioca_adn.utils.fonte import LinhaOracle
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import DocumentoIndexado, FaixaNsu, LinhaIndice

SUFIXO_LOTE = ".ndjson.gz"


class FalhaSimulada(Exception):  # noqa: N818 - exceção de teste
    """Falha injetada pelos testes."""


class ArmazenamentoMemoria:
    """Bucket em memória com geração por objeto (``criar_se_ausente`` é o ``if_generation_match=0`` do GCS)."""

    bucket = "bucket-de-teste"

    def __init__(self) -> None:
        self.objetos: dict[str, tuple[bytes, int]] = {}
        self.gravacoes_de_lote: list[str] = []
        self.falhar_no_lote: int | None = None
        self._geracao = 0

    def uri(self, objeto: str) -> str:
        return f"gs://{self.bucket}/{objeto}"

    def _nova_geracao(self) -> int:
        self._geracao += 1
        return self._geracao

    def criar_se_ausente(self, objeto: str, conteudo: bytes) -> int | None:
        if objeto.endswith(SUFIXO_LOTE):
            numero = len(self.gravacoes_de_lote) + 1
            if self.falhar_no_lote == numero:
                self.gravacoes_de_lote.append(f"falhou:{objeto}")
                raise FalhaSimulada(f"upload do lote {numero}")
            self.gravacoes_de_lote.append(objeto)
        if objeto in self.objetos:
            return None
        geracao = self._nova_geracao()
        self.objetos[objeto] = (conteudo, geracao)
        return geracao

    def ler(self, objeto: str) -> bytes:
        return self.objetos[objeto][0]

    def ler_com_geracao(self, objeto: str) -> tuple[bytes, int] | None:
        return self.objetos.get(objeto)

    def substituir_se_geracao(self, objeto: str, conteudo: bytes, geracao: int) -> int | None:
        atual = self.objetos.get(objeto)
        if atual is None or atual[1] != geracao:
            return None
        nova = self._nova_geracao()
        self.objetos[objeto] = (conteudo, nova)
        return nova

    def apagar_se_geracao(self, objeto: str, geracao: int) -> bool:
        atual = self.objetos.get(objeto)
        if atual is None or atual[1] != geracao:
            return False
        del self.objetos[objeto]
        return True

    def lotes(self) -> list[str]:
        """Nomes dos objetos de lote gravados, em ordem."""
        return sorted(nome for nome in self.objetos if nome.endswith(SUFIXO_LOTE))


class IndiceMemoria:
    """Índice e cursor em memória, com a semântica do BigQuery: carga acrescenta e o cursor nunca recua."""

    def __init__(self) -> None:
        self.linhas: list[LinhaIndice] = []
        self.cursores: dict[str, int] = {}
        self.inexistentes: dict[str, set[int]] = {}
        self.cargas = 0
        self.falhar_carga_em: int | None = None
        self.falhar_cursor: bool = False
        self.apos_gravar_cursor: Callable[[], None] | None = None

    def garantir_tabelas(self) -> None:
        return

    def ler_cursor(self, ambiente: str) -> int | None:
        return self.cursores.get(ambiente)

    def gravar_cursor(self, ambiente: str, ultimo_nsu: int, atualizado_em: datetime) -> None:  # noqa: ARG002
        if self.falhar_cursor:
            raise FalhaSimulada("gravação do cursor")
        self.cursores[ambiente] = max(self.cursores.get(ambiente, ultimo_nsu), ultimo_nsu)
        if self.apos_gravar_cursor is not None:
            self.apos_gravar_cursor()

    def nsus_indexados(self, ambiente: str, nsu_ini: int, nsu_fim: int) -> set[int]:
        return {linha.nsu for linha in self.linhas if linha.ambiente == ambiente and nsu_ini <= linha.nsu <= nsu_fim}

    def documentos_na_faixa(self, ambiente: str, nsu_ini: int, nsu_fim: int) -> list[DocumentoIndexado]:
        return [
            DocumentoIndexado(linha.nsu, linha.xml_sha256, linha.origem, linha.gcs_uri, linha.linha)
            for linha in self.linhas
            if linha.ambiente == ambiente and nsu_ini <= linha.nsu <= nsu_fim
        ]

    def carregar_documentos(self, linhas: Sequence[LinhaIndice]) -> None:
        self.cargas += 1
        if self.falhar_carga_em == self.cargas:
            raise FalhaSimulada(f"carga {self.cargas} do índice")
        self.linhas.extend(linhas)

    def carregar_inexistentes(self, ambiente: str, nsus: Sequence[int], verificado_em: datetime) -> None:  # noqa: ARG002
        self.inexistentes.setdefault(ambiente, set()).update(nsus)

    def lacunas(self, ambiente: str, limite: int) -> list[FaixaNsu]:
        conhecidos = sorted(
            {linha.nsu for linha in self.linhas if linha.ambiente == ambiente} | self.inexistentes.get(ambiente, set())
        )
        faixas = [
            FaixaNsu(anterior + 1, proximo - 1)
            for anterior, proximo in itertools.pairwise(conhecidos)
            if proximo - anterior > 1
        ]
        return faixas[:limite]

    def nsus(self, ambiente: str = "staging") -> list[int]:
        """NSUs do índice do ambiente, na ordem em que foram carregados."""
        return [linha.nsu for linha in self.linhas if linha.ambiente == ambiente]


class RelogioFalso:
    """Relógio manual: ``dormir`` só avança o tempo e o registra."""

    def __init__(self, inicio: datetime | None = None) -> None:
        self._agora = inicio or datetime(2026, 10, 23, 12, 0, 0, tzinfo=UTC)
        self.esperas: list[float] = []

    def monotonic(self) -> float:
        return self._agora.timestamp()

    def agora_utc(self) -> datetime:
        return self._agora

    def dormir(self, segundos: float) -> None:
        self.esperas.append(segundos)
        self._agora += timedelta(seconds=segundos)

    def avancar(self, segundos: float) -> None:
        self._agora += timedelta(seconds=segundos)


class NotificadorGravador:
    """Registra o que o extrator notificou, na ordem."""

    def __init__(self) -> None:
        self.eventos: list[tuple[str, object]] = []

    def iniciar(self, cursor: int) -> None:
        self.eventos.append(("iniciar", cursor))

    def progresso(self, resultado: object) -> None:
        self.eventos.append(("progresso", resultado))

    def concluir(self, resultado: object) -> None:
        self.eventos.append(("concluir", resultado))

    def falhar(self, erro: BaseException) -> None:
        self.eventos.append(("falhar", erro))


class FonteMemoria:
    """Fonte do histórico em memória, com o contrato do Oracle: faixa fechada, ordem por NSU; conta as leituras."""

    def __init__(self, linhas: Sequence[LinhaOracle]) -> None:
        self.linhas = {linha.nsu: linha for linha in linhas}
        self.leituras: list[tuple[int, int]] = []
        self.consultas_de_nsus: list[tuple[int, int]] = []

    def nsus_na_faixa(self, nsu_ini: int, nsu_fim: int) -> dict[int, bool]:
        self.consultas_de_nsus.append((nsu_ini, nsu_fim))
        return {nsu: linha.quarentena for nsu, linha in sorted(self.linhas.items()) if nsu_ini <= nsu <= nsu_fim}

    def ler_faixa(self, nsu_ini: int, nsu_fim: int) -> list[LinhaOracle]:
        self.leituras.append((nsu_ini, nsu_fim))
        return [linha for nsu, linha in sorted(self.linhas.items()) if nsu_ini <= nsu <= nsu_fim]

    def close(self) -> None:
        return
