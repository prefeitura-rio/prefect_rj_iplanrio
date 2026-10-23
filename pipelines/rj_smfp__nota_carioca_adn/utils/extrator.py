"""Extrator do ADN: laço sequencial, um consumidor por ambiente, que grava raw imutável e índice e só então o cursor.

Garantias: (1) uma chamada ao ADN por vez, com intervalo mínimo; (2) nenhum objeto é sobrescrito; (3) o cursor só
avança depois de o objeto do lote e as linhas do índice estarem gravados, então qualquer falha no meio refaz o lote
sem perder nem duplicar; (4) um run por vez por ambiente (trava por objeto de GCS com geração).
"""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from itertools import chain, islice
from typing import Protocol

from pipelines.rj_smfp__nota_carioca_adn.constants import ORIGEM_ADN
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_decode import ItemAdn
from pipelines.rj_smfp__nota_carioca_adn.utils.armazenamento import Armazenamento
from pipelines.rj_smfp__nota_carioca_adn.utils.indice import Indice
from pipelines.rj_smfp__nota_carioca_adn.utils.lote import registro_do_item
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ConfigExtracao, RegistroDocumento, ResultadoExtracao
from pipelines.rj_smfp__nota_carioca_adn.utils.publicacao import Publicador
from pipelines.rj_smfp__nota_carioca_adn.utils.relogio import Relogio
from pipelines.rj_smfp__nota_carioca_adn.utils.trava import Trava
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

INEXISTENTES_POR_CARGA = 1000


class CursorAusente(Exception):  # noqa: N818 - nome do domínio, como as demais exceções do pacote
    """Não há cursor gravado para o ambiente e o run não recebeu ``nsu_inicial``."""


class ClienteAdn(Protocol):
    """O que o extrator usa do cliente do ADN."""

    def buscar_lote_bruto(self, cursor: int) -> tuple[ItemAdn, ...]: ...

    def buscar_um_bruto(self, nsu: int) -> ItemAdn | None: ...

    def close(self) -> None: ...


class Notificador(Protocol):
    """Avisos do run (Discord em produção)."""

    def iniciar(self, cursor: int) -> None: ...

    def progresso(self, resultado: ResultadoExtracao) -> None: ...

    def concluir(self, resultado: ResultadoExtracao) -> None: ...

    def falhar(self, erro: BaseException) -> None: ...


@dataclass(frozen=True, slots=True)
class Servicos:
    """As portas do extrator: ADN, bucket, índice, notificação e relógio."""

    cliente: ClienteAdn
    armazenamento: Armazenamento
    indice: Indice
    notificador: Notificador
    relogio: Relogio


class _Run:
    """Estado de um run: contadores, cursor e o ritmo entre chamadas."""

    def __init__(self, config: ConfigExtracao, servicos: Servicos, trava: Trava) -> None:
        self._config = config
        self._servicos = servicos
        self._trava = trava
        self._publicador = Publicador(
            servicos.armazenamento, servicos.indice, config.ambiente, ORIGEM_ADN, servicos.relogio
        )
        self._ultima_chamada: float | None = None
        self._cursor = 0
        self._parcial = ResultadoExtracao(0, 0, 0, 0, 0, 0, 0, "em_andamento")

    def executar(self) -> ResultadoExtracao:
        self._servicos.indice.garantir_tabelas()
        self._cursor = self._cursor_inicial()
        self._parcial = replace(self._parcial, cursor_inicial=self._cursor, cursor_final=self._cursor)
        self._servicos.notificador.iniciar(self._cursor)
        motivo = self._percorrer_lotes()
        if motivo == "sem_documentos":
            self._preencher_lacunas()
        return replace(self._parcial, motivo_fim=motivo)

    def _cursor_inicial(self) -> int:
        ambiente = self._config.ambiente
        cursor = self._servicos.indice.ler_cursor(ambiente)
        if cursor is not None:
            if self._config.nsu_inicial not in (None, cursor):
                logger.info("nsu_inicial=%s ignorado: o cursor gravado é %d", self._config.nsu_inicial, cursor)
            return cursor
        if self._config.nsu_inicial is None:
            raise CursorAusente(f"Sem cursor para {ambiente}: informe nsu_inicial no primeiro run")
        self._servicos.indice.gravar_cursor(ambiente, self._config.nsu_inicial, self._servicos.relogio.agora_utc())
        return self._config.nsu_inicial

    def _esperar_ritmo(self) -> None:
        """Antes de cada chamada ao ADN: garante o intervalo mínimo desde a anterior e renova a trava."""
        relogio = self._servicos.relogio
        if self._ultima_chamada is not None:
            falta = self._config.intervalo_entre_chamadas_s - (relogio.monotonic() - self._ultima_chamada)
            if falta > 0:
                relogio.dormir(falta)
        self._ultima_chamada = relogio.monotonic()
        self._trava.renovar()

    def _contar(self, registros: Sequence[RegistroDocumento], **somar: int) -> None:
        parcial = self._parcial
        self._parcial = replace(
            parcial,
            documentos=parcial.documentos + len(registros),
            ilegiveis=parcial.ilegiveis + sum(1 for registro in registros if registro.ilegivel is not None),
            **{campo: getattr(parcial, campo) + valor for campo, valor in somar.items()},
        )

    def _percorrer_lotes(self) -> str:
        servicos, limite = self._servicos, self._config.max_lotes_por_run
        while limite is None or self._parcial.lotes < limite:
            self._esperar_ritmo()
            itens = servicos.cliente.buscar_lote_bruto(self._cursor)
            if not itens:
                return "sem_documentos"
            registros = [registro_do_item(item) for item in itens]
            self._publicador.publicar(registros)
            self._cursor = itens[-1].nsu
            servicos.indice.gravar_cursor(self._config.ambiente, self._cursor, servicos.relogio.agora_utc())
            self._contar(registros, lotes=1)
            self._parcial = replace(self._parcial, cursor_final=self._cursor)
            servicos.notificador.progresso(self._parcial)
        return "limite_de_lotes"

    def _nsus_em_lacuna(self) -> Iterator[int]:
        faixas = self._servicos.indice.lacunas(self._config.ambiente, self._config.max_lacunas_por_run)
        return islice(chain.from_iterable(range(f.ini, f.fim + 1) for f in faixas), self._config.max_lacunas_por_run)

    def _preencher_lacunas(self) -> None:
        """Busca cada NSU ausente do índice com ``buscar_um``, até ``max_lacunas_por_run`` por run."""
        encontrados: list[RegistroDocumento] = []
        inexistentes: list[int] = []
        for nsu in self._nsus_em_lacuna():
            self._esperar_ritmo()
            item = self._servicos.cliente.buscar_um_bruto(nsu)
            if item is None:
                inexistentes.append(nsu)
            else:
                encontrados.append(registro_do_item(item))
            if len(encontrados) >= self._config.lacunas_por_objeto:
                encontrados = self._gravar_lacunas(encontrados)
            if len(inexistentes) >= INEXISTENTES_POR_CARGA:
                inexistentes = self._registrar_inexistentes(inexistentes)
        self._gravar_lacunas(encontrados)
        self._registrar_inexistentes(inexistentes)

    def _gravar_lacunas(self, registros: list[RegistroDocumento]) -> list[RegistroDocumento]:
        if registros:
            self._publicador.publicar(registros)
            self._contar(registros, lacunas_preenchidas=len(registros))
            self._servicos.notificador.progresso(self._parcial)
        return []

    def _registrar_inexistentes(self, nsus: list[int]) -> list[int]:
        if nsus:
            self._servicos.indice.carregar_inexistentes(self._config.ambiente, nsus, self._servicos.relogio.agora_utc())
            self._parcial = replace(self._parcial, lacunas_inexistentes=self._parcial.lacunas_inexistentes + len(nsus))
        return []


def executar_extracao(config: ConfigExtracao, servicos: Servicos) -> ResultadoExtracao:
    """Roda um run do extrator: pega a trava do ambiente, percorre os lotes novos e preenche as lacunas.

    Uma falha em qualquer ponto é avisada pelo notificador, a trava é liberada e a exceção sobe: o cursor fica no fim
    do último lote completo e o próximo run retoma dali.

    :param config: Parâmetros do run.
    :param servicos: ADN, bucket, índice, notificação e relógio.
    :returns: O que o run fez.
    :raises TravaOcupada: Se outro run do mesmo ambiente está em andamento.
    :raises CursorAusente: Se não há cursor nem ``nsu_inicial``.
    """
    try:
        trava = Trava(servicos.armazenamento, config.ambiente, config.run_id, servicos.relogio, config.validade_trava_s)
        with trava:
            resultado = _Run(config, servicos, trava).executar()
    except Exception as erro:
        servicos.notificador.falhar(erro)
        raise
    servicos.notificador.concluir(resultado)
    return resultado
