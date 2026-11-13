"""Trava por objeto do GCS com geração: um run de extração por ambiente, com validade e renovação.

O objeto ``adn/{ambiente}/_trava.json`` guarda ``{dono, expira_em}``. Adquirir é criar o objeto se ausente (ou
assumir um vencido trocando-o pela geração lida); renovar e liberar só valem para a geração que este run segura. Assim
dois processos nunca acham que são donos ao mesmo tempo, e um run que perdeu a trava sabe antes do próximo lote.
"""

import json
from datetime import timedelta
from types import TracebackType
from typing import Self

from pipelines.rj_smfp__nota_carioca_adn.constants import VALIDADE_TRAVA_S
from pipelines.rj_smfp__nota_carioca_adn.utils.armazenamento import Armazenamento
from pipelines.rj_smfp__nota_carioca_adn.utils.relogio import Relogio
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


class TravaOcupada(Exception):  # noqa: N818 - nome do domínio, como as demais exceções do pacote
    """Outro run segura a trava do ambiente e ela ainda não venceu."""


class TravaPerdida(Exception):  # noqa: N818 - nome do domínio, como as demais exceções do pacote
    """A trava deste run foi trocada ou apagada por outro processo."""


def objeto_da_trava(ambiente: str) -> str:
    """Nome do objeto de trava do ambiente."""
    return f"adn/{ambiente}/_trava.json"


class Trava:
    """Trava do ambiente; use como gerenciador de contexto (adquire ao entrar, libera ao sair).

    :param armazenamento: Bucket com precondição de geração.
    :param ambiente: ``staging`` ou ``prod``.
    :param dono: Identificação deste run (gravada no objeto).
    :param relogio: Fonte da hora de parede.
    :param validade_s: Validade de cada aquisição ou renovação.
    """

    VALIDADE_S = VALIDADE_TRAVA_S

    def __init__(
        self,
        armazenamento: Armazenamento,
        ambiente: str,
        dono: str,
        relogio: Relogio,
        validade_s: float = VALIDADE_S,
    ) -> None:
        self._armazenamento = armazenamento
        self._objeto = objeto_da_trava(ambiente)
        self._dono = dono
        self._relogio = relogio
        self._validade_s = validade_s
        self._geracao: int | None = None

    def _conteudo(self) -> bytes:
        expira_em = self._relogio.agora_utc() + timedelta(seconds=self._validade_s)
        return json.dumps({"dono": self._dono, "expira_em": expira_em.timestamp()}).encode()

    def _vigente(self, conteudo: bytes) -> bool:
        try:
            return float(json.loads(conteudo)["expira_em"]) > self._relogio.agora_utc().timestamp()
        except (ValueError, KeyError, TypeError):
            logger.warning("Trava %s com conteúdo ilegível; tratada como vencida", self._objeto)
            return False

    def adquirir(self) -> None:
        """Pega a trava.

        :raises TravaOcupada: Se outro run a segura e ela não venceu, ou se perdeu a corrida para assumi-la.
        """
        geracao = self._armazenamento.criar_se_ausente(self._objeto, self._conteudo())
        if geracao is None:
            atual = self._armazenamento.ler_com_geracao(self._objeto)
            if atual is not None and self._vigente(atual[0]):
                raise TravaOcupada(f"Trava {self._objeto} segura por {json.loads(atual[0]).get('dono')!r}")
            geracao = (
                self._armazenamento.criar_se_ausente(self._objeto, self._conteudo())
                if atual is None
                else self._armazenamento.substituir_se_geracao(self._objeto, self._conteudo(), atual[1])
            )
        if geracao is None:
            raise TravaOcupada(f"Trava {self._objeto} tomada por outro run durante a aquisição")
        self._geracao = geracao

    def renovar(self) -> None:
        """Estende a validade; chamar a cada lote.

        :raises TravaPerdida: Se a trava não é mais a deste run.
        """
        if self._geracao is None:
            raise TravaPerdida("Trava nunca adquirida")
        nova = self._armazenamento.substituir_se_geracao(self._objeto, self._conteudo(), self._geracao)
        if nova is None:
            raise TravaPerdida(f"Trava {self._objeto} foi trocada ou apagada por outro processo")
        self._geracao = nova

    def liberar(self) -> None:
        """Apaga a trava se ainda for deste run; nunca levanta."""
        if self._geracao is not None and not self._armazenamento.apagar_se_geracao(self._objeto, self._geracao):
            logger.warning("Trava %s não era mais deste run ao liberar; nada foi apagado", self._objeto)
        self._geracao = None

    def __enter__(self) -> Self:
        self.adquirir()
        return self

    def __exit__(
        self, tipo: type[BaseException] | None, erro: BaseException | None, rastro: TracebackType | None
    ) -> None:
        self.liberar()
