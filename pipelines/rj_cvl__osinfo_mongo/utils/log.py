"""Logger visível na UI do Prefect, escopado a este pipeline.

``prefect_rj_iplanrio.logging.get_logger`` (o ponto de entrada mandado pelo
STYLEGUIDE §5.5) hoje só devolve ``logging.getLogger(name)``, sem handler —
registros feitos por ele nunca chegam na UI do Prefect. É um problema do
módulo compartilhado, confirmado pelo responsável por ``prefect_rj_iplanrio``
e fora do escopo deste pipeline corrigir.

Este wrapper mantém ``get_logger`` do styleguide como base/fallback (fora de
um run: testes, scripts, import do módulo) e, quando a chamada acontece
dentro de um flow/task em execução, encaminha para o run logger nativo do
Prefect (``get_run_logger()``) — o único canal comprovado a aparecer na UI.

``get_run_logger()`` só funciona *dentro* de um run; como cada módulo monta
seu logger uma vez, no import (antes de qualquer run existir), a escolha
entre os dois é adiada para cada chamada (``.info``, ``.warning`` etc.), não
para a criação do logger — do contrário sempre cairia no fallback.
"""

from logging import Logger

from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger

from prefect_rj_iplanrio.logging import get_logger as _get_shared_logger


class _VisibleLogger:
    """Repassa cada chamada para o run logger do Prefect quando disponível."""

    def __init__(self, name: str) -> None:
        self._fallback = _get_shared_logger(name)

    def _resolve(self) -> Logger:
        """Escolhe o logger de destino no momento da chamada.

        :returns: O run logger do Prefect dentro de um flow/task; o logger
            compartilhado (``prefect_rj_iplanrio.logging``) fora disso.
        """
        try:
            return get_run_logger()
        except MissingContextError:
            return self._fallback

    def __getattr__(self, item: str):
        """Encaminha qualquer atributo/método para o logger resolvido na hora.

        :param item: Nome do atributo (``info``, ``warning``, ``error`` etc.).
        :returns: O atributo correspondente do logger resolvido.
        """
        return getattr(self._resolve(), item)


def get_logger(name: str) -> Logger:
    """Retorna um logger visível na UI do Prefect quando chamado dentro de um run.

    :param name: Nome do módulo — passe ``__name__`` do módulo chamador.
    :returns: Logger cujas chamadas aparecem na UI do Prefect durante um run,
        e caem no logger padrão do workspace fora de um run.
    """
    return _VisibleLogger(name)  # type: ignore[return-value]
