"""Compatibilidade do reciclador de processos do ``ProcessPoolExecutor`` (``max_tasks_per_child``) com o Python em uso.

Até a correção do gh-115634 o pool podia travar para sempre: o semáforo de workers ociosos guardava uma ficha vinda de
um worker que depois saía ao atingir ``max_tasks_per_child``, o pool não subia o substituto e, quando todos
chegavam ao teto com faixas na fila, a extração parava sem erro. A correção entrou na série 3.13 em julho de 2026 e a
primeira versão em que o código com ela foi verificado é a 3.13.15 (``_replace_dead_worker`` em ``process.py``).
"""

import sys

# Primeira versão de cada série que tem a correção; séries fora da tabela só a têm a partir da 3.15.
FIXED_FROM = {(3, 13): (3, 13, 15)}
FIRST_FIXED_SERIES = (3, 15)


def recycling_supported(version: tuple[int, int, int] = sys.version_info[:3]) -> bool:
    """Diz se o Python tem a correção do deadlock do ``max_tasks_per_child``.

    :param version: Versão do interpretador, ``(major, minor, micro)``.
    :returns: ``True`` se reciclar workers é seguro nesta versão.
    """
    fixed_from = FIXED_FROM.get(version[:2])
    return version >= fixed_from if fixed_from is not None else version[:2] >= FIRST_FIXED_SERIES
