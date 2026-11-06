"""Leitura e renderização das queries em ``queries/``.

Equivalente ao ``prefect_rj_iplanrio.sql.load_query``, mas resolvendo o
diretório a partir da raiz da pipeline (``utils/`` -> ``../queries``), porque
as queries são chamadas de módulos em ``tasks/`` e ``utils/``.

``string.Template`` (``$var``) em vez de ``str.format``: o SQL do BigQuery tem
``{`` em ``STRUCT<...>``/``UNNEST([{}])`` e o format quebraria.
"""

from __future__ import annotations

from pathlib import Path
from string import Template

_QUERIES = Path(__file__).parent.parent / "queries"


def ler_query(nome: str, **params: object) -> str:
    """Carrega e renderiza ``queries/<nome>.sql``.

    :param nome: nome do arquivo sem a extensão.
    :param params: valores dos placeholders ``$var`` do template.
    :returns: SQL renderizado.
    :raises FileNotFoundError: Se o arquivo não existir.
    :raises KeyError: Se faltar um placeholder obrigatório.
    """
    return Template((_QUERIES / f"{nome}.sql").read_text(encoding="utf-8")).substitute(**params)
