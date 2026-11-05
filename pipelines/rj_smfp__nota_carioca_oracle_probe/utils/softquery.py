"""Consultas ao dicionário que não podem derrubar a sonda quando falta privilégio."""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial

import oracledb

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.session import query_rows
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

PRIVILEGE_ERROR = re.compile(r"ORA-(00942|01031)")


@dataclass(frozen=True)
class SoftRows:
    """Resultado de uma consulta que pode falhar sem abortar a sonda.

    :param view: View consultada, como no SQL (``v$parameter``).
    :param rows: Linhas retornadas; ``None`` se a consulta falhou.
    :param note: Motivo da falha, pronto para o relatório; ``None`` se deu certo.
    """

    view: str
    rows: tuple[dict[str, object], ...] | None
    note: str | None


def grant_name(view: str) -> str:
    """Retorna o nome do objeto a conceder para liberar a leitura da view.

    :param view: View como no SQL (``v$parameter``, ``dba_segments``).
    :returns: Objeto do ``GRANT`` (``SYS.V_$PARAMETER``, ``SYS.DBA_SEGMENTS``).
    """
    lowered = view.lower()
    if lowered.startswith("v$"):
        return f"SYS.V_${view[2:].upper()}"
    if lowered.startswith("dba_"):
        return f"SYS.{view.upper()}"
    return view.upper()


def failure_note(view: str, error: oracledb.DatabaseError) -> str:
    """Descreve por que a consulta falhou, indicando o que pedir à DBA quando for privilégio.

    :param view: View consultada.
    :param error: Erro do driver.
    :returns: Mensagem em português.
    """
    text = str(error)
    if PRIVILEGE_ERROR.search(text):
        return f"sem privilégio para {view} — peça à DBA: GRANT SELECT ON {grant_name(view)} / SELECT_CATALOG_ROLE"
    return f"falha ao consultar {view}: {text.splitlines()[0] if text else type(error).__name__}"


def soft_call(view: str, call: Callable[[], list[dict[str, object]]]) -> SoftRows:
    """Executa a leitura e devolve o motivo em vez de lançar se o banco recusar.

    Só erros do banco (``oracledb.DatabaseError``) são tratados; o motivo é registrado e retornado.

    :param view: View consultada, usada na mensagem de falha.
    :param call: Função que executa a consulta e retorna as linhas.
    :returns: Linhas ou nota de falha.
    """
    try:
        rows = call()
    except oracledb.DatabaseError as error:
        note = failure_note(view, error)
        logger.warning(note)
        return SoftRows(view=view, rows=None, note=note)
    return SoftRows(view=view, rows=tuple(rows), note=None)


def soft_rows(
    cursor: oracledb.Cursor, view: str, name: str, binds: Mapping[str, object] | None = None, **params: object
) -> SoftRows:
    """Executa uma consulta de ``queries/`` sem deixar a falta de privilégio abortar a sonda.

    :param cursor: Cursor de uma conexão aberta.
    :param view: View consultada, usada na mensagem de falha.
    :param name: Nome do arquivo em ``queries/``.
    :param binds: Variáveis de bind.
    :param params: Valores do template do SQL.
    :returns: Linhas ou nota de falha.
    """
    return soft_call(view, partial(query_rows, cursor, name, binds, **params))
