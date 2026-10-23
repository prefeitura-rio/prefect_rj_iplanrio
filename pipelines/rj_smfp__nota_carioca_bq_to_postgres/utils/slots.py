"""Slot A/B: qual das duas cópias as views ``public.*`` servem agora, e portanto qual recebe a próxima carga.

O carregador (``nc_carregador``) não lê ``public.publicacoes`` (migração 00008 do R1), então o slot servido vem da
definição das views no catálogo (``pg_views``), que é o que o gateway de fato lê.
"""

import re

import psycopg

SLOTS = ("a", "b")
VIEWS = ("mvt_notas_nacionais_exigiveis", "mvt_notas_nacionais_exigiveis_detalhes")
SLOT_REFERENCE = re.compile(r"\bexigiveis_([ab])\.")


class SlotError(RuntimeError):
    """Não dá para dizer, pelas views, qual slot está em uso."""


def served_slot(connection: psycopg.Connection) -> str:
    """Slot que as duas views servem agora.

    :param connection: Conexão de qualquer login que acesse o catálogo.
    :returns: ``a`` ou ``b``.
    :raises SlotError: Se faltar uma view, se uma não referenciar um slot ou se as duas discordarem.
    """
    rows = connection.execute(
        "SELECT viewname, definition FROM pg_views WHERE schemaname = 'public' AND viewname = ANY(%s)", [list(VIEWS)]
    ).fetchall()
    served = {name: set(SLOT_REFERENCE.findall(definition)) for name, definition in rows}
    missing = [view for view in VIEWS if view not in served]
    if missing:
        raise SlotError(f"Views dos exigíveis ausentes em public: {missing}")
    slots = {slot for found in served.values() for slot in found}
    if len(slots) != 1 or any(len(found) != 1 for found in served.values()):
        raise SlotError(f"As views dos exigíveis não servem um único slot: {served}")
    return next(iter(slots))


def inactive_slot(connection: psycopg.Connection) -> str:
    """Slot que não está em uso e recebe a próxima carga.

    :param connection: Conexão de qualquer login que acesse o catálogo.
    :returns: ``a`` ou ``b``.
    :raises SlotError: Se o slot servido for indeterminado.
    """
    return "b" if served_slot(connection) == "a" else "a"
