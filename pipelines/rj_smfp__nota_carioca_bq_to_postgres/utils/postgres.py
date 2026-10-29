"""Conexão com o Postgres do Nota Carioca: o DSN vem do ambiente e nunca aparece em log ou mensagem de erro."""

import os
from collections.abc import Mapping

import psycopg

CONNECT_TIMEOUT_SECONDS = 30
KEEPALIVE_IDLE_SECONDS = 60
KEEPALIVE_INTERVAL_SECONDS = 30


class PostgresConfigError(ValueError):
    """A configuração do Postgres está ausente."""


def read_dsn(env_var: str, environ: Mapping[str, str] | None = None) -> str:
    """Lê o DSN da variável de ambiente.

    :param env_var: Nome da variável (``PG_NOTA_CARIOCA__DSN``, do segredo do pod).
    :param environ: Variáveis de ambiente; o padrão é ``os.environ``.
    :returns: O DSN, sem espaços nas pontas.
    :raises PostgresConfigError: Se a variável não existir ou estiver vazia; a mensagem só nomeia a variável.
    """
    value = (environ if environ is not None else os.environ).get(env_var, "").strip()
    if not value:
        raise PostgresConfigError(
            f"A variável de ambiente {env_var} (DSN do Postgres) não está definida; nada foi lido nem conectado."
        )
    return value


def connect(dsn: str, application_name: str) -> psycopg.Connection:
    """Abre uma conexão em modo autocommit, com sonda de keep-alive para as operações longas.

    :param dsn: DSN do Postgres.
    :param application_name: Nome que aparece em ``pg_stat_activity``.
    :returns: A conexão; quem chama a fecha.
    """
    return psycopg.connect(
        dsn,
        autocommit=True,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        application_name=application_name,
        keepalives=1,
        keepalives_idle=KEEPALIVE_IDLE_SECONDS,
        keepalives_interval=KEEPALIVE_INTERVAL_SECONDS,
    )
