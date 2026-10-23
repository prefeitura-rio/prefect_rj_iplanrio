from collections.abc import Iterator
from dataclasses import dataclass

import psycopg
import pytest
from harness import (
    OracleServer,
    PostgresServer,
    docker_available,
    drop_database,
    new_database,
    start_oracle,
    start_postgres,
)
from prefect.testing.utilities import prefect_test_harness

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import discord


def _no_secret_block(name: str) -> str:
    raise ValueError(f"Secret block {name} indisponível nos testes")


@pytest.fixture(autouse=True)
def no_real_discord(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nenhum teste fala com o Discord de verdade, mesmo que o webhook esteja no ambiente de quem roda.

    Também não consulta a API do Prefect atrás do Secret block do webhook.
    """
    monkeypatch.delenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA_STAGING", raising=False)
    monkeypatch.setattr(discord, "_load_secret_block", _no_secret_block)


@pytest.fixture(scope="session")
def postgres_server() -> Iterator[PostgresServer]:
    """Postgres 17 efêmero com as migrações do R1 (cópia em ``tests/fixtures``)."""
    if not docker_available():
        pytest.skip("Docker indisponível: os testes de integração precisam de contêineres")
    yield from start_postgres()


@dataclass(frozen=True)
class Database:
    """Um banco migrado, só deste teste, e os DSNs dos papéis."""

    server: PostgresServer
    name: str

    @property
    def admin_dsn(self) -> str:
        """DSN do superusuário (dono dos objetos), para montar cenários e conferir resultados."""
        return self.server.dsn(self.name)

    @property
    def carregador_dsn(self) -> str:
        """DSN de um login em ``nc_carregador``."""
        return self.server.dsn(self.name, "u_carregador")

    @property
    def sincronizador_dsn(self) -> str:
        """DSN de um login em ``nc_sincronizador``."""
        return self.server.dsn(self.name, "u_sincronizador")

    def admin(self) -> psycopg.Connection:
        """Conexão do superusuário, sem transação aberta."""
        return psycopg.connect(self.admin_dsn, autocommit=True)


@pytest.fixture
def database(postgres_server: PostgresServer) -> Iterator[Database]:
    """Banco novo (cópia do modelo migrado) para um teste."""
    name = new_database(postgres_server)
    try:
        yield Database(postgres_server, name)
    finally:
        drop_database(postgres_server, name)


@pytest.fixture(scope="session")
def oracle_server() -> Iterator[OracleServer]:
    """Oracle Free efêmero, iniciado só no primeiro teste que o pede."""
    if not docker_available():
        pytest.skip("Docker indisponível: os testes de integração precisam de contêineres")
    yield from start_oracle()


@pytest.fixture(scope="session")
def prefect_api() -> Iterator[None]:
    """API temporária do Prefect (SQLite): flows e tasks rodam de verdade, sem tocar no Prefect real."""
    with prefect_test_harness():
        yield
