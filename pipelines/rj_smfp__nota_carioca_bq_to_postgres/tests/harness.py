"""Contêineres efêmeros dos testes: Postgres 17 com as migrações do R1 e Oracle Free, sempre em 127.0.0.1.

Nenhum teste fala com Cloud SQL, Oracle ou GCP reais: o que roda aqui nasce e morre com a sessão do pytest (o Ryuk
do testcontainers remove o que sobrar se o processo morrer).
"""

import itertools
import secrets
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import oracledb
import psycopg
from psycopg import sql
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

POSTGRES_IMAGE = "postgres:17-alpine"
ORACLE_IMAGE = "gvenzl/oracle-free:23-slim-faststart"
ORACLE_SERVICE = "FREEPDB1"
LOOPBACK = "127.0.0.1"
FIXTURES = Path(__file__).parent / "fixtures"
MAX_ORACLE_CONTAINERS = 2
ORACLE_READY_TIMEOUT_SECONDS = 600
ORACLE_WAIT_SLOT_SECONDS = 900
LOGIN_ROLES = {"u_carregador": "nc_carregador", "u_sincronizador": "nc_sincronizador"}
_database_numbers = itertools.count(1)


def docker_available() -> bool:
    """Diz se o daemon do Docker responde; sem ele os testes de integração são pulados, com o motivo."""
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20, check=False).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _loopback_container(image: str, port: int) -> DockerContainer:
    container = DockerContainer(image)
    # A API do testcontainers só publica em 0.0.0.0; o docker-py aceita ("ip",) para porta aleatória nesse IP.
    container.ports[f"{port}/tcp"] = (LOOPBACK,)
    return container


@dataclass(frozen=True)
class PostgresServer:
    """Postgres de teste com um banco-modelo já migrado e um login por papel de grupo.

    :param port: Porta em 127.0.0.1.
    :param admin_password: Senha do superusuário (só existe neste contêiner).
    :param role_password: Senha dos logins ``u_carregador`` e ``u_sincronizador``.
    """

    port: int
    admin_password: str
    role_password: str

    def dsn(self, database: str, user: str = "postgres") -> str:
        """DSN de um login deste servidor."""
        secret = self.admin_password if user == "postgres" else self.role_password
        return f"postgresql://{user}:{secret}@{LOOPBACK}:{self.port}/{database}"


def _wait_for_postgres(port: int, admin_secret: str) -> None:
    deadline = time.monotonic() + 60
    while True:
        try:
            psycopg.connect(
                f"postgresql://postgres:{admin_secret}@{LOOPBACK}:{port}/postgres", connect_timeout=2
            ).close()
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)
        else:
            return


def start_postgres() -> Iterator[PostgresServer]:
    """Sobe o Postgres, aplica as migrações do R1 num banco-modelo e cria os logins; remove tudo ao terminar."""
    admin_secret, role_secret = secrets.token_hex(8), secrets.token_hex(8)
    container = _loopback_container(POSTGRES_IMAGE, 5432).with_env("POSTGRES_PASSWORD", admin_secret)
    with container:
        server = PostgresServer(int(container.get_exposed_port(5432)), admin_secret, role_secret)
        _wait_for_postgres(server.port, admin_secret)
        with psycopg.connect(server.dsn("postgres"), autocommit=True) as admin:
            admin.execute("CREATE DATABASE nc_modelo")
        with psycopg.connect(server.dsn("nc_modelo"), autocommit=True) as model:
            model.execute((FIXTURES / "migracoes_up.sql").read_text(encoding="utf-8"))
            for login, group in LOGIN_ROLES.items():
                model.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {} IN ROLE {}").format(
                        sql.Identifier(login), sql.Literal(role_secret), sql.Identifier(group)
                    )
                )
        yield server


def new_database(server: PostgresServer) -> str:
    """Cria um banco novo a partir do modelo migrado e retorna o nome dele."""
    name = f"nc_teste_{next(_database_numbers)}"
    with psycopg.connect(server.dsn("postgres"), autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE nc_modelo").format(sql.Identifier(name)))
    return name


def drop_database(server: PostgresServer, name: str) -> None:
    """Apaga um banco de teste, derrubando as conexões que sobraram."""
    with psycopg.connect(server.dsn("postgres"), autocommit=True) as admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def _oracle_containers_running() -> int:
    images = subprocess.run(
        ["docker", "ps", "--format", "{{.Image}}"], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    return sum("oracle-free" in image for image in images)


@dataclass(frozen=True)
class OracleServer:
    """Oracle Free de teste (PDB ``FREEPDB1``) com o usuário SYSTEM.

    :param port: Porta em 127.0.0.1.
    :param system_password: Senha de SYS/SYSTEM (só existe neste contêiner).
    """

    port: int
    system_password: str

    def connect(self, user: str, secret: str) -> oracledb.Connection:
        """Conecta ao PDB com o usuário informado."""
        return oracledb.connect(user=user, password=secret, dsn=f"{LOOPBACK}:{self.port}/{ORACLE_SERVICE}")


def start_oracle() -> Iterator[OracleServer]:
    """Sobe o Oracle Free (espera haver vaga: no máximo 2 destas imagens ao mesmo tempo no host) e o remove ao fim."""
    deadline = time.monotonic() + ORACLE_WAIT_SLOT_SECONDS
    while _oracle_containers_running() >= MAX_ORACLE_CONTAINERS:
        if time.monotonic() > deadline:
            raise TimeoutError("Já há dois contêineres Oracle Free rodando no host; tente de novo mais tarde.")
        time.sleep(15)
    system_secret = secrets.token_hex(8)
    container = _loopback_container(ORACLE_IMAGE, 1521).with_env("ORACLE_PASSWORD", system_secret)
    with container:
        wait_for_logs(container, "DATABASE IS READY TO USE!", timeout=ORACLE_READY_TIMEOUT_SECONDS)
        yield OracleServer(int(container.get_exposed_port(1521)), system_secret)
