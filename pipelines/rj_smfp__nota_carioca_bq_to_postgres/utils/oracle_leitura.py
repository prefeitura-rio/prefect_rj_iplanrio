"""Oracle somente leitura: a configuração vem das cinco variáveis ``DB_ORACLE_NC_LEITURA__*`` e a sessão é READ ONLY.

A convenção do nome das variáveis é a de ``bq_to_oracle/utils/oracle.py``: a pasta ``/db-oracle-nc-leitura`` do
Infisical e a chave ``DB_HOST`` viram ``DB_ORACLE_NC_LEITURA__DB_HOST``. Nada aqui escreve no Oracle: a sessão abre com
``SET TRANSACTION READ ONLY`` (o Oracle recusa qualquer DML/DDL, ORA-01456, mesmo para quem tivesse o privilégio) e o
resultado é conferido por uma sonda antes de qualquer leitura.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.constants import ORACLE_LEITURA_SECRET_PATH

KEYS = ("DB_HOST", "DB_PORT", "DB_SERVICE_NAME", "DB_USERNAME", "DB_PASSWORD")
READ_ONLY_ERROR_CODE = 1456
CONNECT_TIMEOUT_SECONDS = 30


class OracleLeituraConfigError(ValueError):
    """Faltam variáveis de ambiente do usuário somente leitura do Oracle (ou a porta não é um número)."""


class ReadOnlySessionError(RuntimeError):
    """A sessão do Oracle aceitaria escrita: não está em ``SET TRANSACTION READ ONLY``."""


@dataclass(frozen=True)
class OracleLeituraConfig:
    """Conexão do usuário somente leitura do Oracle.

    :param host: Host do listener.
    :param port: Porta do listener.
    :param service_name: Service name do banco.
    :param user: Usuário somente leitura.
    :param secret: Senha do usuário (nunca vai para log nem para mensagem de erro).
    """

    host: str
    port: int
    service_name: str
    user: str
    secret: str

    @property
    def dsn(self) -> str:
        """DSN no formato easy connect (``host:porta/service``)."""
        return f"{self.host}:{self.port}/{self.service_name}"


def secret_env_key(infisical_secret_path: str, key: str) -> str:
    """Variável de ambiente de uma chave do Infisical: ``/db-oracle-x`` e ``DB_HOST`` → ``DB_ORACLE_X__DB_HOST``."""
    prefix = infisical_secret_path.upper().replace("-", "_").replace("/", "")
    return f"{prefix}__{key}"


def read_config(
    infisical_secret_path: str = ORACLE_LEITURA_SECRET_PATH, environ: Mapping[str, str] | None = None
) -> OracleLeituraConfig:
    """Lê do ambiente a conexão do usuário somente leitura.

    :param infisical_secret_path: Pasta do segredo no Infisical.
    :param environ: Variáveis de ambiente; o padrão é ``os.environ``.
    :returns: A configuração.
    :raises OracleLeituraConfigError: Se alguma das cinco variáveis faltar (todas as que faltam são nomeadas) ou se a
        porta não for um número; os valores nunca aparecem na mensagem.
    """
    source = environ if environ is not None else os.environ
    names = {key: secret_env_key(infisical_secret_path, key) for key in KEYS}
    values = {key: source.get(name, "").strip() for key, name in names.items()}
    missing = [names[key] for key, value in values.items() if not value]
    if missing:
        raise OracleLeituraConfigError(
            f"Variáveis de ambiente do Oracle somente leitura ausentes: {', '.join(missing)}; nada foi conectado."
        )
    if not values["DB_PORT"].isdigit():
        raise OracleLeituraConfigError(f"A variável {names['DB_PORT']} precisa ser o número da porta do listener.")
    return OracleLeituraConfig(
        host=values["DB_HOST"],
        port=int(values["DB_PORT"]),
        service_name=values["DB_SERVICE_NAME"],
        user=values["DB_USERNAME"],
        secret=values["DB_PASSWORD"],
    )


def _numbers_as_decimal(cursor: oracledb.Cursor, metadata: oracledb.FetchInfo) -> oracledb.Var | None:
    """Lê todo ``NUMBER`` como ``Decimal`` exato, nunca como ponto flutuante."""
    if metadata.type_code is oracledb.DB_TYPE_NUMBER:
        return cursor.var(Decimal, arraysize=cursor.arraysize)
    return None


def assert_read_only(connection: oracledb.Connection) -> None:
    """Confere que a transação aberta é somente leitura.

    A sonda é ``SELECT ... FOR UPDATE`` em ``DUAL``, que não depende de nenhum privilégio: numa transação READ ONLY o
    Oracle responde ORA-01456; em qualquer outra, o comando passaria. O erro não encerra a transação.

    :param connection: Conexão que acabou de executar ``SET TRANSACTION READ ONLY``.
    :raises ReadOnlySessionError: Se a sonda não for recusada com ORA-01456.
    """
    with connection.cursor() as cursor:
        try:
            cursor.execute("SELECT 1 FROM DUAL FOR UPDATE")
        except oracledb.DatabaseError as error:
            if getattr(error.args[0], "code", None) == READ_ONLY_ERROR_CODE:
                return
            raise
    connection.rollback()
    raise ReadOnlySessionError("A sessão do Oracle não está em SET TRANSACTION READ ONLY; nada foi lido.")


def open_read_only(config: OracleLeituraConfig) -> oracledb.Connection:
    """Abre uma conexão em ``SET TRANSACTION READ ONLY`` (o primeiro comando da transação) e confere a sessão.

    Todas as consultas da conexão enxergam o mesmo instante do banco; ``NUMBER`` chega como ``Decimal``.

    :param config: Conexão do usuário somente leitura.
    :returns: A conexão aberta; quem chama a fecha.
    :raises ReadOnlySessionError: Se a sessão aceitasse escrita.
    """
    connection = oracledb.connect(
        user=config.user, password=config.secret, dsn=config.dsn, tcp_connect_timeout=CONNECT_TIMEOUT_SECONDS
    )
    try:
        connection.outputtypehandler = _numbers_as_decimal
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
        assert_read_only(connection)
    except BaseException:
        connection.close()
        raise
    return connection
