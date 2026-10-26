"""Acesso ao Oracle de destino: configuração, criação protegida e contagem de tabelas."""

import re
from dataclasses import dataclass

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_query import QUERIES_ANCHOR, fetch_all
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.settings import Settings
from utils.utils import render_query

TABLE_PREFIX = "BQLOAD_"


MANAGED_TABLE_MARKER = "rj_smfp__nota_carioca_bq_to_oracle"


# Os sinônimos ficam também nos schemas dos consumidores, o que exige CREATE ANY SYNONYM
# até do dono.
OWNER_PRIVILEGES = ("CREATE ANY SYNONYM",)


CROSS_SCHEMA_PRIVILEGES = (
    *OWNER_PRIVILEGES,
    "ALTER ANY INDEX",
    "ANALYZE ANY",
    "COMMENT ANY TABLE",
    "CREATE ANY INDEX",
    "CREATE ANY TABLE",
    "DROP ANY INDEX",
    "DROP ANY TABLE",
    "GRANT ANY OBJECT PRIVILEGE",
    "INSERT ANY TABLE",
    "LOCK ANY TABLE",
    "SELECT ANY TABLE",
)


# Um CREATE INDEX grande passa minutos sem tráfego na rede; a sonda evita que um
# firewall derrube a conexão ociosa.
KEEPALIVE_MINUTES = 2


IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")


@dataclass(frozen=True)
class OracleConfig:
    """Conexão com o Oracle de destino, lida do Infisical.

    :param user: Usuário do banco.
    :param password: Senha do usuário.
    :param host: Host ou IP do banco.
    :param port: Porta do listener.
    :param service_name: Service name do banco.
    :param schema: Schema onde as tabelas são criadas.
    """

    user: str
    password: str
    host: str
    port: str
    service_name: str
    schema: str

    @property
    def dsn(self) -> str:
        """Retorna o DSN no formato easy connect (``host:porta/service``)."""
        return f"{self.host}:{self.port}/{self.service_name}"


def validate_identifier(name: str) -> str:
    """Normaliza e valida um identificador do Oracle.

    :param name: Nome do schema ou da tabela.
    :returns: O nome em maiúsculas.
    :raises ValueError: Se o nome não for um identificador válido.
    """
    upper = name.upper()
    if not IDENTIFIER_PATTERN.match(upper):
        raise ValueError(f"Identificador inválido para o Oracle: {name!r}")
    return upper


def read_oracle_config() -> OracleConfig:
    """Lê do ambiente a configuração do Oracle.

    :returns: Configuração da conexão.
    :raises pydantic.ValidationError: Se alguma variável estiver ausente.
    :raises ValueError: Se o schema for inválido.
    """
    settings = Settings()

    return OracleConfig(
        user=settings.DB_USERNAME,
        password=settings.DB_PASSWORD.get_secret_value(),
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        service_name=settings.DB_SERVICE_NAME,
        schema=validate_identifier(settings.DB_SCHEMA),
    )


def oracle_table_name(bq_table_id: str) -> str:
    """Retorna o nome da tabela de destino, sempre com o prefixo de proteção.

    :param bq_table_id: Nome da tabela no BigQuery.
    :returns: Nome da tabela no Oracle.
    :raises ValueError: Se o nome resultante não for um identificador válido.
    """
    return validate_identifier(f"{TABLE_PREFIX}{bq_table_id}")


def assert_managed_table(table: str, comment: str | None) -> None:
    """Garante que a tabela foi criada por esta pipeline antes de alterá-la.

    :param table: Nome da tabela no Oracle.
    :param comment: Comentário atual da tabela.
    :raises PermissionError: Se a tabela não tiver o prefixo ou a marca da
        pipeline.
    """
    if not table.startswith(TABLE_PREFIX):
        raise PermissionError(f"{table} não começa com {TABLE_PREFIX}; a pipeline não altera essa tabela.")
    if not (comment or "").startswith(MANAGED_TABLE_MARKER):
        raise PermissionError(f"{table} já existe e não foi criada por {MANAGED_TABLE_MARKER}; nada foi alterado.")


def missing_privileges(session_user: str, schema: str, privileges: set[str]) -> list[str]:
    """Lista os privilégios que faltam para operar as tabelas do schema.

    O dono do schema precisa apenas criar sinônimos nos schemas dos
    consumidores. Os demais usuários precisam de todos: criar e comentar a
    tabela, esvaziá-la com ``TRUNCATE``, travá-la e inserir no direct path do
    SQL*Loader, contar as linhas, apagar e criar os índices, tirar o paralelismo
    deles, coletar estatísticas, conceder acesso e criar os sinônimos.

    :param session_user: Usuário efetivo da sessão (o alvo, em conexões proxy).
    :param schema: Schema onde as tabelas ficam.
    :param privileges: Privilégios de sistema da sessão.
    :returns: Privilégios ausentes, em ordem alfabética.
    """
    required = OWNER_PRIVILEGES if session_user == schema else CROSS_SCHEMA_PRIVILEGES
    return sorted(set(required) - privileges)


def assert_privileges(cursor: oracledb.Cursor, schema: str) -> None:
    """Falha antes de qualquer alteração se a sessão não puder operar o schema.

    Sem essa checagem, o ``CREATE TABLE`` podia funcionar e o ``COMMENT`` falhar,
    deixando uma tabela sem a marca da pipeline que bloqueia os runs seguintes.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema onde as tabelas ficam.
    :raises PermissionError: Se faltar algum privilégio.
    """
    cursor.execute(render_query(QUERIES_ANCHOR, "get_session_privileges"))
    rows = fetch_all(cursor)
    session_user = str(rows[0][0]) if rows else ""
    missing = missing_privileges(session_user, schema, {str(privilege) for _, privilege in rows})
    if missing and session_user == schema:
        raise PermissionError(f"O usuário {schema} não tem {missing}. Nada foi alterado. Peça à DBA.")
    if missing:
        raise PermissionError(
            f"O usuário {session_user} não é dono de {schema} e não tem {missing}. Nada foi alterado. "
            + f"Conecte como {schema} via proxy (DB_USERNAME=<usuario>[{schema}], após "
            + f'ALTER USER {schema} GRANT CONNECT THROUGH "<usuario>") ou peça esses privilégios à DBA.'
        )


def connect(config: OracleConfig) -> oracledb.Connection:
    """Abre uma conexão em modo thick com o Oracle.

    O modo thick usa o Instant Client da imagem base e aceita usuários com
    verifier de senha 10G, que o modo thin rejeita (``DPY-3015``). A conexão
    envia uma sonda a cada ``KEEPALIVE_MINUTES`` enquanto espera o banco.

    :param config: Configuração da conexão.
    :returns: Conexão aberta.
    """
    if oracledb.is_thin_mode():
        oracledb.init_oracle_client()
    dsn = f"{config.dsn}?expire_time={KEEPALIVE_MINUTES}"
    return oracledb.connect(user=config.user, password=config.password, dsn=dsn)
