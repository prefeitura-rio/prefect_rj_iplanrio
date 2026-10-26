"""Access the target Oracle: configuration, protected creation and table counts."""

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
    """The connection to the target Oracle, read from Infisical.

    `host` is the host or IP of the database, `port` is the port of the listener, and
    `service_name` is its service name. `schema` is where the tables are created.
    """

    user: str
    password: str
    host: str
    port: str
    service_name: str
    schema: str

    @property
    def dsn(self) -> str:
        """Return the DSN in the easy connect format (`host:port/service`)."""
        return f"{self.host}:{self.port}/{self.service_name}"


def validate_identifier(name: str) -> str:
    """Normalize and validate an Oracle identifier, and return it in upper case.

    Raise `ValueError` if the name is not a valid identifier.
    """
    upper = name.upper()
    if not IDENTIFIER_PATTERN.match(upper):
        raise ValueError(f"Identificador inválido para o Oracle: {name!r}")
    return upper


def read_oracle_config() -> OracleConfig:
    """Read the Oracle configuration from the environment.

    Raise `pydantic.ValidationError` if a variable is missing, and `ValueError` if the
    schema is not valid.
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
    """Return the name of the target table, always with the protection prefix.

    Raise `ValueError` if the resulting name is not a valid identifier.
    """
    return validate_identifier(f"{TABLE_PREFIX}{bq_table_id}")


def assert_managed_table(table: str, comment: str | None) -> None:
    """Make sure that this pipeline made the table before it changes it.

    Raise `PermissionError` if the table has no prefix or no mark of the pipeline.
    """
    if not table.startswith(TABLE_PREFIX):
        raise PermissionError(f"{table} não começa com {TABLE_PREFIX}; a pipeline não altera essa tabela.")
    if not (comment or "").startswith(MANAGED_TABLE_MARKER):
        raise PermissionError(f"{table} já existe e não foi criada por {MANAGED_TABLE_MARKER}; nada foi alterado.")


def missing_privileges(session_user: str, schema: str, privileges: set[str]) -> list[str]:
    """List the privileges that are missing to operate the tables of the schema.

    The owner of the schema only needs to create synonyms in the schemas of the
    consumers. The other users need all of them: to create and comment the table, empty
    it with `TRUNCATE`, lock it and insert in the direct path of SQL*Loader, count the
    rows, drop and create the indexes, remove their parallelism, gather statistics,
    grant access and create the synonyms. `session_user` is the effective user of the
    session (the target, in proxy connections). Return the missing privileges in
    alphabetical order.
    """
    required = OWNER_PRIVILEGES if session_user == schema else CROSS_SCHEMA_PRIVILEGES
    return sorted(set(required) - privileges)


def assert_privileges(cursor: oracledb.Cursor, schema: str) -> None:
    """Fail before any change if the session cannot operate the schema.

    Without this check, `CREATE TABLE` could work and `COMMENT` fail, and leave a table
    without the mark of the pipeline, which blocks the next runs. Raise
    `PermissionError` if a privilege is missing.
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
    """Open a thick mode connection to Oracle.

    The thick mode uses the Instant Client of the base image, and it accepts users with
    a 10G password verifier, which the thin mode rejects (`DPY-3015`). The connection
    sends a probe every `KEEPALIVE_MINUTES` while it waits for the database.
    """
    if oracledb.is_thin_mode():
        oracledb.init_oracle_client()
    dsn = f"{config.dsn}?expire_time={KEEPALIVE_MINUTES}"
    return oracledb.connect(user=config.user, password=config.password, dsn=dsn)
