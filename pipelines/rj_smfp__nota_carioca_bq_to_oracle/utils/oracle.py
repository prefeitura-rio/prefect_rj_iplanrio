"""Acesso ao Oracle de destino: configuração, criação protegida e contagem de tabelas."""

import re
from dataclasses import dataclass
from pathlib import Path

import oracledb

from iplanrio.pipelines_utils.env import getenv_or_action
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import OracleColumn, oracle_column
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)

TABLE_PREFIX = "BQLOAD_"
MANAGED_TABLE_MARKER = "rj_smfp__nota_carioca_bq_to_oracle"
CROSS_SCHEMA_PRIVILEGES = (
    "COMMENT ANY TABLE",
    "CREATE ANY TABLE",
    "DROP ANY TABLE",
    "INSERT ANY TABLE",
    "LOCK ANY TABLE",
    "SELECT ANY TABLE",
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")
# load_query resolve queries/ no diretório pai do caminho recebido; a pasta utils/ aponta para a raiz da pipeline.
QUERIES_ANCHOR = str(Path(__file__).parent)


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


def secret_env_key(infisical_secret_path: str, key: str) -> str:
    """Monta o nome da variável de ambiente de uma chave do Infisical.

    Segue a convenção da lib ``iplanrio``: ``/db-oracle-x`` e ``DB_HOST`` viram
    ``DB_ORACLE_X__DB_HOST``.

    :param infisical_secret_path: Pasta do segredo no Infisical.
    :param key: Nome da chave sem o prefixo da pasta.
    :returns: Nome da variável de ambiente.
    """
    prefix = infisical_secret_path.upper().replace("-", "_").replace("/", "")
    return f"{prefix}__{key}"


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


def read_oracle_config(infisical_secret_path: str) -> OracleConfig:
    """Lê do ambiente a configuração do Oracle cadastrada no Infisical.

    :param infisical_secret_path: Pasta do segredo no Infisical.
    :returns: Configuração da conexão.
    :raises ValueError: Se alguma variável estiver ausente ou o schema for
        inválido.
    """

    def read(key: str) -> str:
        return str(getenv_or_action(secret_env_key(infisical_secret_path, key)))

    return OracleConfig(
        user=read("DB_USERNAME"),
        password=read("DB_PASSWORD"),
        host=read("DB_HOST"),
        port=read("DB_PORT"),
        service_name=read("DB_SERVICE_NAME"),
        schema=validate_identifier(read("DB_SCHEMA")),
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
    """Lista os privilégios que faltam para operar tabelas em outro schema.

    O dono do schema não precisa de privilégios ``ANY``. Os demais usuários
    precisam de todos: criar e comentar a tabela, esvaziá-la com ``TRUNCATE``,
    travá-la e inserir no direct path do SQL*Loader e contar as linhas.

    :param session_user: Usuário efetivo da sessão (o alvo, em conexões proxy).
    :param schema: Schema onde as tabelas ficam.
    :param privileges: Privilégios de sistema da sessão.
    :returns: Privilégios ausentes, em ordem alfabética.
    """
    if session_user == schema:
        return []
    return sorted(set(CROSS_SCHEMA_PRIVILEGES) - privileges)


def assert_privileges(cursor: oracledb.Cursor, schema: str) -> None:
    """Falha antes de qualquer alteração se a sessão não puder operar o schema.

    Sem essa checagem, o ``CREATE TABLE`` podia funcionar e o ``COMMENT`` falhar,
    deixando uma tabela sem a marca da pipeline que bloqueia os runs seguintes.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema onde as tabelas ficam.
    :raises PermissionError: Se faltar algum privilégio.
    """
    cursor.execute(load_query(QUERIES_ANCHOR, "get_session_privileges"))
    rows = cursor.fetchall()
    session_user = rows[0][0] if rows else ""
    missing = missing_privileges(session_user, schema, {privilege for _, privilege in rows})
    if missing:
        raise PermissionError(
            f"O usuário {session_user} não é dono de {schema} e não tem {missing}. Nada foi alterado. "
            f"Conecte como {schema} via proxy (DB_USERNAME=<usuario>[{schema}], após "
            f'ALTER USER {schema} GRANT CONNECT THROUGH "<usuario>") ou peça esses privilégios à DBA.'
        )


def column_definitions(columns: list[OracleColumn]) -> str:
    """Monta a lista de colunas do ``CREATE TABLE``.

    :param columns: Colunas da tabela.
    :returns: Fragmento SQL com uma coluna por linha.
    """
    return ",\n".join(f"  {column.definition}" for column in columns)


def read_column_definitions(cursor: oracledb.Cursor, owner: str, table: str) -> list[OracleColumn]:
    """Lê as colunas de uma tabela no dicionário do Oracle, na ordem da tabela.

    :param cursor: Cursor de uma conexão aberta.
    :param owner: Dono da tabela.
    :param table: Nome da tabela.
    :returns: Colunas da tabela, ou lista vazia se ela não existir ou não estiver
        visível para a sessão.
    """
    cursor.execute(load_query(QUERIES_ANCHOR, "get_column_definitions"), {"owner": owner, "table_name": table})
    names = [description[0].lower() for description in cursor.description]
    return [oracle_column(dict(zip(names, row, strict=True))) for row in cursor.fetchall()]


def fetch_template_columns(config: OracleConfig, template_schema: str, table: str) -> list[OracleColumn]:
    """Lê a definição da tabela original, que serve de modelo para a de destino.

    :param config: Configuração da conexão.
    :param template_schema: Schema da tabela original.
    :param table: Nome da tabela original (o mesmo da tabela no BigQuery).
    :returns: Colunas da tabela original, na ordem dela.
    :raises LookupError: Se a tabela original não existir ou não estiver visível.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        columns = read_column_definitions(cursor, template_schema, table)
    if not columns:
        raise LookupError(
            f"Tabela original {template_schema}.{table} não encontrada; ela define os tipos da tabela de destino."
        )
    return columns


def connect(config: OracleConfig) -> oracledb.Connection:
    """Abre uma conexão em modo thick com o Oracle.

    O modo thick usa o Instant Client da imagem base e aceita usuários com
    verifier de senha 10G, que o modo thin rejeita (``DPY-3015``).

    :param config: Configuração da conexão.
    :returns: Conexão aberta.
    """
    if oracledb.is_thin_mode():
        oracledb.init_oracle_client()
    return oracledb.connect(user=config.user, password=config.password, dsn=config.dsn)


def definition_differences(existing: list[str], expected: list[str]) -> list[str]:
    """Lista, posição a posição, as colunas cuja definição difere da original.

    :param existing: Definições das colunas da tabela existente.
    :param expected: Definições das colunas da tabela original.
    :returns: Diferenças no formato ``atual → esperada``; vazia se forem iguais.
    """
    size = max(len(existing), len(expected))
    padded_existing = existing + [""] * (size - len(existing))
    padded_expected = expected + [""] * (size - len(expected))
    return [
        f"{found or '(ausente)'} → {wanted or '(ausente)'}"
        for found, wanted in zip(padded_existing, padded_expected, strict=True)
        if found != wanted
    ]


def create_managed_table(
    cursor: oracledb.Cursor, schema: str, table: str, columns: list[OracleColumn], source: str
) -> None:
    """Cria a tabela de destino e grava a marca da pipeline no comentário.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema da tabela.
    :param table: Nome da tabela.
    :param columns: Colunas, na ordem da tabela original.
    :param source: Tabela de origem, gravada no comentário.
    """
    cursor.execute(
        load_query(QUERIES_ANCHOR, "create_table", schema=schema, table=table, columns=column_definitions(columns))
    )
    comment = f"{MANAGED_TABLE_MARKER}: carga a partir de {source}".replace("'", "''")
    cursor.execute(load_query(QUERIES_ANCHOR, "comment_on_table", schema=schema, table=table, comment=comment))


def ensure_table(config: OracleConfig, table: str, columns: list[OracleColumn], source: str) -> str:
    """Deixa a tabela de destino com a mesma definição da tabela original.

    Cria a tabela se ela não existir. Se existir e tiver sido criada pela
    pipeline (prefixo e marca no comentário), é reaproveitada quando a definição
    é igual à original, ou apagada e recriada quando diverge. Tabelas sem a
    marca da pipeline nunca são alteradas.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :param columns: Colunas da tabela original.
    :param source: Tabela de origem, gravada no comentário.
    :returns: O que foi feito, em texto, para registrar no log.
    :raises PermissionError: Se a sessão não tiver os privilégios necessários ou
        se a tabela existir sem a marca da pipeline.
    """
    target = f"{config.schema}.{table}"
    binds = {"owner": config.schema, "table_name": table}
    with connect(config) as connection, connection.cursor() as cursor:
        assert_privileges(cursor, config.schema)
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_comment"), binds)
        row = cursor.fetchone()
        if row is None:
            create_managed_table(cursor, config.schema, table, columns, source)
            return f"Tabela {target} criada com a definição da original ({len(columns)} colunas)."

        assert_managed_table(table, row[1])
        existing = [column.definition for column in read_column_definitions(cursor, config.schema, table)]
        differences = definition_differences(existing, [column.definition for column in columns])
        if not differences:
            return f"Tabela {target} já existe com a definição da original e será recarregada."

        cursor.execute(load_query(QUERIES_ANCHOR, "drop_table", schema=config.schema, table=table))
        create_managed_table(cursor, config.schema, table, columns, source)
        return f"Tabela {target} divergia da original e foi apagada e recriada. Diferenças: {differences}"


def truncate_table(config: OracleConfig, table: str) -> None:
    """Esvazia a tabela de destino, após confirmar que ela pertence à pipeline.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_comment"), {"owner": config.schema, "table_name": table})
        row = cursor.fetchone()
        if row is None:
            raise PermissionError(f"{config.schema}.{table} não existe.")
        assert_managed_table(table, row[1])
        cursor.execute(load_query(QUERIES_ANCHOR, "truncate_table", schema=config.schema, table=table))
    logger.info("Tabela %s.%s esvaziada", config.schema, table)


def count_rows(config: OracleConfig, table: str) -> int:
    """Conta as linhas da tabela de destino.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :returns: Número de linhas.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(load_query(QUERIES_ANCHOR, "count_rows", schema=config.schema, table=table))
        (count,) = cursor.fetchone()
    return int(count)
