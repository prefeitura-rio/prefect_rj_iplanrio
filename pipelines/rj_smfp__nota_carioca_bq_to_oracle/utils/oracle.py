"""Acesso ao Oracle de destino: configuração, criação protegida e contagem de tabelas."""

import re
from dataclasses import dataclass
from pathlib import Path

import oracledb

from iplanrio.pipelines_utils.env import getenv_or_action
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import Column
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)

TABLE_PREFIX = "BQLOAD_"
MANAGED_TABLE_MARKER = "rj_smfp__nota_carioca_bq_to_oracle"
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


def column_definitions(columns: list[Column]) -> str:
    """Monta a lista de colunas do ``CREATE TABLE``.

    :param columns: Colunas da tabela.
    :returns: Fragmento SQL com uma coluna por linha.
    """
    return ",\n".join(f'  "{column.name}" {column.oracle_type}' for column in columns)


def connect(config: OracleConfig) -> oracledb.Connection:
    """Abre uma conexão em modo thin com o Oracle.

    :param config: Configuração da conexão.
    :returns: Conexão aberta.
    """
    return oracledb.connect(user=config.user, password=config.password, dsn=config.dsn)


def ensure_table(config: OracleConfig, table: str, columns: list[Column], source: str) -> None:
    """Cria a tabela de destino se ela não existir, ou confere a existente.

    Uma tabela existente só é aceita se tiver a marca da pipeline no comentário
    e as mesmas colunas, na mesma ordem, do schema atual do BigQuery.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :param columns: Colunas esperadas.
    :param source: Tabela de origem, gravada no comentário.
    :raises PermissionError: Se a tabela existir sem a marca da pipeline.
    :raises ValueError: Se as colunas da tabela existente divergirem do schema.
    """
    binds = {"owner": config.schema, "table_name": table}
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_comment"), binds)
        row = cursor.fetchone()
        if row is None:
            cursor.execute(
                load_query(
                    QUERIES_ANCHOR,
                    "create_table",
                    schema=config.schema,
                    table=table,
                    columns=column_definitions(columns),
                )
            )
            comment = f"{MANAGED_TABLE_MARKER}: carga a partir de {source}".replace("'", "''")
            cursor.execute(
                load_query(QUERIES_ANCHOR, "comment_on_table", schema=config.schema, table=table, comment=comment)
            )
            logger.info("Tabela %s.%s criada com %d colunas", config.schema, table, len(columns))
            return

        assert_managed_table(table, row[1])
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_columns"), binds)
        existing = [column_name for (column_name,) in cursor.fetchall()]
        expected = [column.name for column in columns]
        if existing != expected:
            raise ValueError(
                f"As colunas de {config.schema}.{table} divergem do schema do BigQuery. "
                f"Existentes: {existing}. Esperadas: {expected}."
            )
        logger.info("Tabela %s.%s já existe e foi criada pela pipeline", config.schema, table)


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
