"""Acesso ao Oracle de destino: configuração, criação protegida e contagem de tabelas."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import oracledb

from iplanrio.pipelines_utils.env import getenv_or_action
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import OracleColumn, oracle_column
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.structure import (
    IndexDefinition,
    TableLayout,
    index_from_dictionary,
    index_statement_parts,
    inmemory_from_dictionary,
    layout_differences,
    partitioning_from_dictionary,
    storage_clause,
)
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)

TABLE_PREFIX = "BQLOAD_"
MANAGED_TABLE_MARKER = "rj_smfp__nota_carioca_bq_to_oracle"
# Os sinônimos de grant_access ficam em outros schemas, o que exige CREATE ANY SYNONYM até do dono.
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
# Um CREATE INDEX grande passa minutos sem tráfego na rede; a sonda evita que um firewall derrube a conexão ociosa.
KEEPALIVE_MINUTES = 2
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
    cursor.execute(load_query(QUERIES_ANCHOR, "get_session_privileges"))
    rows = cursor.fetchall()
    session_user = rows[0][0] if rows else ""
    missing = missing_privileges(session_user, schema, {privilege for _, privilege in rows})
    if missing and session_user == schema:
        raise PermissionError(f"O usuário {schema} não tem {missing}. Nada foi alterado. Peça à DBA.")
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


def fetch_rows(cursor: oracledb.Cursor, query: str, binds: Mapping[str, object]) -> list[dict[str, object]]:
    """Executa uma consulta e retorna as linhas como dicionários.

    :param cursor: Cursor de uma conexão aberta.
    :param query: Nome do arquivo em ``queries/``, sem a extensão.
    :param binds: Valores das variáveis de bind.
    :returns: Linhas com as colunas em minúsculas.
    """
    cursor.execute(load_query(QUERIES_ANCHOR, query), binds)
    names = [description[0].lower() for description in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def read_table_layout(cursor: oracledb.Cursor, owner: str, table: str) -> TableLayout | None:
    """Lê tablespace, particionamento, índices e In-Memory de uma tabela no dicionário do Oracle.

    :param cursor: Cursor de uma conexão aberta.
    :param owner: Dono da tabela.
    :param table: Nome da tabela.
    :returns: Estrutura da tabela, ou ``None`` se ela não existir ou não estiver
        visível para a sessão.
    """
    binds = {"owner": owner, "table_name": table}
    storage = fetch_rows(cursor, "get_table_tablespace", binds)
    if not storage:
        return None
    partitioning = None
    part_table = fetch_rows(cursor, "get_partitioning", binds)
    if part_table:
        keys = [str(row["column_name"]) for row in fetch_rows(cursor, "get_partition_key_columns", binds)]
        partitions = fetch_rows(cursor, "get_table_partitions", binds)
        partitioning = partitioning_from_dictionary(part_table[0], keys, partitions)

    columns: dict[tuple[object, object], list[str]] = {}
    for row in fetch_rows(cursor, "get_index_columns", binds):
        columns.setdefault((row["index_owner"], row["index_name"]), []).append(str(row["column_name"]))
    indexes = tuple(
        index_from_dictionary(row, columns.get((row["owner"], row["index_name"]), []))
        for row in fetch_rows(cursor, "get_indexes", binds)
    )
    tablespace = storage[0]["tablespace_name"] or storage[0]["def_tablespace_name"]
    return TableLayout(
        tablespace=None if tablespace is None else str(tablespace),
        partitioning=partitioning,
        indexes=indexes,
        inmemory=inmemory_from_dictionary(storage[0]),
    )


def fetch_template_layout(config: OracleConfig, template_schema: str, table: str) -> TableLayout:
    """Lê tablespace, particionamento e índices da tabela original.

    :param config: Configuração da conexão.
    :param template_schema: Schema da tabela original.
    :param table: Nome da tabela original.
    :returns: Estrutura da tabela original.
    :raises LookupError: Se a tabela original não existir ou não estiver visível.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        layout = read_table_layout(cursor, template_schema, table)
    if layout is None:
        raise LookupError(f"Tabela original {template_schema}.{table} não encontrada.")
    return layout


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


@dataclass(frozen=True)
class TableDefinition:
    """Definição completa da tabela de destino.

    :param columns: Colunas, na ordem da tabela original.
    :param layout: Tablespace e particionamento da tabela original.
    :param source: Tabela de origem, gravada no comentário.
    """

    columns: list[OracleColumn]
    layout: TableLayout
    source: str


def create_managed_table(cursor: oracledb.Cursor, schema: str, table: str, definition: TableDefinition) -> None:
    """Cria a tabela de destino e grava a marca da pipeline no comentário.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema da tabela.
    :param table: Nome da tabela.
    :param definition: Colunas, tablespace, particionamento e origem.
    """
    cursor.execute(
        load_query(
            QUERIES_ANCHOR,
            "create_table",
            schema=schema,
            table=table,
            columns=column_definitions(definition.columns),
            storage=storage_clause(definition.layout),
        )
    )
    comment = f"{MANAGED_TABLE_MARKER}: carga a partir de {definition.source}".replace("'", "''")
    cursor.execute(load_query(QUERIES_ANCHOR, "comment_on_table", schema=schema, table=table, comment=comment))


def ensure_table(config: OracleConfig, table: str, definition: TableDefinition) -> str:
    """Deixa a tabela de destino com a mesma definição da tabela original.

    Cria a tabela se ela não existir. Se existir e tiver sido criada pela
    pipeline (prefixo e marca no comentário), é reaproveitada quando colunas,
    tablespace e partições são iguais aos da original, ou apagada e recriada
    quando divergem. Tabelas sem a marca da pipeline nunca são alteradas.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :param definition: Colunas, tablespace, particionamento e origem.
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
            create_managed_table(cursor, config.schema, table, definition)
            return f"Tabela {target} criada com a definição da original ({len(definition.columns)} colunas)."

        assert_managed_table(table, row[1])
        existing = [column.definition for column in read_column_definitions(cursor, config.schema, table)]
        differences = definition_differences(existing, [column.definition for column in definition.columns])
        existing_layout = read_table_layout(cursor, config.schema, table)
        if existing_layout is not None:
            differences += layout_differences(existing_layout, definition.layout)
        if not differences:
            return f"Tabela {target} já existe com a definição da original e será recarregada."

        cursor.execute(load_query(QUERIES_ANCHOR, "drop_table", schema=config.schema, table=table))
        create_managed_table(cursor, config.schema, table, definition)
        return f"Tabela {target} divergia da original e foi apagada e recriada. Diferenças: {differences}"


def assert_managed_existing_table(cursor: oracledb.Cursor, schema: str, table: str) -> None:
    """Confirma que a tabela existe e pertence à pipeline antes de alterá-la.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema da tabela.
    :param table: Nome da tabela.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    cursor.execute(load_query(QUERIES_ANCHOR, "get_table_comment"), {"owner": schema, "table_name": table})
    row = cursor.fetchone()
    if row is None:
        raise PermissionError(f"{schema}.{table} não existe.")
    assert_managed_table(table, row[1])


def truncate_table(config: OracleConfig, table: str) -> None:
    """Esvazia a tabela de destino, após confirmar que ela pertence à pipeline.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        cursor.execute(load_query(QUERIES_ANCHOR, "truncate_table", schema=config.schema, table=table))
    logger.info("Tabela %s.%s esvaziada", config.schema, table)


def foreign_indexes(indexes: list[IndexDefinition], schema: str) -> list[str]:
    """Lista os índices que a pipeline não criou e, por isso, não pode apagar.

    :param indexes: Índices da tabela de destino.
    :param schema: Schema da tabela de destino.
    :returns: Nomes, no formato ``dono.índice``, dos índices fora do schema ou sem
        o prefixo da pipeline.
    """
    return [
        f"{index.owner}.{index.name}"
        for index in indexes
        if index.owner != schema or not index.name.startswith(TABLE_PREFIX)
    ]


def drop_managed_indexes(config: OracleConfig, table: str) -> list[str]:
    """Apaga os índices da tabela de destino, que a carga paralela não aceita.

    Só apaga índices com o prefixo da pipeline, no schema de destino e em tabela
    criada pela pipeline. Se houver qualquer outro índice, nada é apagado.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :returns: Nomes dos índices apagados.
    :raises PermissionError: Se a tabela não pertencer à pipeline ou tiver índice
        que a pipeline não criou.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        layout = read_table_layout(cursor, config.schema, table)
        indexes = list(layout.indexes) if layout else []
        others = foreign_indexes(indexes, config.schema)
        if others:
            raise PermissionError(
                f"{config.schema}.{table} tem índices que a pipeline não criou: {others}. Nada foi alterado; "
                "a carga em paralelo exige a tabela sem índices."
            )
        for index in indexes:
            cursor.execute(load_query(QUERIES_ANCHOR, "drop_index", schema=config.schema, index=index.name))
    return [index.name for index in indexes]


def create_index(config: OracleConfig, table: str, index: IndexDefinition, parallel_degree: int) -> None:
    """Cria um índice na tabela de destino e depois tira o paralelismo dele.

    O ``PARALLEL`` acelera a criação, mas, se ficasse gravado no índice, faria o
    otimizador usar consultas paralelas em quem lê a tabela.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :param index: Índice a criar, já com o nome da tabela de destino.
    :param parallel_degree: Grau de paralelismo da criação.
    :raises PermissionError: Se a tabela não pertencer à pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        parts = index_statement_parts(index, parallel_degree)
        cursor.execute(
            load_query(QUERIES_ANCHOR, "create_index", schema=config.schema, table=table, index=index.name, **parts)
        )
        cursor.execute(load_query(QUERIES_ANCHOR, "disable_index_parallel", schema=config.schema, index=index.name))


def gather_table_stats(config: OracleConfig, table: str, parallel_degree: int) -> None:
    """Coleta as estatísticas da tabela de destino para o otimizador.

    As estatísticas dos índices já são calculadas no ``CREATE INDEX``.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :param parallel_degree: Grau de paralelismo da coleta.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            load_query(QUERIES_ANCHOR, "gather_table_stats"),
            {"owner": config.schema, "table_name": table, "degree": parallel_degree},
        )


def grant_access(config: OracleConfig, table: str) -> None:
    """Concede o acesso dos consumidores à tabela e cria os sinônimos deles.

    O ``DROP`` de ``ensure_table`` apaga os grants, então a concessão roda após
    toda carga. Os comandos podem ser repetidos sem efeito colateral.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        cursor.execute(load_query(QUERIES_ANCHOR, "grant_access", schema=config.schema, table=table))
    logger.info("Acesso concedido em %s.%s", config.schema, table)


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
