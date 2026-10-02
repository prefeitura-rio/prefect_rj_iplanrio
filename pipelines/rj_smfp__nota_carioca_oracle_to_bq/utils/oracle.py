"""Acesso ao Oracle de origem: configuração, conexão, foto consistente, metadados e contagem."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import oracledb

from iplanrio.pipelines_utils.env import getenv_or_action
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn, column_kind
from prefect_rj_iplanrio.sql import load_query

# Um COUNT ou um CREATE_CHUNKS grande passa minutos sem tráfego; a sonda evita que um firewall derrube a conexão.
KEEPALIVE_MINUTES = 2
IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")
# SCN em JSON é um número; acima de 2**53 os consumidores perderiam precisão.
MAX_JSON_SAFE_INTEGER = 2**53 - 1


@dataclass(frozen=True)
class OracleConfig:
    """Conexão com o Oracle de origem, lida do Infisical.

    :param user: Usuário do banco.
    :param password: Senha do usuário.
    :param host: Host ou IP do banco.
    :param port: Porta do listener.
    :param service_name: Service name do banco.
    :param schema: Schema dono das tabelas de origem.
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


@dataclass(frozen=True)
class Snapshot:
    """Ponto de leitura consistente: todas as tabelas são lidas ``AS OF SCN``.

    :param scn: System change number no início da carga.
    :param taken_at: Horário (UTC) em que o SCN foi lido.
    """

    scn: int
    taken_at: datetime

    @property
    def sync_id(self) -> int:
        """Retorna o SCN como ``sync_id`` de ``_airbyte_meta``.

        O SCN é monotônico e único por foto, o que o torna um identificador
        determinístico da carga. Fica abaixo de ``2**53``, o limite de inteiros
        exatos em JSON.

        :raises ValueError: Se o SCN não couber num número JSON exato.
        """
        if self.scn > MAX_JSON_SAFE_INTEGER:
            raise ValueError(f"SCN {self.scn} excede o maior inteiro exato em JSON.")
        return self.scn


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


def to_int(value: object) -> int:
    """Converte para inteiro um número lido do banco.

    :param value: Valor de uma coluna numérica.
    :returns: O valor inteiro.
    :raises TypeError: Se o valor não for numérico.
    """
    if isinstance(value, int | float | Decimal | str):
        return int(value)
    raise TypeError(f"Valor não numérico: {value!r}")


def fetch_rows(cursor: oracledb.Cursor, query: str, binds: dict[str, object]) -> list[dict[str, object]]:
    """Executa uma consulta de ``queries/`` e retorna as linhas como dicionários.

    :param cursor: Cursor de uma conexão aberta.
    :param query: Nome do arquivo em ``queries/``, sem a extensão.
    :param binds: Valores das variáveis de bind.
    :returns: Linhas com as colunas em minúsculas.
    """
    cursor.execute(load_query(QUERIES_ANCHOR, query), binds)
    if cursor.description is None:
        raise LookupError(f"A consulta {query} não retornou colunas.")
    names = [column.name.lower() for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def read_snapshot(config: OracleConfig) -> Snapshot:
    """Lê o SCN atual e o horário do banco, que fixam o ponto de leitura.

    :param config: Configuração da conexão.
    :returns: SCN e horário (UTC) lidos na mesma consulta.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        (row,) = fetch_rows(cursor, "get_snapshot", {})
    return Snapshot(scn=to_int(row["scn"]), taken_at=to_utc(row["taken_at"]))


def to_utc(value: object) -> datetime:
    """Marca como UTC um horário sem fuso devolvido por ``SYS_EXTRACT_UTC``.

    :param value: Valor da coluna ``TIMESTAMP``.
    :returns: O horário com fuso UTC.
    :raises TypeError: Se o valor não for um ``datetime``.
    """
    if not isinstance(value, datetime):
        raise TypeError(f"Horário esperado, recebido {value!r}")
    return value.replace(tzinfo=UTC)


def read_columns(config: OracleConfig, schema: str, table: str) -> tuple[OracleColumn, ...]:
    """Lê as colunas da tabela no dicionário de dados, na ordem da tabela.

    :param config: Configuração da conexão.
    :param schema: Dono da tabela.
    :param table: Nome da tabela.
    :returns: Colunas, com tipos validados como suportados.
    :raises LookupError: Se a tabela não existir ou não estiver visível.
    :raises NotImplementedError: Se alguma coluna tiver tipo não suportado.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        rows = fetch_rows(cursor, "get_columns", {"owner": validate_identifier(schema), "table_name": table})
    if not rows:
        raise LookupError(f"Tabela {schema}.{table} não encontrada ou sem permissão de leitura.")
    columns = tuple(
        OracleColumn(
            name=str(row["column_name"]),
            data_type=str(row["data_type"]),
            precision=None if row["data_precision"] is None else to_int(row["data_precision"]),
            scale=None if row["data_scale"] is None else to_int(row["data_scale"]),
        )
        for row in rows
    )
    for column in columns:
        column_kind(column)
    return columns


def count_as_of_scn(config: OracleConfig, schema: str, table: str, snapshot: Snapshot) -> int:
    """Conta as linhas da tabela exatamente como estavam no SCN da foto.

    :param config: Configuração da conexão.
    :param schema: Dono da tabela.
    :param table: Nome da tabela.
    :param snapshot: Foto de referência.
    :returns: Número de linhas.
    """
    sql = load_query(
        QUERIES_ANCHOR, "count_as_of_scn", schema=validate_identifier(schema), table=validate_identifier(table)
    )
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(sql, {"scn": snapshot.scn})
        row = cursor.fetchone()
    if row is None:
        raise LookupError(f"COUNT de {schema}.{table} não retornou linha.")
    return to_int(row[0])
