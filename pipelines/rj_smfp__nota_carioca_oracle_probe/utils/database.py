"""Informações do banco: versão, papel, parâmetros e undo, cada consulta independente."""

from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import OracleConfig, to_int
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.session import connect_read_only
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.softquery import SoftRows, soft_rows

DEFAULT_BLOCK_SIZE = 8192
# (título no relatório, view consultada, arquivo em queries/)
SECTION_SPECS = (
    ("Versão (v$version)", "v$version", "get_version"),
    ("Componentes (product_component_version)", "product_component_version", "get_product_version"),
    ("Banco (v$database)", "v$database", "get_database"),
    ("Parâmetros (v$parameter)", "v$parameter", "get_parameters"),
    ("Undo, últimos 7 dias (v$undostat)", "v$undostat", "get_undostat_summary"),
    ("Tablespaces de undo (dba_tablespaces)", "dba_tablespaces", "get_undo_tablespaces"),
    ("Arquivos do undo (dba_data_files)", "dba_data_files", "get_undo_datafiles"),
)


@dataclass(frozen=True)
class DatabaseInfo:
    """Resultado das consultas ao dicionário do banco.

    :param sections: Título e resultado de cada consulta, na ordem do relatório.
    :param block_size: Tamanho do bloco, em bytes.
    :param block_size_source: De onde veio o tamanho do bloco.
    """

    sections: tuple[tuple[str, SoftRows], ...]
    block_size: int
    block_size_source: str


def block_size_from_parameters(parameters: SoftRows) -> int | None:
    """Extrai ``db_block_size`` do resultado de ``v$parameter``.

    :param parameters: Resultado da consulta de parâmetros.
    :returns: Bytes, ou ``None`` se a consulta falhou ou o parâmetro não veio.
    """
    for row in parameters.rows or ():
        if row["name"] == "db_block_size":
            return to_int(row["value"])
    return None


def collect_database(config: OracleConfig) -> DatabaseInfo:
    """Consulta o banco; a falta de privilégio em uma view não impede as demais.

    :param config: Configuração da conexão.
    :returns: Seções do relatório e o tamanho do bloco.
    """
    with connect_read_only(config) as connection, connection.cursor() as cursor:
        sections = tuple((title, soft_rows(cursor, view, query)) for title, view, query in SECTION_SPECS)
        block_size = block_size_from_parameters(dict(sections)["Parâmetros (v$parameter)"])
        if block_size is not None:
            return DatabaseInfo(sections, block_size, "v$parameter")
        fallback = soft_rows(cursor, "user_tablespaces", "get_block_size")
    for row in fallback.rows or ():
        if row["block_size"] is not None:
            return DatabaseInfo(sections, to_int(row["block_size"]), "user_tablespaces")
    return DatabaseInfo(
        sections, DEFAULT_BLOCK_SIZE, f"assumido ({DEFAULT_BLOCK_SIZE}); {fallback.note or 'sem dados'}"
    )
