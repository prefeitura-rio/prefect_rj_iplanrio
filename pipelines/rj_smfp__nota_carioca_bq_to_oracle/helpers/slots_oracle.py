"""Operações no Oracle sobre os slots: sinônimos, permissões e registro da carga."""

from datetime import UTC, datetime

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import (
    OracleConfig,
    assert_managed_table,
    assert_privileges,
    connect,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_ddl import assert_managed_existing_table
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_query import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_read import fetch_rows
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.slots import (
    CONSUMER_GRANTS,
    CONSUMER_SYNONYM_OWNERS,
    REPLACEABLE_OBJECT_TYPES,
    SlotPlan,
    SynonymTarget,
    choose_slots,
    load_comment,
    synonym_owners,
    synonyms_to_realign,
)
from utils.utils import render_query


def read_synonyms(cursor: oracledb.Cursor, schema: str, base: str) -> list[SynonymTarget]:
    """Lê os sinônimos da pipeline com o nome estável da tabela.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema das tabelas físicas.
    :param base: Nome estável da tabela.
    :returns: Sinônimos do schema de destino e dos consumidores.
    """
    owners = synonym_owners(schema)
    return [
        SynonymTarget(owner=str(row["owner"]), table_owner=str(row["table_owner"]), table_name=str(row["table_name"]))
        for row in fetch_rows(cursor, "get_synonyms", {"synonym_name": base})
        if row["owner"] in owners
    ]


def create_synonym(cursor: oracledb.Cursor, owner: str, base: str, schema: str, table: str) -> None:
    """Cria ou repõe um sinônimo apontando para uma tabela física.

    :param cursor: Cursor de uma conexão aberta.
    :param owner: Dono do sinônimo.
    :param base: Nome do sinônimo.
    :param schema: Schema da tabela física.
    :param table: Tabela física.
    """
    cursor.execute(
        render_query(QUERIES_ANCHOR, "create_synonym", owner=owner, synonym=base, schema=schema, table=table)
    )


def assert_replaceable_legacy_table(cursor: oracledb.Cursor, schema: str, base: str) -> bool:
    """Confere se a tabela única antiga, se existir, pode dar lugar ao sinônimo.

    Roda antes de qualquer alteração, para que uma tabela que a pipeline não
    criou com o nome ``BQLOAD_<tabela>`` interrompa o run sem mexer em nada.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema das tabelas físicas.
    :param base: Nome estável da tabela.
    :returns: Se a tabela única antiga existe.
    :raises PermissionError: Se a tabela existir sem a marca da pipeline, ou se
        houver com esse nome outro tipo de objeto, que impediria criar o sinônimo.
    """
    types = {
        str(row["object_type"])
        for row in fetch_rows(cursor, "get_objects_named", {"owner": schema, "object_name": base})
    }
    others = sorted(types - REPLACEABLE_OBJECT_TYPES)
    if others:
        raise PermissionError(
            f"Existe {others} com o nome {schema}.{base}, que impede criar o sinônimo. Nada foi alterado."
        )
    rows = fetch_rows(cursor, "get_table_comment", {"owner": schema, "table_name": base})
    if not rows:
        return False
    comment = rows[0]["comments"]
    assert_managed_table(base, None if comment is None else str(comment))
    return True


def resolve_slots(config: OracleConfig, base: str) -> tuple[SlotPlan, list[str]]:
    """Descobre a tabela em uso e a que recebe a carga, corrigindo trocas interrompidas.

    :param config: Configuração da conexão.
    :param base: Nome estável da tabela.
    :returns: Plano das tabelas e donos dos sinônimos repontados para a tabela em
        uso.
    :raises PermissionError: Se faltar privilégio, se algum sinônimo apontar para
        um objeto que a pipeline não gerencia ou se existir uma tabela
        ``BQLOAD_<tabela>`` que a pipeline não criou.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_privileges(cursor, config.schema)
        assert_replaceable_legacy_table(cursor, config.schema, base)
        synonyms = read_synonyms(cursor, config.schema, base)
        plan = choose_slots(base, config.schema, synonyms)
        realigned = synonyms_to_realign(plan, synonyms)
        for owner in realigned:
            create_synonym(cursor, owner, base, config.schema, str(plan.active))
    return plan, realigned


def grant_access(config: OracleConfig, table: str) -> None:
    """Concede aos consumidores o acesso à tabela física.

    Roda a cada carga, antes da troca: recriar a tabela apaga os grants. Os
    comandos podem ser repetidos sem efeito colateral.

    :param config: Configuração da conexão.
    :param table: Tabela física.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        for grantee, privileges in CONSUMER_GRANTS:
            cursor.execute(
                render_query(
                    QUERIES_ANCHOR,
                    "grant_on_table",
                    privileges=privileges,
                    schema=config.schema,
                    table=table,
                    grantee=grantee,
                )
            )


def record_load(config: OracleConfig, table: str, source: str, snapshot_modified: datetime) -> str:
    """Grava no comentário da tabela física qual foto do BigQuery ela contém.

    :param config: Configuração da conexão.
    :param table: Tabela física.
    :param source: Tabela de origem no BigQuery.
    :param snapshot_modified: Última alteração do BigQuery na foto carregada.
    :returns: Comentário gravado.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    comment = load_comment(source, snapshot_modified, datetime.now(UTC))
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        cursor.execute(
            render_query(
                QUERIES_ANCHOR,
                "comment_on_table",
                schema=config.schema,
                table=table,
                comment=comment.replace("'", "''"),
            )
        )
    return comment


def swap_synonyms(config: OracleConfig, plan: SlotPlan) -> list[str]:
    """Aponta os sinônimos para a tabela recém-carregada.

    Os sinônimos dos consumidores são trocados primeiro e o do schema de destino
    por último, porque é ele que define a tabela em uso no próximo run. Na
    primeira troca, a antiga tabela única ``BQLOAD_<tabela>`` é apagada para dar
    lugar ao sinônimo de mesmo nome; só nesse instante quem lê
    ``<schema>.BQLOAD_<tabela>`` direto fica sem o objeto, por milissegundos.

    :param config: Configuração da conexão.
    :param plan: Tabela em uso e tabela recém-carregada.
    :returns: Ações executadas, em texto, para o log.
    :raises PermissionError: Se a tabela carregada ou a tabela única antiga não
        pertencerem à pipeline; a conferência acontece antes de qualquer
        sinônimo ser repontado.
    """
    actions = []
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, plan.inactive)
        legacy = assert_replaceable_legacy_table(cursor, config.schema, plan.base)
        for owner in CONSUMER_SYNONYM_OWNERS:
            create_synonym(cursor, owner, plan.base, config.schema, plan.inactive)
            actions.append(f"{owner}.{plan.base} → {plan.inactive}")
        if legacy:
            cursor.execute(render_query(QUERIES_ANCHOR, "drop_table", schema=config.schema, table=plan.base))
            actions.append(f"tabela única antiga {config.schema}.{plan.base} apagada")
        create_synonym(cursor, config.schema, plan.base, config.schema, plan.inactive)
        actions.append(f"{config.schema}.{plan.base} → {plan.inactive}")
    return actions


def refresh_synonyms(config: OracleConfig, base: str) -> list[str]:
    """Reaponta todos os sinônimos para a tabela física já em uso, sem carga.

    Serve para criar sinônimos de um dono novo ou corrigir um sinônimo removido
    sem esperar uma carga completa. Os grants dos consumidores são reaplicados na
    tabela em uso antes, para o sinônimo novo já funcionar.

    :param config: Configuração da conexão.
    :param base: Nome estável da tabela.
    :returns: Ações executadas, em texto, para o log.
    :raises LookupError: Se ainda não houver tabela A/B em uso.
    :raises PermissionError: Se faltar privilégio, se algum sinônimo apontar para
        um objeto que a pipeline não gerencia ou se a migração para A/B não tiver
        terminado (a tabela única antiga ainda existe).
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_privileges(cursor, config.schema)
        if assert_replaceable_legacy_table(cursor, config.schema, base):
            raise PermissionError(
                f"{config.schema}.{base} ainda é a tabela única antiga; rode a carga completa (mode=full) "
                + "para terminar a migração para A/B. Nada foi alterado."
            )
        plan = choose_slots(base, config.schema, read_synonyms(cursor, config.schema, base))
        if plan.active is None:
            raise LookupError(f"{base}: nenhuma tabela A/B em uso; rode a carga completa (mode=full).")
        assert_managed_existing_table(cursor, config.schema, plan.active)
    grant_access(config, plan.active)
    actions = []
    with connect(config) as connection, connection.cursor() as cursor:
        for owner in (*CONSUMER_SYNONYM_OWNERS, config.schema):
            create_synonym(cursor, owner, base, config.schema, plan.active)
            actions.append(f"{owner}.{base} → {plan.active}")
    return actions
