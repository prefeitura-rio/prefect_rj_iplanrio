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
    """Read the synonyms of the pipeline that have the stable name of the table.

    Return those of the target schema and of the consumers.
    """
    owners = synonym_owners(schema)
    return [
        SynonymTarget(owner=str(row["owner"]), table_owner=str(row["table_owner"]), table_name=str(row["table_name"]))
        for row in fetch_rows(cursor, "get_synonyms", {"synonym_name": base})
        if row["owner"] in owners
    ]


def create_synonym(cursor: oracledb.Cursor, owner: str, base: str, schema: str, table: str) -> None:
    """Create or replace a synonym that points to a physical table."""
    cursor.execute(
        render_query(QUERIES_ANCHOR, "create_synonym", owner=owner, synonym=base, schema=schema, table=table)
    )


def assert_replaceable_legacy_table(cursor: oracledb.Cursor, schema: str, base: str) -> bool:
    """Check that the old single table, if it exists, can give way to the synonym.

    It runs before any change, so a table that the pipeline did not create with the name
    `BQLOAD_<table>` stops the run without touching anything. Return whether the old
    single table exists. Raise `PermissionError` if the table exists without the mark of
    the pipeline, or if another kind of object has that name, which would block the
    synonym.
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
    """Find the table in use and the one that loads, and fix interrupted swaps.

    Return the plan of the tables and the owners of the synonyms that were pointed again
    to the table in use. Raise `PermissionError` if a privilege is missing, if a synonym
    points to an object that the pipeline does not manage, or if a table
    `BQLOAD_<table>` exists that the pipeline did not create.
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
    """Grant the consumers access to the physical table."""
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
    """Write in the comment of the physical table which BigQuery snapshot it holds.

    `snapshot_modified` is the last change of BigQuery in the loaded snapshot. Return
    the comment that was written. Raise `PermissionError` if the table does not exist or
    does not belong to the pipeline.
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
    """Point the synonyms to the table that was just loaded.

    The synonyms of the consumers change first, and the one of the target schema last,
    because that one defines the table in use in the next run. In the first swap, the
    old single table `BQLOAD_<table>` is dropped to make room for the synonym of the
    same name. Only at that moment, for milliseconds, whoever reads
    `<schema>.BQLOAD_<table>` directly has no object. Return the actions that ran, as
    text for the log. Raise `PermissionError` if the loaded table or the old single
    table does not belong to the pipeline. The check happens before any synonym points
    elsewhere.
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
    """Point all the synonyms again to the physical table that is in use, with no load.

    It serves to create the synonyms of a new owner, or to fix a removed synonym,
    without a full load. The grants of the consumers are applied again on the table in
    use first, so the new synonym works at once. Return the actions that ran, as text
    for the log. Raise `LookupError` if no A or B table is in use yet. Raise
    `PermissionError` if a privilege is missing, if a synonym points to an object that
    the pipeline does not manage, or if the migration to A and B has not finished (the
    old single table still exists).
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
