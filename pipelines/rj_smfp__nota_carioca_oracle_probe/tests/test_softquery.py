from collections.abc import Callable

import oracledb

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.softquery import failure_note, grant_name, soft_call


def raising(message: str) -> Callable[[], list[dict[str, object]]]:
    def call() -> list[dict[str, object]]:
        raise oracledb.DatabaseError(message)

    return call


def test_missing_view_becomes_privilege_note_and_run_continues() -> None:
    result = soft_call("v$parameter", raising("ORA-00942: table or view does not exist"))

    assert result.rows is None
    assert result.note == (
        "sem privilégio para v$parameter — peça à DBA: GRANT SELECT ON SYS.V_$PARAMETER / SELECT_CATALOG_ROLE"
    )


def test_insufficient_privileges_is_also_a_privilege_note() -> None:
    note = failure_note("dba_segments", oracledb.DatabaseError("ORA-01031: insufficient privileges"))

    assert note.startswith("sem privilégio para dba_segments")
    assert "SYS.DBA_SEGMENTS" in note


def test_other_database_error_keeps_its_reason() -> None:
    result = soft_call("v$undostat", raising("ORA-00904: invalid identifier\nmore"))

    assert result.note == "falha ao consultar v$undostat: ORA-00904: invalid identifier"


def test_successful_call_returns_rows_without_note() -> None:
    result = soft_call("v$version", lambda: [{"banner": "Oracle"}])

    assert result.rows == ({"banner": "Oracle"},)
    assert result.note is None


def test_grant_name_maps_dynamic_views() -> None:
    assert grant_name("v$database") == "SYS.V_$DATABASE"
    assert grant_name("dba_tablespaces") == "SYS.DBA_TABLESPACES"
    assert grant_name("user_tablespaces") == "USER_TABLESPACES"
