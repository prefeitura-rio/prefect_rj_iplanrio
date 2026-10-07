from datetime import UTC, datetime

import oracledb
import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import (
    SNAPSHOT_SOURCES,
    SnapshotError,
    snapshot_from_sources,
)
from prefect_rj_iplanrio.sql import load_query

TAKEN_AT = datetime(2026, 10, 2, 12, 30, 5)
NO_V_DATABASE = "ORA-00942: table or view does not exist"
NO_FLASHBACK = 'ORA-00904: "DBMS_FLASHBACK"."GET_SYSTEM_CHANGE_NUMBER": invalid identifier'


class FakeOracle:
    """Responde por consulta: devolve a linha de SCN ou levanta o erro ORA configurado."""

    def __init__(self, scn_by_query: dict[str, int], errors_by_query: dict[str, str]) -> None:
        self.scn_by_query = scn_by_query
        self.errors_by_query = errors_by_query
        self.calls: list[str] = []

    def fetch(self, query: str) -> list[dict[str, object]]:
        self.calls.append(query)
        if query in self.errors_by_query:
            raise oracledb.DatabaseError(f"{self.errors_by_query[query]}\nHelp: https://docs.oracle.com/error-help/db/")
        return [{"scn": self.scn_by_query[query], "taken_at": TAKEN_AT}]


def test_snapshot_uses_v_database_when_available() -> None:
    # Given a user that can read v$database and DBMS_FLASHBACK
    oracle = FakeOracle({"get_snapshot": 111, "get_snapshot_flashback": 222}, {})
    # When the snapshot is read
    snapshot = snapshot_from_sources(oracle.fetch, SNAPSHOT_SOURCES)
    # Then v$database wins and the fallback is never queried
    assert (snapshot.scn, snapshot.source, snapshot.taken_at.tzinfo) == (111, "v$database", UTC)
    assert oracle.calls == ["get_snapshot"]


def test_snapshot_falls_back_to_flashback_when_v_database_fails() -> None:
    # Given a user without access to v$database
    oracle = FakeOracle({"get_snapshot_flashback": 222}, {"get_snapshot": NO_V_DATABASE})
    # When the snapshot is read
    snapshot = snapshot_from_sources(oracle.fetch, SNAPSHOT_SOURCES)
    # Then the DBMS_FLASHBACK value is used, after trying v$database first
    assert (snapshot.scn, snapshot.source) == (222, "DBMS_FLASHBACK")
    assert oracle.calls == ["get_snapshot", "get_snapshot_flashback"]


def test_snapshot_error_lists_both_ora_errors_and_grants_when_both_fail() -> None:
    # Given a user with neither source
    oracle = FakeOracle({}, {"get_snapshot": NO_V_DATABASE, "get_snapshot_flashback": NO_FLASHBACK})
    # When the snapshot is read
    with pytest.raises(SnapshotError) as excinfo:
        snapshot_from_sources(oracle.fetch, SNAPSHOT_SOURCES)
    # Then the message carries both ORA lines and the GRANT instructions
    message = str(excinfo.value)
    assert NO_V_DATABASE in message
    assert NO_FLASHBACK in message
    assert "Peça à DBA: GRANT SELECT ON SYS.V_$DATABASE ou GRANT EXECUTE ON SYS.DBMS_FLASHBACK" in message


def test_snapshot_queries_read_v_database_and_flashback() -> None:
    # Given the rendered snapshot queries
    primary = load_query(QUERIES_ANCHOR, "get_snapshot")
    fallback = load_query(QUERIES_ANCHOR, "get_snapshot_flashback")
    # Then the primary needs no DBMS_FLASHBACK and the fallback does
    assert "v$database" in primary
    assert "DBMS_FLASHBACK" not in primary
    assert "DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER" in fallback
