from datetime import UTC, datetime

import pytest
from toolkit.chunks import Chunk, Probe
from toolkit.executor.sources.backends.files import FilesSource
from toolkit.executor.sources.backends.mssql import MSSQLSource
from toolkit.executor.sources.backends.oracle import OracleSource
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.protocols import DBConn
from toolkit.types import Chunks, MergeSpec

from utils.modes import (
    Capture,
    Merge,
    Overwrite,
    Replace,
    build_ledger_key,
    cut_mode,
    pick_mode,
    pick_pending,
    pick_probe,
    pick_shard,
    plan_shards,
    probes,
    skips_unchanged,
    sweeps,
)
from utils.types import DatabaseDumpOptions, DumpMode, DumpOptions, Shard

RUN = datetime(2024, 3, 10, 9, 0, tzinfo=UTC)
DAYS: Chunks = {"column": "dia", "width": "day", "begin": "2024-03-01", "end": "2024-03-02"}
MERGE = MergeSpec(key=["id"])
CONN = DBConn("host", 5432, "db", "user", "password")
POSTGRES = PostgresSource(CONN)
MSSQL = MSSQLSource(CONN)
ORACLE = OracleSource(CONN)
FILES = FilesSource()
COLUMN = {"range_column": "updated_at"}


@pytest.mark.parametrize(
    ("dump_mode", "options", "expected"),
    [
        pytest.param("append", {}, Replace(None), id="an_append_replaces_its_chunks"),
        pytest.param("append", {"chunks": DAYS}, Replace(DAYS), id="an_append_with_chunks"),
        pytest.param("append", {"capture": RUN}, Capture(RUN), id="an_append_with_a_capture"),
        pytest.param("overwrite", {}, Overwrite(None), id="an_overwrite"),
        pytest.param("overwrite", {"chunks": DAYS}, Overwrite(DAYS), id="an_overwrite_with_chunks"),
        pytest.param("merge", {"merge": MERGE}, Merge(MERGE), id="a_merge"),
    ],
)
def test_the_mode_follows_the_dump_mode_and_the_options(dump_mode: DumpMode, options: DumpOptions, expected):
    """Pick the way to load, from the mode that the flow asks and the options that it gives."""
    assert pick_mode(dump_mode, options) == expected


@pytest.mark.parametrize(
    ("dump_mode", "options", "message"),
    [
        pytest.param("merge", {}, "merge mode and the merge options", id="a_merge_without_options"),
        pytest.param("append", {"merge": MERGE}, "merge mode and the merge options", id="merge_options_in_an_append"),
        pytest.param(
            "overwrite", {"merge": MERGE}, "merge mode and the merge options", id="merge_options_in_an_overwrite"
        ),
        pytest.param("merge", {"merge": MERGE, "chunks": DAYS}, "one chunk", id="a_merge_with_chunks"),
        pytest.param("merge", {"merge": MERGE, "capture": RUN}, "one chunk", id="a_merge_with_a_capture"),
        pytest.param("append", {"capture": RUN, "chunks": DAYS}, "capture is added", id="a_capture_with_chunks"),
        pytest.param("overwrite", {"capture": RUN}, "capture is added", id="a_capture_with_an_overwrite"),
        pytest.param("append", {"shard": {"index": 0, "count": 2}}, "set chunks", id="a_shard_without_chunks"),
        pytest.param(
            "append",
            {"chunks": DAYS, "shard": {"index": 2, "count": 2}},
            "not in the 2 shards",
            id="a_shard_after_the_last",
        ),
        pytest.param(
            "append",
            {"chunks": DAYS, "shard": {"index": -1, "count": 2}},
            "not in the 2 shards",
            id="a_shard_before_the_first",
        ),
    ],
)
def test_a_mode_that_does_not_fit_is_refused_before_any_write(dump_mode: DumpMode, options: DumpOptions, message):
    """Raise with the reason, so a flow never starts a load that it could not finish."""
    with pytest.raises(ValueError, match=message):
        pick_mode(dump_mode, options)


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        pytest.param(Replace(None), ["all"], id="no_chunks_is_one_chunk"),
        pytest.param(Replace(DAYS), ["2024-03-01", "2024-03-02", "null"], id="the_days_and_the_null_chunk"),
        pytest.param(Overwrite(DAYS), ["2024-03-01", "2024-03-02", "null"], id="an_overwrite_cuts_like_an_append"),
        pytest.param(Capture(RUN), ["2024-03-10"], id="a_capture_is_in_the_chunk_of_its_day"),
        pytest.param(Merge(MERGE), ["all"], id="a_merge_is_one_chunk"),
    ],
)
def test_each_mode_cuts_its_chunks(mode, expected):
    """Name the chunks that a mode reads."""
    assert [chunk.key for chunk in cut_mode(mode)] == expected


@pytest.mark.parametrize(
    ("mode", "chunk", "expected"),
    [
        pytest.param(Replace(None), Chunk("all", 0, 0), "all", id="a_chunk_is_its_own_key"),
        pytest.param(Merge(MERGE), Chunk("all", 0, 0), "all", id="a_merge_chunk_is_its_own_key"),
        pytest.param(Capture(RUN), Chunk("2024-03-10", 0, 0), RUN.isoformat(), id="a_capture_is_its_own_time"),
    ],
)
def test_the_ledger_key_tells_one_load_from_another(mode, chunk, expected):
    """Give a capture the key of its time, so two captures of a day are two loads."""
    assert build_ledger_key(mode, chunk) == expected


@pytest.mark.parametrize(
    ("mode", "seen", "expected"),
    [
        pytest.param(Overwrite(None), {"all"}, ["all"], id="an_overwrite_reads_what_it_has_seen"),
        pytest.param(Capture(RUN), {"2024-03-10"}, [], id="a_capture_that_was_seen_is_not_read"),
        pytest.param(Capture(RUN), set(), ["2024-03-10"], id="a_new_capture_is_read"),
        pytest.param(Replace(None), {"all"}, ["all"], id="an_append_checks_a_known_chunk_again"),
        pytest.param(
            Replace({**DAYS, "lookback": 1}),
            {"2024-03-01", "2024-03-02"},
            ["2024-03-02", "null"],
            id="the_lookback_limits_the_checks",
        ),
    ],
)
def test_each_mode_picks_the_chunks_to_read(mode, seen, expected):
    """Skip only what the mode may skip: a capture that the ledger knows."""
    assert [chunk.key for chunk in pick_pending(mode, cut_mode(mode), seen)] == expected


@pytest.mark.parametrize(
    ("mode", "skips", "cleans"),
    [
        pytest.param(Replace(None), True, False, id="an_append"),
        pytest.param(Capture(RUN), True, False, id="a_capture"),
        pytest.param(Merge(MERGE), True, False, id="a_merge"),
        pytest.param(Overwrite(None), False, True, id="an_overwrite_writes_all_and_sweeps"),
    ],
)
def test_only_an_overwrite_writes_an_unchanged_chunk_and_sweeps_the_old_ones(mode, skips, cleans):
    """State what each mode does with a chunk that did not change, and with the old chunks."""
    assert (skips_unchanged(mode), sweeps(mode)) == (skips, cleans)


KEYS = ["a", "b", "c", "d", "e"]
CUT = [Chunk(key, 0, 1) for key in KEYS]


@pytest.mark.parametrize(
    ("shard", "expected"),
    [
        pytest.param({"index": 0, "count": 2}, ["a", "c", "e"], id="the_first_of_two_takes_every_second_chunk"),
        pytest.param({"index": 1, "count": 2}, ["b", "d"], id="the_second_of_two_takes_the_others"),
        pytest.param({"index": 0, "count": 1}, KEYS, id="one_shard_takes_every_chunk"),
        pytest.param({"index": 7, "count": 8}, [], id="a_shard_beyond_the_chunks_takes_none"),
    ],
)
def test_a_shard_takes_every_nth_chunk_from_its_index(shard: Shard, expected):
    """Slice the whole cut round-robin, so a dense area of the range spreads over the shards."""
    assert [chunk.key for chunk in pick_shard(CUT, shard)] == expected


@pytest.mark.parametrize("count", [1, 2, 3, 5, 8])
def test_the_shards_together_hold_every_chunk_once(count):
    """Cover the cut with no gap and no repeat, for any number of shards."""
    keys = [chunk.key for index in range(count) for chunk in pick_shard(CUT, {"index": index, "count": count})]

    assert sorted(keys) == KEYS


@pytest.mark.parametrize(
    ("dump_mode", "options", "expected"),
    [
        pytest.param("append", {"chunks": DAYS, "shards": 2}, 2, id="the_shards_that_were_asked"),
        pytest.param("append", {"chunks": DAYS, "shards": 10}, 3, id="never_more_shards_than_chunks"),
        pytest.param("overwrite", {"chunks": DAYS, "shards": 3}, 3, id="an_overwrite_in_shards"),
    ],
)
def test_a_dump_takes_the_shards_that_it_asked_and_no_more_than_its_chunks(
    dump_mode: DumpMode, options: DatabaseDumpOptions, expected
):
    """Give as many child runs as asked, and never an empty one."""
    assert plan_shards(dump_mode, options) == expected


@pytest.mark.parametrize(
    ("dump_mode", "options", "message"),
    [
        pytest.param("append", {"chunks": DAYS, "shards": 0}, "at least 1", id="no_shard"),
        pytest.param("append", {"shards": 2}, "set chunks", id="shards_without_chunks"),
        pytest.param(
            "append",
            {"chunks": DAYS, "shards": 2, "shard": {"index": 0, "count": 2}},
            "shards of its own",
            id="a_shard_with_shards",
        ),
        pytest.param("merge", {"chunks": DAYS, "shards": 2, "merge": MERGE}, "one chunk", id="a_merge_in_shards"),
        pytest.param("append", {"capture": RUN, "shards": 2}, "set chunks", id="a_capture_in_shards"),
    ],
)
def test_a_dump_that_cannot_run_in_shards_is_refused_before_any_run_starts(
    dump_mode: DumpMode, options: DatabaseDumpOptions, message
):
    """Raise with the reason, so the parent never starts child runs that cannot work."""
    with pytest.raises(ValueError, match=message):
        plan_shards(dump_mode, options)


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        pytest.param(Replace(DAYS), True, id="a_replace_probes"),
        pytest.param(Overwrite(DAYS), False, id="an_overwrite_writes_every_chunk_anyway"),
        pytest.param(Capture(RUN), False, id="a_capture_loads_one_new_folder"),
        pytest.param(Merge(MERGE), False, id="a_merge_loads_one_new_folder"),
    ],
)
def test_only_a_replace_can_skip_a_chunk_before_its_download(mode, expected):
    """Probe only the mode that skips an unchanged chunk."""
    assert probes(mode) is expected


@pytest.mark.parametrize(
    ("source", "mode", "options", "expected"),
    [
        pytest.param(
            POSTGRES,
            Replace(DAYS),
            COLUMN,
            Probe("pg-range-1", "updated_at"),
            id="a_database_with_a_column_probes",
        ),
        pytest.param(POSTGRES, Replace(DAYS), {}, None, id="a_database_with_no_column_does_not_probe"),
        pytest.param(POSTGRES, Replace(DAYS), {**COLUMN, "probe": False}, None, id="probe_false_turns_the_probe_off"),
        pytest.param(
            MSSQL, Replace(DAYS), COLUMN, Probe("mssql-range-1", "updated_at"), id="sql_server_probes_with_a_column"
        ),
        pytest.param(ORACLE, Replace(DAYS), {}, Probe("oracle-rowscn-1"), id="oracle_probes_with_no_column"),
        pytest.param(ORACLE, Replace(DAYS), {"probe": False}, None, id="oracle_probe_can_be_turned_off"),
        pytest.param(FILES, Replace(DAYS), {}, None, id="files_have_no_probe"),
        pytest.param(POSTGRES, Overwrite(DAYS), COLUMN, None, id="an_overwrite_never_probes"),
    ],
)
def test_the_probe_follows_the_options_the_source_and_the_mode(source, mode, options, expected):
    """Probe a database by its column, Oracle and CouchDB by default, and never an overwrite."""
    assert pick_probe(source, mode, options) == expected


@pytest.mark.parametrize(
    ("source", "options", "message"),
    [
        pytest.param(POSTGRES, {"probe": True}, "needs the option range_column", id="a_demand_with_no_column"),
        pytest.param(ORACLE, COLUMN, "for a database probe only", id="a_column_for_a_source_that_has_none"),
        pytest.param(FILES, {"probe": True}, "no probe", id="files_cannot_probe"),
    ],
)
def test_a_probe_that_cannot_run_is_refused(source, options, message):
    """Raise with the reason, so a misspelled deployment never falls back to a download."""
    with pytest.raises(ValueError, match=message):
        pick_probe(source, Replace(DAYS), options)
