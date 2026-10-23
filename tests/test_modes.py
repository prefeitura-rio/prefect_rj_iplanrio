from datetime import UTC, datetime

import pytest
from toolkit.chunks import Chunk
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
    skips_unchanged,
    sweeps,
)
from utils.types import DumpMode, DumpOptions

RUN = datetime(2024, 3, 10, 9, 0, tzinfo=UTC)
DAYS: Chunks = {"column": "dia", "width": "day", "begin": "2024-03-01", "end": "2024-03-02"}
MERGE = MergeSpec(key=["id"])


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
