import json

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import Progress, format_progress
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.table_progress import (
    TableProgress,
    TableReporter,
    TableStage,
)

GIB = 1024**3


def progress(memory_bytes: int | None = None, memory_limit_bytes: int | None = None) -> Progress:
    return Progress(
        chunks_read=30,
        chunks_uploaded=25,
        chunks_total=100,
        rows_read=500_000,
        bytes_uploaded=3 * GIB,
        pending_files=5,
        elapsed_seconds=100,
        memory_bytes=memory_bytes,
        memory_limit_bytes=memory_limit_bytes,
    )


def test_progress_line_shows_the_pod_memory_with_limit_and_percentage() -> None:
    # Given 4.1 GiB used of a 6 GiB limit
    line = format_progress("DPS", progress(int(4.1 * GIB), 6 * GIB))
    # Then the line has it between the pending files and the elapsed time
    assert "memória do pod 4.1/6.0 GB (68%)" in line
    assert line.index("arquivos locais pendentes") < line.index("memória do pod") < line.index("decorrido")


def test_progress_line_without_a_memory_reading_is_the_same_as_before() -> None:
    line = format_progress("DPS", progress())

    assert "memória" not in line
    assert "5 arquivos locais pendentes, decorrido 1m40s, faltam ~5m00s" in line


def test_progress_line_with_usage_but_no_limit_shows_only_the_usage() -> None:
    line = format_progress("DPS", progress(int(2.5 * GIB), None))

    assert "memória do pod 2.5 GB, decorrido" in line
    assert "/" not in line.split("memória do pod")[1].split("decorrido")[0]


def test_progress_defaults_keep_the_positional_constructor_working() -> None:
    legacy = Progress(2, 0, 10, 100, 0, 2, 5)

    assert (legacy.memory_bytes, legacy.memory_limit_bytes) == (None, None)


def test_table_progress_json_carries_the_pod_memory() -> None:
    # Given a table progress with memory
    original = TableProgress(table="DPS", stage=TableStage.EXTRACTION, memory_bytes=4 * GIB, memory_limit_bytes=8 * GIB)
    # When it goes through the JSON the child writes and the parent reads
    data = json.loads(original.to_json())
    restored = TableProgress.from_json(original.to_json())
    # Then the fields are in the JSON and survive the round trip
    assert (data["memory_bytes"], data["memory_limit_bytes"]) == (4 * GIB, 8 * GIB)
    assert restored == original


def test_table_progress_json_written_before_the_memory_fields_still_loads() -> None:
    # Given the JSON of an older child, without the memory fields
    old = json.loads(TableProgress(table="DPS", stage=TableStage.EXTRACTION).to_json())
    del old["memory_bytes"], old["memory_limit_bytes"]
    # When the parent reads it
    restored = TableProgress.from_json(json.dumps(old))
    # Then the memory is simply unknown
    assert (restored.memory_bytes, restored.memory_limit_bytes) == (None, None)


def test_reporter_copies_the_memory_of_each_extraction_tick_into_the_progress_json() -> None:
    # Given a reporter with a sink that records every snapshot
    seen: list[TableProgress] = []
    reporter = TableReporter("DPS", [seen.append])
    # When an extraction tick with memory arrives, and then the extraction stage changes
    reporter.extraction(progress(int(4.1 * GIB), 6 * GIB))
    reporter.stage(TableStage.LOAD)
    # Then the tick carried it and a later stage change does not erase it
    assert (seen[0].memory_bytes, seen[0].memory_limit_bytes) == (int(4.1 * GIB), 6 * GIB)
    assert seen[1].memory_bytes == int(4.1 * GIB)
    assert json.loads(seen[0].to_json())["memory_limit_bytes"] == 6 * GIB


def test_reporter_without_memory_leaves_the_fields_empty() -> None:
    seen: list[TableProgress] = []
    TableReporter("DPS", [seen.append]).extraction(progress())

    assert (seen[0].memory_bytes, seen[0].memory_limit_bytes) == (None, None)
