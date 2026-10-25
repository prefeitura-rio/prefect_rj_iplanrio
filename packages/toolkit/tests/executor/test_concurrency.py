from asyncio import Event, wait_for
from pathlib import Path

import pytest

from toolkit.chunks import Chunk, ChunkPlan, Rows
from toolkit.executor.executor import Executor

KEYS = [f"w{number}" for number in range(6)]
CHUNKS = [Chunk(key, 0, 1) for key in KEYS]


def plan(chunk: Chunk) -> ChunkPlan:
    """Return rows whose column is the key of the chunk, which the fake reads back."""
    return ChunkPlan(Rows(column=chunk.key), [])


async def run(executor: Executor, handled: list[str]) -> None:
    """Hand every chunk to a handler that records its key."""

    async def handle(chunk: Chunk, folder: Path, token: int | None) -> None:
        """Record the chunk, after the file that the engine wrote."""
        assert (folder / "out.parquet").read_text() == chunk.key
        handled.append(chunk.key)

    await executor.run_chunks(CHUNKS, plan, handle)


@pytest.mark.parametrize(
    ("workers", "peak"),
    [
        pytest.param(1, 1, id="one_worker_reads_one_chunk_at_a_time"),
        pytest.param(2, 2, id="two_workers_read_two_chunks_together_and_never_three"),
        pytest.param(4, 4, id="four_workers_read_four_chunks_together"),
    ],
)
async def test_the_engine_says_how_many_chunks_are_read_together(
    fake_chunk_engine, workers, peak
):
    """Read as many chunks together as the engine says, and never more."""
    engine = fake_chunk_engine(workers=workers)
    handled: list[str] = []

    await run(Executor(engine), handled)

    assert sorted(handled) == KEYS
    assert engine.activity.peak == peak


async def test_the_chunks_are_handed_over_together(fake_chunk_engine):
    """Run the handler for two chunks at once: each waits for the other and both end."""
    engine = fake_chunk_engine(workers=2)
    arrived = [Event(), Event()]
    handled: list[str] = []

    async def handle(chunk: Chunk, folder: Path, token: int | None) -> None:
        """Wait until the other handler has started."""
        mine = len(handled) % 2
        handled.append(chunk.key)
        arrived[mine].set()
        await wait_for(arrived[1 - mine].wait(), timeout=1)

    await Executor(engine).run_chunks(CHUNKS[:2], plan, handle)

    assert sorted(handled) == KEYS[:2]


async def test_each_extra_worker_reads_on_an_engine_of_its_own(fake_chunk_engine):
    """Open one clone for each worker, and close them all when the chunks are done."""
    engine = fake_chunk_engine(workers=3)

    await run(Executor(engine), [])

    assert (engine.activity.opened, engine.activity.closed) == (3, 3)


async def test_one_worker_reads_on_the_engine_that_it_has(fake_chunk_engine):
    """Open no clone when one chunk is read at a time."""
    engine = fake_chunk_engine(workers=1)

    await run(Executor(engine), [])

    assert engine.activity.opened == 0


async def test_the_first_error_is_raised_as_it_is_and_stops_the_other_workers(
    fake_chunk_engine,
):
    """Raise the `ValueError`, not a group, and leave the other chunks unread."""
    engine = fake_chunk_engine(workers=2, fail_on="w1")
    handled: list[str] = []

    with pytest.raises(ValueError, match="w1 failed"):
        await run(Executor(engine), handled)

    assert len(handled) < len(KEYS)
    assert engine.activity.running == 0


async def test_the_executor_opens_the_engine_and_closes_it(fake_chunk_engine):
    """Open the engine on enter and close it on exit."""
    engine = fake_chunk_engine()

    async with Executor(engine) as executor:
        assert executor.engine is engine
        assert (engine.activity.opened, engine.activity.closed) == (1, 0)

    assert (engine.activity.opened, engine.activity.closed) == (1, 1)
