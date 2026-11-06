from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.chunks import Chunk, chunk_task_name, pick_chunks


def everything(chunk: Chunk) -> bool:
    return True


def make_chunks(count: int) -> list[Chunk]:
    return [Chunk(index, f"start{index}", f"end{index}") for index in range(count)]


def test_pick_chunks_spreads_samples_across_the_whole_list() -> None:
    picked = pick_chunks(make_chunks(100), 4, everything)

    assert [chunk.chunk_id for chunk in picked] == [12, 37, 62, 87]


def test_pick_chunks_returns_all_when_there_are_fewer_than_requested() -> None:
    assert [chunk.chunk_id for chunk in pick_chunks(make_chunks(3), 4, everything)] == [0, 1, 2]


def test_pick_chunks_never_repeats_a_chunk() -> None:
    picked = pick_chunks(make_chunks(5), 4, everything)

    assert len({chunk.chunk_id for chunk in picked}) == 4


def test_pick_chunks_of_nothing_is_empty() -> None:
    assert pick_chunks([], 4, everything) == ()


def test_chunk_task_name_is_prefixed_unique_and_safe() -> None:
    name = chunk_task_name("DPS", "1a2b-3c4d")

    assert name == "O2BQPROBE_DPS_1A2B_3C4D"
    assert chunk_task_name("DPS", "x" * 300).startswith("O2BQPROBE_DPS_")
    assert len(chunk_task_name("DPS", "x" * 300)) == 128


def test_pick_chunks_skips_empty_chunks_within_the_same_slice() -> None:
    populated = {13, 70}

    picked = pick_chunks(make_chunks(100), 4, lambda chunk: chunk.chunk_id in populated)

    assert [chunk.chunk_id for chunk in picked] == [13, 37, 70, 87]
    # the second and fourth slices are empty, so they keep their central chunk


def test_pick_chunks_falls_back_to_the_central_chunk_when_the_slice_is_empty() -> None:
    picked = pick_chunks(make_chunks(100), 4, lambda chunk: False)

    assert [chunk.chunk_id for chunk in picked] == [12, 37, 62, 87]
