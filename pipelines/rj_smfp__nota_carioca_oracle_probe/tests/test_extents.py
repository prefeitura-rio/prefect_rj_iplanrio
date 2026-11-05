import pytest

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.extents import (
    BlockRange,
    Extent,
    encode_base64_number,
    extended_rowid,
    group_extents,
    range_rowids,
)


def test_encode_base64_number_uses_the_rowid_alphabet_big_endian() -> None:
    assert encode_base64_number(0, 3) == "AAA"
    assert encode_base64_number(63, 1) == "/"
    assert encode_base64_number(64, 2) == "BA"
    assert encode_base64_number(26, 1) == "a"


def test_encode_base64_number_rejects_values_that_do_not_fit() -> None:
    with pytest.raises(ValueError, match="não cabe"):
        encode_base64_number(64**3, 3)


def test_extended_rowid_layout_is_object_file_block_row() -> None:
    rowid = extended_rowid(data_object_id=1, relative_fno=2, block=3, row=4)

    assert rowid == "AAAAAB" + "AAC" + "AAAAAD" + "AAE"


def test_group_extents_merges_same_segment_and_file_up_to_the_chunk_size() -> None:
    extents = [Extent(10, 4, 1000, 8), Extent(10, 4, 1008, 8), Extent(10, 4, 1016, 8), Extent(10, 4, 1024, 8)]

    assert group_extents(extents, 16) == [BlockRange(10, 4, 1000, 1015), BlockRange(10, 4, 1016, 1031)]


def test_group_extents_never_mixes_segments_or_files() -> None:
    extents = [Extent(11, 4, 10, 8), Extent(10, 5, 10, 8), Extent(10, 4, 10, 8)]

    assert group_extents(extents, 1_000) == [
        BlockRange(10, 4, 10, 17),
        BlockRange(10, 5, 10, 17),
        BlockRange(11, 4, 10, 17),
    ]


def test_group_extents_keeps_a_big_extent_alone() -> None:
    assert group_extents([Extent(10, 4, 0, 50_000)], 32_768) == [BlockRange(10, 4, 0, 49_999)]


def test_range_rowids_cover_first_row_of_first_block_to_last_row_of_last_block() -> None:
    start, end = range_rowids(BlockRange(10, 4, 1000, 1015))

    assert start == extended_rowid(10, 4, 1000, 0)
    assert end == extended_rowid(10, 4, 1015, 32767)
