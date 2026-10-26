from collections.abc import AsyncIterator, Callable

import pytest
from httpx2 import HTTPStatusError, Request, Response

from pipelines.rj_segur__forca_municipal.helpers.collect import (
    check_error_rate,
    count_cycles,
    dump_nested,
    flush_buffer,
    read_detail,
    read_details,
    read_kmls,
    read_pages,
    read_unit_days,
)
from pipelines.rj_segur__forca_municipal.tests.conftest import FakeApi, reply
from pipelines.rj_segur__forca_municipal.types import Endpoint, Row, Tally

KML = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"><Document>
  <Folder><name>Zone</name><Placemark><name>P1</name><Point><coordinates>-43.2,-22.9,0</coordinates></Point></Placemark></Folder>
</Document></kml>"""


async def batches(*groups: list[Row]) -> AsyncIterator[list[Row]]:
    """Yield the groups of rows as batches."""
    for group in groups:
        yield group


def rows(count: int, start: int = 0) -> list[Row]:
    """Build `count` rows with an id."""
    return [{"id": number} for number in range(start, start + count)]


@pytest.mark.parametrize(
    ("groups", "limit", "expected"),
    [
        pytest.param([rows(3)], 10, [(3, 1)], id="rows_under_the_limit_make_one_cycle"),
        pytest.param([rows(4), rows(4)], 4, [(4, 1), (4, 2)], id="a_batch_that_fills_the_limit_is_a_cycle"),
        pytest.param([rows(3), rows(3)], 4, [(4, 1), (2, 2)], id="a_batch_over_the_limit_leaves_the_rest"),
        pytest.param([rows(9)], 4, [(4, 1), (4, 2), (1, 3)], id="one_batch_fills_many_cycles"),
        pytest.param([], 4, [], id="no_batch_makes_no_cycle"),
    ],
)
async def test_rows_upload_in_cycles_of_the_limit(
    groups: list[list[Row]], limit: int, expected: list[tuple[int, int]]
) -> None:
    """Upload each full buffer, then the rest, with the number of each cycle."""
    uploads: list[tuple[int, int]] = []

    async def upload(found: list[Row], cycle: int) -> None:
        uploads.append((len(found), cycle))

    total = await flush_buffer(batches(*groups), upload, limit)

    assert uploads == expected
    assert total == sum(len(group) for group in groups)


@pytest.mark.parametrize(
    ("pages", "page_size", "expected"),
    [
        pytest.param(1, 500, (100, 1), id="one_page"),
        pytest.param(100, 500, (100, 1), id="pages_that_fill_one_cycle"),
        pytest.param(101, 500, (100, 2), id="one_page_over"),
        pytest.param(10, 25_000, (2, 5), id="a_big_page_size"),
    ],
)
def test_the_pages_of_a_cycle_depend_on_the_page_size(pages: int, page_size: int, expected: tuple[int, int]) -> None:
    """Count the pages per cycle and the cycles."""
    assert count_cycles(pages, page_size) == expected


async def test_the_pages_after_the_first_come_in_cycles(fake_api: Callable[..., FakeApi]) -> None:
    """Yield the first page, then a batch for each range of pages that fills a cycle."""
    api = fake_api(pages={number: [{"page": number}] for number in range(1, 6)})

    found = [batch async for batch in read_pages(api, "/api/x", "x", 25_000, (5, [{"page": 1}]))]

    assert found == [[{"page": 1}], [{"page": 2}, {"page": 3}], [{"page": 4}, {"page": 5}]]
    assert [(call[1]) for call in api.calls] == [2, 4]


def test_the_nested_values_of_a_detail_become_json_text() -> None:
    """Dump lists and objects, and keep the rest."""
    assert dump_nested({"a": 1, "b": [1], "c": {"é": 2}, "d": None}) == {"a": 1, "b": "[1]", "c": '{"é": 2}', "d": None}


def status_error(status: int) -> HTTPStatusError:
    """Build the error of a response with a status."""
    request = Request("GET", "http://api/")

    return HTTPStatusError("error", request=request, response=Response(status, request=request))


@pytest.mark.parametrize(
    ("replies", "errors", "found"),
    [
        pytest.param({"/q/1": reply({"id": 1, "tags": [1]})}, 0, {"id": 1, "tags": "[1]"}, id="a_detail"),
        pytest.param({"/q/1": status_error(404)}, 0, None, id="a_gone_qmd_is_not_an_error"),
        pytest.param({"/q/1": status_error(410)}, 0, None, id="a_410_is_not_an_error"),
        pytest.param({"/q/1": status_error(500)}, 1, None, id="a_server_error"),
        pytest.param({"/q/1": OSError("down")}, 1, None, id="a_network_error"),
        pytest.param({"/q/1": reply("not json")}, 1, None, id="a_body_that_is_not_json"),
        pytest.param({"/q/1": reply([1])}, 0, None, id="a_body_that_is_not_an_object"),
    ],
)
async def test_a_detail_is_read_or_skipped(
    fake_api: Callable[..., FakeApi], replies: dict[str, Response | Exception], errors: int, found: Row | None
) -> None:
    """Skip a QMD that is gone without an error, and count any other failure."""
    tally = Tally()

    assert await read_detail(fake_api(replies=replies), "/q/{id}", "1", tally) == found
    assert tally.errors == errors


async def test_the_details_come_in_groups_and_the_gone_ones_are_dropped(fake_api: Callable[..., FakeApi]) -> None:
    """Group the ids, count them, and yield only the batches that have rows."""
    api = fake_api(
        replies={
            "/q/1": reply({"id": 1}),
            "/q/2": status_error(404),
            "/q/3": status_error(404),
            "/q/4": reply({"id": 4}),
        }
    )
    tally = Tally()

    found = [batch async for batch in read_details(api, "/q/{id}", ["1", "2", "3", "4"], 2, tally)]

    assert found == [[{"id": 1}], [{"id": 4}]]
    assert (tally.attempted, tally.errors) == (4, 0)


async def test_a_kml_without_placemarks_counts_as_an_error(fake_api: Callable[..., FakeApi]) -> None:
    """Count a KML that fails, one that does not parse and one with no placemark.

    It keeps the placemarks that are good.
    """
    api = fake_api(
        replies={
            "/k/1": reply(KML),
            "/k/2": status_error(500),
            "/k/3": reply("not xml"),
            "/k/4": reply('<kml xmlns="http://www.opengis.net/kml/2.2"><Document/></kml>'),
        }
    )
    tally = Tally()

    found = [batch async for batch in read_kmls(api, "/k/{id}", ["1", "2", "3", "4"], 4, tally)]

    assert [
        [(row["qmd_id"], row["kml_folder"], row["name"], row["geometry_type"]) for row in batch] for batch in found
    ] == [[("1", "Zone", "P1", "Point")]]
    assert (tally.attempted, tally.errors) == (4, 3)


@pytest.mark.parametrize(
    ("errors", "attempted", "fails"),
    [
        pytest.param(0, 100, False, id="no_error"),
        pytest.param(5, 100, False, id="at_the_limit"),
        pytest.param(6, 100, True, id="over_the_limit"),
    ],
)
def test_the_run_fails_when_too_many_ids_fail(errors: int, attempted: int, fails: bool) -> None:
    """Allow 5% of failed ids."""
    tally = Tally(attempted=attempted, errors=errors)

    if fails:
        with pytest.raises(RuntimeError, match="6 of 100"):
            check_error_rate("table", tally, 0.05)
    else:
        check_error_rate("table", tally, 0.05)


async def test_the_positions_of_a_unit_carry_the_day_and_a_failed_unit_is_counted() -> None:
    """Read the pages of each unit of the group, skip the one that fails, and tally.

    The positions carry the day.
    """
    api = FakeApi(pages={1: [{"p": 1}], 2: [{"p": 2}]}, bad_units={"bad"})
    tally = Tally()

    found = [
        batch async for batch in read_unit_days(Endpoint(api, "/pos", 500), ["U1", "bad", "U2"], "2026-07-17", 3, tally)
    ]

    assert found == [[{"p": number, "data_coleta": "2026-07-17"} for _ in range(2) for number in (1, 2)]]
    assert (tally.attempted, tally.errors) == (3, 1)
