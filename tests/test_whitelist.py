from random import Random
from typing import final

import pytest
from httpx2 import AsyncClient, MockTransport, Request, Response
from pydantic import JsonValue, TypeAdapter

from utils.whitelist import (
    Selection,
    Whitelist,
    add_to_whitelist,
    normalize_numbers,
    remove_from_whitelist,
    sample_numbers,
)

BODY: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])


@final
class FakeApi:
    """The whitelist API: it gives a token, keeps the groups and the numbers, and records the calls that change them."""

    def __init__(
        self, groups: list[dict[str, JsonValue]] | None = None, numbers: list[str] | None = None, login: int = 200
    ) -> None:
        self.groups = groups or []
        self.numbers = numbers or []
        self.login = login
        self.added: list[dict[str, JsonValue]] = []
        self.removed: list[list[JsonValue]] = []
        self.created: list[str] = []
        self.pages: list[int] = []
        self.whitelist = Whitelist(
            AsyncClient(transport=MockTransport(self.handle), base_url="https://w.test"),
            "https://sso.test",
            ("id", "secret"),
        )

    def handle(self, request: Request) -> Response:
        """Answer a request of the client."""
        path = request.url.path

        if path == "/protocol/openid-connect/token":
            return Response(self.login, json={"access_token": "t"} if self.login == 200 else {})

        if path == "/groups" and request.method == "GET":
            return Response(200, json={"groups": self.groups})

        if path == "/groups":
            name = str(BODY.validate_json(request.content)["name"])
            self.created.append(name)

            return Response(200, json={"id": "new-group", "name": name})

        if path == "/whitelist":
            page = int(request.url.params["page"])
            self.pages.append(page)
            chunk = self.numbers[(page - 1) * 100 : page * 100]

            return Response(200, json={"whitelisted": [{"phone_number": number} for number in chunk]})

        body = BODY.validate_json(request.content)

        if path == "/whitelist/bulk-add":
            self.added.append(body)
        else:
            numbers = body["phone_numbers"]
            self.removed.append(numbers if isinstance(numbers, list) else [])

        return Response(200, json={})


@pytest.mark.parametrize(
    ("number", "expected"),
    [
        pytest.param("21987654321", ["552187654321", "5521987654321"], id="mobile_with_nine"),
        pytest.param("2187654321", ["552187654321", "5521987654321"], id="mobile_without_nine"),
        pytest.param("5521987654321", ["552187654321", "5521987654321"], id="with_the_country_code"),
        pytest.param("552133334444", ["552133334444", "5521933334444"], id="a_landline_gets_a_nine_too"),
    ],
)
def test_a_number_gets_a_version_with_and_without_the_nine(number: str, expected: list[str]) -> None:
    """Give both forms, with the country code."""
    assert normalize_numbers(number) == expected


@pytest.mark.parametrize(
    ("count", "percentage", "expected"),
    [
        pytest.param(10, 30, 3, id="a_share"),
        pytest.param(10, 0, 0, id="zero"),
        pytest.param(10, 100, 10, id="all"),
        pytest.param(10, 250, 10, id="more_than_all"),
        pytest.param(3, 30, 0, id="rounds_down_to_nothing"),
    ],
)
def test_the_share_of_numbers_is_rounded_down(count: int, percentage: int, expected: int) -> None:
    """Pick `percentage` percent of the different numbers."""
    numbers = [f"n{number}" for number in range(count)]

    assert len(sample_numbers([*numbers, "n0"], percentage, Random(1))) == expected


async def test_a_group_that_exists_is_used_and_the_known_numbers_are_skipped() -> None:
    """Add the numbers that the whitelist lacks, with both forms, to the found group."""
    api = FakeApi(groups=[{"id": "g1", "name": "citizen-hsm-x"}], numbers=["5521987654321"])

    sent = await add_to_whitelist(
        api.whitelist, ["21987654321", "21911112222"], Selection(100, "citizen-hsm-x"), Random(1)
    )

    assert sent == 2
    assert api.created == []
    assert api.added == [{"group_id": "g1", "phone_numbers": ["552111112222", "5521911112222"]}]


async def test_a_missing_group_is_created() -> None:
    """Create the group and add to it."""
    api = FakeApi()

    await add_to_whitelist(api.whitelist, ["21987654321"], Selection(100, "citizen-hsm-x"), Random(1))

    assert api.created == ["citizen-hsm-x"]
    assert api.added[0]["group_id"] == "new-group"


async def test_force_adds_the_numbers_even_if_the_whitelist_has_them() -> None:
    """Skip the read of the whitelist."""
    api = FakeApi(groups=[{"id": "g1", "name": "g"}], numbers=["5521987654321"])

    sent = await add_to_whitelist(api.whitelist, ["21987654321"], Selection(100, "g", force=True), Random(1))

    assert sent == 2
    assert api.pages == []


async def test_nothing_is_sent_when_every_number_is_known_or_the_share_is_empty() -> None:
    """Send no request that changes the whitelist."""
    known = FakeApi(groups=[{"id": "g", "name": "g"}], numbers=["5521987654321"])
    empty = FakeApi()

    assert await add_to_whitelist(known.whitelist, ["21987654321"], Selection(100, "g"), Random(1)) == 0
    assert await add_to_whitelist(empty.whitelist, ["21987654321"], Selection(0, "g"), Random(1)) == 0
    assert known.added == []
    assert empty.created == []


async def test_a_login_that_fails_stops_the_add() -> None:
    """Raise `RuntimeError` and change nothing."""
    api = FakeApi(login=401)

    with pytest.raises(RuntimeError, match="login"):
        await add_to_whitelist(api.whitelist, ["21987654321"], Selection(100, "g"), Random(1))

    assert api.created == api.added == []


async def test_the_whitelist_is_read_page_by_page() -> None:
    """Read pages of 100 until a page is short."""
    api = FakeApi(numbers=[f"5521987{number:06d}" for number in range(250)])
    await api.whitelist.authenticate()

    numbers = await api.whitelist.existing_numbers()

    assert api.pages == [1, 2, 3]
    assert len(numbers) == 500


async def test_phones_are_removed_with_both_forms_in_one_request() -> None:
    """Remove each phone with and without the nine, once."""
    api = FakeApi()

    sent = await remove_from_whitelist(api.whitelist, ["21987654321", "2187654321"])

    assert sent == 2
    assert api.removed == [["552187654321", "5521987654321"]]


async def test_nothing_is_removed_for_no_phone_or_a_failed_login() -> None:
    """Send no request."""
    api = FakeApi(login=401)

    assert await remove_from_whitelist(api.whitelist, []) == 0
    assert await remove_from_whitelist(api.whitelist, ["21987654321"]) == 0
    assert api.removed == []
