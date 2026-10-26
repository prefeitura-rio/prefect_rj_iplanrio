from collections.abc import Callable

import pytest
from httpx2 import AsyncClient, ConnectError, MockTransport, Request, Response
from pydantic import JsonValue

from pipelines.rj_crm__geolocalizacao_residencia.helpers.geocode import (
    backoff_delay,
    geocode_one,
    geocode_xyz_one,
    poll_geoapify,
    submit_geoapify,
)
from pipelines.rj_crm__geolocalizacao_residencia.helpers.providers import (
    WAZE,
    add_rio,
    empty_geocode,
    finish_geoapify,
    fold,
    is_valid_xyz,
    locationiq,
    maptiler,
    nominatim,
    opencage,
    parse_xyz,
    xyz_url,
)
from pipelines.rj_crm__geolocalizacao_residencia.helpers.rows import has_point, join_rows, stringify, to_pluscode
from pipelines.rj_crm__geolocalizacao_residencia.tests.conftest import no_pause, reply
from pipelines.rj_crm__geolocalizacao_residencia.types import Address, Run

TODAY = "2026-07-17"


@pytest.mark.parametrize(
    ("attempt", "base", "jitter", "expected"),
    [
        pytest.param(0, 1.1, 0.0, 1.1, id="the_first_try_waits_the_base"),
        pytest.param(2, 2.0, 0.0, 8.0, id="each_try_doubles"),
        pytest.param(10, 2.0, 0.0, 60.0, id="the_wait_stops_at_a_minute"),
        pytest.param(0, 1.0, 0.1, 1.1, id="jitter_adds_a_share"),
        pytest.param(0, 0.01, -0.1, 0.1, id="never_under_a_tenth_of_a_second"),
    ],
)
def test_the_wait_grows_with_the_attempt_and_has_a_floor(
    attempt: int, base: float, jitter: float, expected: float
) -> None:
    """Double the base, cut at a minute, add the jitter and keep the floor."""
    assert backoff_delay(attempt, base, jitter) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("São João", "sao joao", id="accents_and_capitals"),
        pytest.param("  Rua A ", "rua a", id="spaces_are_trimmed"),
        pytest.param("", None, id="empty"),
        pytest.param(None, None, id="nothing"),
        pytest.param(5, None, id="not_text"),
    ],
)
def test_a_name_is_folded_to_lower_case_without_accents(value: JsonValue, expected: str | None) -> None:
    """Drop accents and capitals."""
    assert fold(value) == expected


def test_an_address_gets_rio_de_janeiro_unless_it_has_it() -> None:
    """Add the city once."""
    assert add_rio("Rua A 10") == "Rua A 10, Rio de Janeiro"
    assert add_rio("Rua A 10, Rio de Janeiro, RJ") == "Rua A 10, Rio de Janeiro, RJ"


def test_the_nominatim_request_has_the_address_in_english_and_the_user_agent() -> None:
    """Build the search with `Brazil` and without `RJ,`."""
    query = nominatim("geo.example", "agent").build("rua a 10, copacabana, Rio de Janeiro, RJ, Brasil")

    assert query.url == "https://geo.example/search"
    assert query.params["q"] == "rua a 10, copacabana, Rio de Janeiro, Brazil"
    assert (query.params["countrycodes"], query.params["limit"]) == ("br", "1")
    assert query.headers == {"User-Agent": "agent"}


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        pytest.param(
            reply(
                [
                    {
                        "lat": "-22.9",
                        "lon": "-43.2",
                        "address": {
                            "road": "Rua Ávila",
                            "house_number": "10",
                            "suburb": "Tijuca",
                            "city": "Rio",
                            "state": "RJ",
                            "postcode": "20000",
                        },
                    }
                ]
            ),
            ("-22.9", "-43.2", "rua avila", "10", "tijuca"),
            id="a_match",
        ),
        pytest.param(reply([]), None, id="no_match"),
        pytest.param(reply([{"lat": "1"}], 503), None, id="a_server_error"),
    ],
)
def test_nominatim_answers_are_read(response: Response, expected: tuple[str, ...] | None) -> None:
    """Take the first place with its address fields folded."""
    found = nominatim("d", "a").parse(response, TODAY)

    if expected is None:
        assert found is None
    else:
        assert found is not None
        assert (
            found["latitude"],
            found["longitude"],
            found["logradouro_geocode"],
            found["numero_porta_geocode"],
            found["bairro_geocode"],
        ) == expected
        assert found["geocode"] == "nominatim"


def test_waze_reads_the_name_as_street_neighborhood_and_city() -> None:
    """Split `fullName` and give the confidence 1."""
    response = reply([{"fullName": "Rua Ávila, Tijuca, Rio de Janeiro", "location": {"lat": -22.9, "lon": -43.2}}])

    found = WAZE.parse(response, TODAY)

    assert found is not None
    assert (found["logradouro_geocode"], found["bairro_geocode"], found["cidade_geocode"]) == (
        "rua avila",
        "tijuca",
        "rio de janeiro",
    )
    assert (found["latitude"], found["longitude"], found["confianca"], found["estado_geocode"]) == (
        -22.9,
        -43.2,
        1.0,
        "rio de janeiro",
    )
    assert WAZE.parse(reply([]), TODAY) is None
    assert WAZE.parse(reply([{}], 429), TODAY) is None


def test_waze_uses_the_name_when_there_is_no_full_name_and_rio_when_there_is_no_city() -> None:
    """Fall back to `name`, and to the city of Rio."""
    found = WAZE.parse(reply([{"name": "Rua B, Centro", "location": {"lat": 1, "lon": 2}}]), TODAY)

    assert found is not None
    assert (found["logradouro_geocode"], found["bairro_geocode"], found["cidade_geocode"]) == (
        "rua b",
        "centro",
        "rio de janeiro",
    )


def test_maptiler_puts_the_address_in_the_path_and_reads_the_context() -> None:
    """Add `Rio de Janeiro, Brazil`, and read the place fields from the context.

    It takes the neighborhood, city, state and postcode.
    """
    provider = maptiler("key")
    body = {
        "features": [
            {
                "geometry": {"coordinates": [-43.2, -22.9]},
                "properties": {
                    "text": "Rua Ávila",
                    "context": [
                        {"id": "locality.1", "text": "Tijuca"},
                        {"id": "place.2", "text": "Rio"},
                        {"id": "region.3", "text": "Rio de Janeiro"},
                        {"id": "postcode.4", "text": " 20000 "},
                    ],
                },
            }
        ]
    }

    found = provider.parse(reply(body), TODAY)

    assert provider.build("Rua A 10").url == "https://api.maptiler.com/geocoding/Rua A 10, Rio de Janeiro, Brazil.json"
    assert provider.build("Rua A, Rio de Janeiro").url.endswith("Rua A, Rio de Janeiro, Brazil.json")
    assert found is not None
    assert (
        found["latitude"],
        found["longitude"],
        found["bairro_geocode"],
        found["cidade_geocode"],
        found["cep_geocode"],
    ) == (-22.9, -43.2, "tijuca", "rio", "20000")
    assert provider.parse(reply({"features": []}), TODAY) is None
    assert provider.parse(reply({}, 402), TODAY) is None
    assert provider.parse(reply({}, 429), TODAY) is None


def test_opencage_scales_the_confidence_and_falls_back_to_the_county() -> None:
    """Divide the confidence by 10, and use the county where there is no city."""
    body = {
        "results": [
            {
                "confidence": 8,
                "geometry": {"lat": -22.9, "lng": -43.2},
                "components": {"road": "Rua Ávila", "county": "Rio", "postcode": "20000"},
            }
        ]
    }

    found = opencage("k").parse(reply(body), TODAY)

    assert found is not None
    assert (found["confianca"], found["cidade_geocode"], found["logradouro_geocode"], found["longitude"]) == (
        0.8,
        "rio",
        "rua avila",
        -43.2,
    )
    assert opencage("k").parse(reply({"results": []}), TODAY) is None
    assert opencage("k").parse(reply({}, 402), TODAY) is None


def test_locationiq_keeps_only_results_in_rio_and_takes_the_most_important() -> None:
    """Filter by the city in the display name, and read the fields from its parts."""
    good = {
        "lat": "-22.9",
        "lon": "-43.2",
        "importance": 0.6,
        "display_name": "Rua A, Tijuca, Rio de Janeiro, Região, RJ, 20000, Brasil",
    }
    better = {**good, "importance": 0.9, "display_name": "Rua B, Centro, Rio de Janeiro, Região, RJ, 20001, Brasil"}
    other = {**good, "importance": 1.0, "display_name": "Rua C, Centro, Niterói, Região, RJ, 24000, Brasil"}

    found = locationiq("k").parse(reply([good, better, other]), TODAY)

    assert found is not None
    assert (found["logradouro_geocode"], found["bairro_geocode"], found["cidade_geocode"], found["confianca"]) == (
        "rua b",
        "centro",
        "rio de janeiro",
        0.9,
    )
    assert locationiq("k").parse(reply([other]), TODAY) is None
    assert locationiq("k").parse(reply({}, 402), TODAY) is None


@pytest.mark.parametrize(
    ("data", "valid"),
    [
        pytest.param({"latt": "-22.9", "longt": "-43.2"}, True, id="negative_numbers"),
        pytest.param({"latt": "22.9", "longt": "-43.2"}, False, id="a_positive_latitude"),
        pytest.param({"latt": "0", "longt": "-43.2"}, False, id="zero"),
        pytest.param({"latt": "Throttled! See geocode.xyz/pricing", "longt": "-43"}, False, id="a_throttled_answer"),
        pytest.param({}, False, id="no_coordinates"),
    ],
)
def test_geocode_xyz_coordinates_must_be_negative_numbers(data: JsonValue, valid: bool) -> None:
    """Accept only the numbers that fit Rio."""
    assert is_valid_xyz(data) is valid


def test_the_geocode_xyz_answer_is_read_and_its_state_may_be_text_or_a_list() -> None:
    """Fold the text and take the first item of a list."""
    data: JsonValue = {
        "latt": "-22.9",
        "longt": "-43.2",
        "standard": {
            "addresst": "Rua Ávila",
            "stnumber": "10",
            "city": "Rio",
            "statename": "RJ",
            "postal": "20000",
            "confidence": "0.9",
        },
    }
    as_list: JsonValue = {**data, "standard": {"statename": ["rj"]}}

    found = parse_xyz(data, TODAY)

    assert (
        found["logradouro_geocode"],
        found["numero_porta_geocode"],
        found["estado_geocode"],
        found["confianca"],
    ) == ("rua avila", "10", "rj", 0.9)
    assert parse_xyz(as_list, TODAY)["estado_geocode"] == "rj"
    assert xyz_url("Rua A", "") == "https://geocode.xyz/Rua A?json=1"
    assert xyz_url("Rua A", "k") == "https://geocode.xyz/Rua A?json=1&auth=k"


def test_a_geoapify_result_is_read_or_nothing() -> None:
    """Read the fields, and give nothing for a result with no coordinates."""
    item: JsonValue = {
        "lat": -22.9,
        "lon": -43.2,
        "street": "Rua Ávila",
        "housenumber": " 10 ",
        "district": "Tijuca",
        "rank": {"confidence": 0.8},
    }

    found = finish_geoapify(item, TODAY)

    assert found is not None
    assert (
        found["logradouro_geocode"],
        found["numero_porta_geocode"],
        found["bairro_geocode"],
        found["confianca"],
    ) == ("rua avila", "10", "tijuca", 0.8)
    assert finish_geoapify({"lat": None, "lon": 1}, TODAY) is None
    assert finish_geoapify(None, TODAY) is None


async def test_a_service_that_fails_gives_an_empty_geocode(make_run: Callable[..., tuple[Run, list[Request]]]) -> None:
    """Answer with an empty geocode for a network error, a bad body and a match."""
    run, seen = make_run(
        ConnectError("down"),
        reply("not json"),
        reply({"features": [{"geometry": {"coordinates": [-43.2, -22.9]}, "properties": {"text": "Rua A"}}]}),
    )
    provider = maptiler("k")

    first = await geocode_one(run, provider, "a")
    second = await geocode_one(run, provider, "b")
    third = await geocode_one(run, provider, "c")

    assert [has_point(item) for item in (first, second, third)] == [False, False, True]
    assert first == empty_geocode("maptiler", TODAY)
    assert len(seen) == 3


async def test_geocode_xyz_tries_again_after_a_throttled_answer(
    make_run: Callable[..., tuple[Run, list[Request]]],
) -> None:
    """Retry after a 429 and a throttled body, and stop at the first good answer."""
    good = {"latt": "-22.9", "longt": "-43.2", "standard": {"addresst": "Rua A"}}
    run, seen = make_run(reply({}, 429), reply({"latt": "Throttled! See"}), reply(good))

    found = await geocode_xyz_one(run, "rua a", "key")

    assert has_point(found)
    assert len(seen) == 3
    assert seen[0].url.params["auth"] == "key"


@pytest.mark.parametrize(
    ("replies", "calls"),
    [
        pytest.param([reply({}, 402)], 1, id="no_credits_stops_at_once"),
        pytest.param([reply({"error": "x", "remaining_credits": 0})], 1, id="an_error_with_no_credits_left"),
        pytest.param([reply({}, 500), reply({}, 500), reply({}, 500)], 3, id="three_server_errors"),
        pytest.param([reply({"latt": "22.9", "longt": "-43"})] * 3, 3, id="coordinates_that_are_not_valid"),
    ],
)
async def test_geocode_xyz_gives_up_empty(
    make_run: Callable[..., tuple[Run, list[Request]]], replies: list[Response], calls: int
) -> None:
    """Return an empty geocode, after the tries that the case allows."""
    run, seen = make_run(*replies)

    assert not has_point(await geocode_xyz_one(run, "rua a", ""))
    assert len(seen) == calls


def make_client(*replies: Response | Exception) -> tuple[AsyncClient, list[Request]]:
    """Build a client whose service gives the replies in order."""
    seen: list[Request] = []
    queue = list(replies)

    def handler(request: Request) -> Response:
        seen.append(request)
        item = queue.pop(0)

        if isinstance(item, Exception):
            raise item

        return item

    return AsyncClient(transport=MockTransport(handler)), seen


async def test_a_geoapify_batch_gives_the_job_url_or_nothing() -> None:
    """Send the addresses, and read the URL of the job from a 200."""
    client, seen = make_client(reply({"url": "https://job"}), reply({}, 402), ConnectError("down"), reply({"url": 5}))

    assert await submit_geoapify(client, ["a", "b"], "k") == "https://job"
    assert await submit_geoapify(client, ["a"], "k") is None
    assert await submit_geoapify(client, ["a"], "k") is None
    assert await submit_geoapify(client, ["a"], "k") is None
    assert seen[0].url.params["apiKey"] == "k"
    assert seen[0].url.params["filter"] == "countrycode:br"
    assert seen[0].content == b'["a","b"]'


async def test_a_geoapify_job_is_polled_until_it_is_done() -> None:
    """Wait while it runs, and return the results of a completed job."""
    client, seen = make_client(
        reply({"status": "pending"}), reply({}, 500), reply({"status": "completed", "results": [{"lat": 1}]})
    )

    assert await poll_geoapify(client, "https://job", "k", no_pause) == [{"lat": 1}]
    assert len(seen) == 3


async def test_a_geoapify_job_that_fails_or_times_out_gives_nothing() -> None:
    """Stop at a failed job, and after 300 seconds of 5-second polls."""
    failed, _ = make_client(reply({"status": "failed"}))
    slow, seen = make_client(*[reply({"status": "pending"})] * 60)

    assert await poll_geoapify(failed, "https://job", "k", no_pause) is None
    assert await poll_geoapify(slow, "https://job", "k", no_pause) is None
    assert len(seen) == 60


def address(street: str | None, number: str | None = "10") -> Address:
    """Build an address."""
    return Address(logradouro_tratado=street, numero_porta=number, bairro="tijuca", endereco_completo="full")


def test_the_rows_keep_the_addresses_with_a_point_and_add_the_geocode_and_the_plus_code() -> None:
    """Write everything as text, and drop an address without a point."""
    found = {
        **empty_geocode("waze", TODAY),
        "latitude": -22.907104,
        "longitude": -43.172897,
        "logradouro_geocode": "rua a",
    }
    rows = join_rows([address("rua a"), address("rua b")], [found, empty_geocode("waze", TODAY)], need_street=False)

    assert len(rows) == 1
    assert rows[0]["logradouro_tratado"] == "rua a"
    assert (rows[0]["latitude"], rows[0]["geocode"], rows[0]["pluscode"], rows[0]["confianca"]) == (
        "-22.907104",
        "waze",
        "589R3RVG+5R",
        "",
    )


def test_the_fallback_needs_a_street_and_a_real_treated_street() -> None:
    """Drop a geocode with no street, and an address whose treated street is `nan`."""
    with_street = {**empty_geocode("waze", TODAY), "latitude": 1.0, "longitude": 2.0, "logradouro_geocode": "x"}
    no_street = {**empty_geocode("waze", TODAY), "latitude": 1.0, "longitude": 2.0}

    rows = join_rows(
        [address("a"), address("b"), address("nan")], [with_street, no_street, with_street], need_street=True
    )
    loose = join_rows([address("b")], [no_street], need_street=False)

    assert [row["logradouro_tratado"] for row in rows] == ["a"]
    assert len(loose) == 1


@pytest.mark.parametrize(
    ("latitude", "longitude", "expected"),
    [
        pytest.param("-22.907104", "-43.172897", "589R3RVG+5R", id="a_point_in_rio"),
        pytest.param("95", "0", None, id="a_latitude_out_of_range"),
        pytest.param("0", "190", None, id="a_longitude_out_of_range"),
        pytest.param("x", "0", None, id="not_a_number"),
    ],
)
def test_the_plus_code_comes_from_a_valid_point(latitude: str, longitude: str, expected: str | None) -> None:
    """Encode a point, and give nothing for one that is not valid."""
    assert to_pluscode(latitude, longitude) == expected


def test_a_value_is_written_as_text() -> None:
    """Give empty text for nothing."""
    assert (stringify(None), stringify(1.5), stringify("a")) == ("", "1.5", "a")
