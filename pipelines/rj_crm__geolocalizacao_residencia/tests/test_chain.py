from collections.abc import Sequence

import pytest

from pipelines.rj_crm__geolocalizacao_residencia.helpers import geocode as geocode_module
from pipelines.rj_crm__geolocalizacao_residencia.helpers.geocode import run_chain
from pipelines.rj_crm__geolocalizacao_residencia.helpers.providers import empty_geocode
from pipelines.rj_crm__geolocalizacao_residencia.helpers.rows import split_found
from pipelines.rj_crm__geolocalizacao_residencia.types import Address, Geocode, Step

KEYS = {
    "MAPTILER_API_KEY": "m",
    "GEOCODE_XYZ_API_KEY": "x",
    "LOCATIONIQ_API_KEY": "l",
    "OPENCAGE_API_KEY": "o",
}


def blank() -> Geocode:
    """Return the geocode of an address that no service found."""
    return empty_geocode("test", "2026-01-01")


def point() -> Geocode:
    """Return the geocode of an address that a service found."""
    geocode = blank()
    geocode["latitude"] = "-22.9"
    geocode["longitude"] = "-43.2"

    return geocode


def address(text: str) -> Address:
    """Return an address with the full text."""
    return {"logradouro_tratado": text, "numero_porta": "1", "bairro": "b", "endereco_completo": text}


def test_the_split_keeps_the_order_of_the_addresses_with_and_without_a_point() -> None:
    """Return the addresses with a point, their geocodes and the others."""
    addresses = [address("a"), address("b"), address("c")]
    geocodes: list[Geocode] = [point(), blank(), point()]

    found, points, left = split_found(addresses, geocodes)

    assert [item["endereco_completo"] for item in found] == ["a", "c"]
    assert points == [point(), point()]
    assert [item["endereco_completo"] for item in left] == ["b"]


@pytest.mark.parametrize(
    ("answers", "keys", "expected_calls", "expected_found"),
    [
        pytest.param(
            {"waze": [point(), blank()], "maptiler": [point()]},
            KEYS,
            ["waze", "maptiler"],
            ["a", "b"],
            id="each_service_takes_what_the_one_before_missed",
        ),
        pytest.param(
            {"waze": [blank(), blank()], "maptiler": [point(), point()]},
            KEYS,
            ["waze", "maptiler"],
            ["a", "b"],
            id="a_service_with_only_empty_answers_is_skipped",
        ),
        pytest.param(
            {"waze": [blank(), blank()], "maptiler": [point(), point()]},
            {**KEYS, "MAPTILER_API_KEY": ""},
            ["waze"],
            [],
            id="a_service_without_its_key_is_skipped",
        ),
    ],
)
async def test_the_chain_runs_the_services_in_order_over_what_is_left(
    monkeypatch: pytest.MonkeyPatch,
    answers: dict[str, list[Geocode]],
    keys: dict[str, str],
    expected_calls: list[str],
    expected_found: list[str],
) -> None:
    """Call a service only with the addresses that the others did not find."""
    calls: list[str] = []

    async def fake_step(step: Step, addresses: Sequence[str], key: str, today: str) -> list[Geocode]:
        del key, today
        calls.append(step.name)

        return list(answers.get(step.name, [blank()] * len(addresses)))[: len(addresses)]

    monkeypatch.setattr(geocode_module, "run_step", fake_step)

    found, points = await run_chain([address("a"), address("b")], keys, "2026-01-01")

    assert [item["endereco_completo"] for item in found] == expected_found
    assert len(points) == len(expected_found)
    assert calls[: len(expected_calls)] == expected_calls
