"""Types of rj_crm__geolocalizacao_residencia."""

from asyncio import Semaphore
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal

from httpx2 import AsyncClient, Response

type Strategy = Literal["nominatim", "fallback", "geoapify_batch"]
type Source = Literal["citizens", "chamados"]
type Address = dict[str, str | None]
type Geocode = dict[str, str | float | None]
type Row = dict[str, str]
type Pause = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class Step:
    """A service of the fallback: its name, the setting of its key and its pace.

    A service that needs no key has none.

    `concurrent` is how many requests run at one time, and `wait` the seconds that each
    one rests after it.
    """

    name: str
    key_setting: str | None
    concurrent: int
    wait: float


@dataclass(frozen=True)
class Run:
    """What the requests of a run share: client, request limit, day and pace.

    `wait` is how many seconds each request rests after it, and `pause` is the function
    that waits.
    """

    client: AsyncClient
    slots: Semaphore
    today: str
    wait: float
    pause: Pause


@dataclass(frozen=True)
class Query:
    """A GET request to a service."""

    url: str
    params: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Provider:
    """A geocoding service: its `geocode` name, its request and its reader.

    `parse` takes the address, the response and the day, and gives the geocode or
    nothing.
    """

    name: str
    build: Callable[[str], Query]
    parse: Callable[[Response, str], Geocode | None]
