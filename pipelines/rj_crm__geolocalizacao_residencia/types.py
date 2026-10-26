"""Types of rj_crm__geolocalizacao_residencia."""

from asyncio import Semaphore
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, TypedDict

from httpx2 import AsyncClient

type Strategy = Literal["nominatim", "fallback", "geoapify_batch"]
type Geocode = dict[str, str | float | None]
type Row = dict[str, str]
type Pause = Callable[[float], Awaitable[None]]


class Address(TypedDict):
    """An address of a citizen that has no coordinates yet."""

    logradouro_tratado: str | None
    numero_porta: str | None
    bairro: str | None
    endereco_completo: str | None


@dataclass(frozen=True)
class Step:
    """A service of the fallback: its name, the setting of its key (none if it needs no key), and its pace.

    `concurrent` is how many requests run at one time, and `wait` the seconds that each
    one rests after it.
    """

    name: str
    key_setting: str | None
    concurrent: int
    wait: float


@dataclass(frozen=True)
class Run:
    """What the requests of one run share: the client, the limit of requests at one time, the day, and the pace.

    `wait` is how many seconds each request rests after it, and `pause` is the function
    that waits.
    """

    client: AsyncClient
    slots: Semaphore
    today: str
    wait: float
    pause: Pause
