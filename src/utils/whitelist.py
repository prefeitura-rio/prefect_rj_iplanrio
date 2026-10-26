"""The whitelist of the beta groups: the numbers that may get a message in a test."""

from collections.abc import Sequence
from dataclasses import dataclass
from random import Random
from types import TracebackType
from typing import Self, final

from httpx2 import AsyncClient, HTTPError
from pydantic import JsonValue
from toolkit.telemetry import logger

from .records import dig, read_json
from .settings import WhitelistSettings

PAGE_SIZE = 100
MIN_DIGITS = 10
BRAZIL = "55"
NINTH_DIGIT_POSITION = 2


def normalize_numbers(number: str) -> list[str]:
    """Return a Brazilian number as `55`, area code and number.

    It gives the forms with and without the 9.

    ```python
    normalize_numbers("21987654321")  # ["5521987654321", "552187654321"]
    ```
    """
    digits = number if number.startswith(BRAZIL) else f"{BRAZIL}{number}"
    local = digits[2:]

    if len(local) >= MIN_DIGITS + 1 and local[NINTH_DIGIT_POSITION] == "9":
        with_nine, without_nine = digits, f"{BRAZIL}{local[:NINTH_DIGIT_POSITION]}{local[NINTH_DIGIT_POSITION + 1 :]}"
    else:
        without_nine = digits
        with_nine = f"{BRAZIL}{local[:NINTH_DIGIT_POSITION]}9{local[NINTH_DIGIT_POSITION:]}"

    return sorted({with_nine, without_nine})


def sample_numbers(numbers: Sequence[str], percentage: int, rng: Random) -> list[str]:
    """Return a random share of the different numbers, rounded down.

    `percentage` is a percent, and 100 or more gives all.
    """
    unique = sorted(set(numbers))
    count = int(len(unique) * percentage / 100)

    return unique if count >= len(unique) else rng.sample(unique, count)


@final
class Whitelist:
    """A client of the whitelist API. Call `authenticate` before any other method."""

    def __init__(self, client: AsyncClient, issuer: str, credentials: tuple[str, str]) -> None:
        """Wrap a client with the token issuer and the client id and secret."""
        self.client = client
        self.issuer = issuer
        self.credentials = credentials
        self.token: str | None = None

    @classmethod
    def connect(cls) -> Self:
        """Return a client of the API of the settings, to use with `async with`."""
        settings = WhitelistSettings()
        client = AsyncClient(base_url=settings.WHITELIST_API_BASE_URL, timeout=60)

        return cls(
            client,
            settings.WHITELIST_ISSUER,
            (settings.WHITELIST_CLIENT_ID, settings.WHITELIST_CLIENT_SECRET.get_secret_value()),
        )

    async def __aenter__(self) -> Self:
        """Return the client."""
        return self

    async def __aexit__(
        self, error_type: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None
    ) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def authenticate(self) -> bool:
        """Get an access token.

        Return `False` if the issuer refuses or answers with no token.
        """
        client_id, client_secret = self.credentials

        try:
            response = await self.client.post(
                f"{self.issuer}/protocol/openid-connect/token",
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": "client_credentials",
                    "scope": "profile email",
                },
            )
            response.raise_for_status()
            token = dig(read_json(response), "access_token")
        except (HTTPError, ValueError) as error:
            logger.warning("The whitelist login failed: %s", type(error).__name__)

            return False

        self.token = token if isinstance(token, str) and token else None

        return self.token is not None

    async def call(self, method: str, path: str, **options: JsonValue) -> JsonValue:
        """Send an authorized request and return its JSON body.

        Raise `HTTPStatusError` for an error status.
        """
        response = await self.client.request(method, path, headers={"Authorization": f"Bearer {self.token}"}, **options)  # pyright: ignore[reportArgumentType]  # `options` are the keyword arguments of the request
        response.raise_for_status()

        return read_json(response)

    async def find_group(self, name: str) -> str | None:
        """Return the id of the group with a name, or nothing."""
        groups = dig(await self.call("GET", "/groups"), "groups")

        if not isinstance(groups, list):
            return None

        for group in groups:
            if dig(group, "name") == name:
                found = dig(group, "id")

                return str(found) if found is not None else None

        return None

    async def create_group(self, name: str) -> str | None:
        """Create a group and return its id."""
        created = dig(await self.call("POST", "/groups", json={"name": name}), "id")

        return str(created) if created is not None else None

    async def existing_numbers(self) -> set[str]:
        """Return every number of the whitelist, with and without the 9.

        It reads page by page.
        """
        numbers: set[str] = set()
        page = 1

        while True:
            body = await self.call("GET", "/whitelist", params={"page": page, "per_page": PAGE_SIZE})
            entries = dig(body, "whitelisted")
            entries = entries if isinstance(entries, list) else []

            for entry in entries:
                digits = "".join(char for char in str(dig(entry, "phone_number") or "") if char.isdigit())

                if len(digits) >= MIN_DIGITS:
                    numbers.update(normalize_numbers(digits))

            if len(entries) < PAGE_SIZE:
                return numbers

            page += 1

    async def add_numbers(self, group_id: str, numbers: Sequence[str]) -> None:
        """Add numbers to a group."""
        await self.call("POST", "/whitelist/bulk-add", json={"group_id": group_id, "phone_numbers": list(numbers)})

    async def remove_numbers(self, numbers: Sequence[str]) -> None:
        """Remove numbers from the whitelist, whatever their group."""
        await self.call("POST", "/whitelist/bulk-remove", json={"phone_numbers": list(numbers)})


@dataclass(frozen=True)
class Selection:
    """A share of the phones for a group: `percentage` percent, into `group`.

    With `force`, the numbers that the whitelist already has are sent again.
    """

    percentage: int
    group: str
    force: bool = False


async def add_to_whitelist(whitelist: Whitelist, phones: Sequence[str], selection: Selection, rng: Random) -> int:
    """Add a random share of the phones to a group, and create the group if missing.

    A number that the whitelist already has, with or without the 9, is skipped, unless
    `selection.force` is on. Each number goes in with and without the 9 after the area
    code. Return how many different numbers were sent. Raise `RuntimeError` if the login
    fails or the group cannot be found or created.
    """
    selected = sample_numbers(phones, selection.percentage, rng)

    if not selected:
        logger.info("Whitelist: the share of %d%% gives no number", selection.percentage)

        return 0

    if not await whitelist.authenticate():
        raise RuntimeError("The whitelist login failed")

    group = selection.group
    group_id = await whitelist.find_group(group) or await whitelist.create_group(group)

    if group_id is None:
        raise RuntimeError(f"The whitelist group {group!r} was not found and could not be created")

    known: set[str] = set() if selection.force else await whitelist.existing_numbers()
    new = sorted(
        {
            variant
            for number in selected
            if known.isdisjoint(normalize_numbers(number))
            for variant in normalize_numbers(number)
        }
    )

    if not new:
        logger.info("Whitelist: every selected number is already there: group=%s", group)

        return 0

    await whitelist.add_numbers(group_id, new)
    logger.info("Whitelist: numbers added: group=%s count=%d", group, len(new))

    return len(new)


async def remove_from_whitelist(whitelist: Whitelist, phones: Sequence[str]) -> int:
    """Remove the phones, with and without the 9, from the whitelist.

    Return how many numbers were sent.
    """
    numbers = sorted({variant for phone in phones for variant in normalize_numbers(phone)})

    if not numbers or not await whitelist.authenticate():
        return 0

    await whitelist.remove_numbers(numbers)
    logger.info("Whitelist: numbers removed: count=%d", len(numbers))

    return len(numbers)
