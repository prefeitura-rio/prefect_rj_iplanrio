"""The client of the Forca Municipal API."""

from asyncio import Lock, Semaphore, gather
from types import TracebackType
from typing import Self, final

from httpx2 import (
    AsyncClient,
    ConnectError,
    HTTPStatusError,
    Limits,
    Response,
    Timeout,
    TimeoutException,
)
from pydantic import JsonValue, TypeAdapter
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter
from toolkit.telemetry import logger

from pipelines.rj_segur__forca_municipal.constants import (
    API_CONNECT_TIMEOUT,
    API_TIMEOUT,
    MAX_CONNECTIONS,
    MAX_KEEPALIVE_CONNECTIONS,
    MAX_PAGE_RETRIES,
    MAX_RETRIES,
    RETRY_MAX_WAIT,
    RETRY_MIN_WAIT,
    SERVER_ERROR,
    UNAUTHORIZED,
)
from pipelines.rj_segur__forca_municipal.settings import Settings
from pipelines.rj_segur__forca_municipal.types import Page, Row

PAGE: TypeAdapter[Page] = TypeAdapter(Page)
JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


def is_retryable(error: BaseException) -> bool:
    """Tell whether a failed request is worth a new try: a network error or a 5xx."""
    if isinstance(error, HTTPStatusError):
        return error.response.status_code >= SERVER_ERROR

    return isinstance(error, ConnectError | TimeoutException)


def log_retry(state: RetryCallState) -> None:
    """Log a request that will run again."""
    error = state.outcome.exception() if state.outcome else None
    logger.warning("Retry %d of %d: %s: %s", state.attempt_number, MAX_RETRIES, type(error).__name__, error)


@final
class FMApi:
    """A logged-in API client with a pool of connections and retries.

    It logs in again after a 401.
    """

    def __init__(self, client: AsyncClient, login: tuple[str, str], concurrency: int) -> None:
        """Wrap a client with the user, the password and the most requests at once."""
        self.client = client
        self.login = login
        self.slots = Semaphore(concurrency)
        self.lock = Lock()
        self.token: str | None = None

    @classmethod
    def connect(cls, concurrency: int) -> Self:
        """Return a client of the API of the settings, to use with `async with`."""
        settings = Settings()
        client = AsyncClient(
            base_url=settings.FORCA_MUNICIPAL_URL.rstrip("/"),
            limits=Limits(max_connections=MAX_CONNECTIONS, max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS),
            proxy=settings.FORCA_MUNICIPAL_PROXY_URL if settings.FORCA_MUNICIPAL_USE_PROXY else None,
            timeout=Timeout(API_TIMEOUT, connect=API_CONNECT_TIMEOUT),
        )

        return cls(
            client,
            (settings.FORCA_MUNICIPAL_USERNAME, settings.FORCA_MUNICIPAL_PASSWORD.get_secret_value()),
            concurrency,
        )

    async def __aenter__(self) -> Self:
        """Log in and return the client."""
        await self.log_in()

        return self

    async def __aexit__(
        self, error_type: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None
    ) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def log_in(self) -> None:
        """Log in and keep the token."""
        user, password = self.login
        response = await self.client.post("/api/login", json={"userName": user, "password": password})
        response.raise_for_status()
        token = JSON.validate_json(response.content)

        if not isinstance(token, dict) or not isinstance(token.get("accessToken"), str):
            raise ValueError("The login of the Forca Municipal API has no accessToken")

        self.token = str(token["accessToken"])

    async def relog(self, stale: str | None) -> None:
        """Log in again only if the token is the one that got the 401.

        One request then does it.
        """
        async with self.lock:
            if self.token == stale:
                logger.warning("The token expired, logging in again")
                await self.log_in()

    async def send(self, path: str, params: dict[str, str | int]) -> Response:
        """Send a GET with the token."""
        return await self.client.get(path, params=params, headers={"Authorization": f"Bearer {self.token}"})

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential_jitter(initial=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
        retry=retry_if_exception(is_retryable),
        before_sleep=log_retry,
        reraise=True,
    )
    async def get(self, path: str, params: dict[str, str | int] | None = None) -> Response:
        """Send a GET, log in again after a 401, and retry a network error or 5xx.

        Raise `HTTPStatusError` for any other error status.
        """
        async with self.slots:
            stale = self.token
            response = await self.send(path, params or {})

            if response.status_code == UNAUTHORIZED:
                await self.relog(stale)
                response = await self.send(path, params or {})

            response.raise_for_status()

            return response

    async def get_page(self, path: str, page: int, page_size: int, extra: dict[str, str] | None = None) -> Page:
        """Return a page of a list endpoint."""
        response = await self.get(path, {"page": page, "pageSize": page_size, **(extra or {})})

        return PAGE.validate_json(response.content)

    async def get_first(self, path: str, page_size: int, extra: dict[str, str] | None = None) -> tuple[int, list[Row]]:
        """Return how many pages the endpoint has, and the items of the first."""
        body = await self.get_page(path, 1, page_size, extra)
        items = body.get("data", [])
        total = body.get("totalPages", 1)
        logger.info("GET %s: page 1 of %d, %d items", path, total, len(items))

        return total, items

    async def get_pages(
        self, path: str, first: int, last: int, page_size: int, extra: dict[str, str] | None = None
    ) -> list[Row]:
        """Return the items of the pages `first` to `last`, in page order.

        The pages are read at the same time.

        The pages that fail go again in up to two more rounds.
        Raise `RuntimeError` if a page still fails.
        """
        pending = list(range(first, last + 1))
        found: dict[int, list[Row]] = {}

        for attempt in range(MAX_PAGE_RETRIES + 1):
            if not pending:
                break

            if attempt:
                logger.warning("Round %d of %d for the pages %s", attempt, MAX_PAGE_RETRIES, pending)

            outcomes = await gather(
                *(self.get_page(path, page, page_size, extra) for page in pending), return_exceptions=True
            )
            failed: list[int] = []

            for page, outcome in zip(pending, outcomes, strict=True):
                if isinstance(outcome, BaseException):
                    failed.append(page)
                else:
                    found[page] = outcome.get("data", [])

            pending = failed

        if pending:
            raise RuntimeError(f"GET {path}: the pages {pending} failed after {MAX_PAGE_RETRIES + 1} rounds")

        logger.info("GET %s: pages %d to %d read", path, first, last)

        return [item for page in range(first, last + 1) for item in found[page]]
