"""HTTP client for the WeTalkie API with login and automatic token refresh.

Every request carries a network timeout, so a slow or silent WeTalkie API
fails the call instead of hanging the Prefect worker indefinitely (the task
timeout of Prefect does not bound the socket wait of ``requests``)::

    from prefect_rj_iplanrio.wetalkie import ApiHandler

    api = ApiHandler(base_url=url, username=username, password=password)
    response = api.get("/callcenter/attendances/pull")
    response = api.post(f"/callcenter/hsm/send/{id_hsm}", json=payload, timeout=(10, 600))
"""

from typing import Any

import requests

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

Timeout = float | tuple[float, float]

DEFAULT_TIMEOUT: tuple[float, float] = (10.0, 300.0)
"""Default ``(connect, read)`` timeout in seconds for ``get``, ``post`` and ``put``."""

LOGIN_TIMEOUT: float = 30.0
"""Timeout in seconds for the login request."""

TOKEN_KEYS: tuple[str, ...] = ("token", "access_token", "authToken", "jwt")
"""Top-level keys of the login response that may hold the token, in lookup order."""

UNAUTHORIZED = 401


class WetalkieAuthenticationError(Exception):
    """Raised when the login request to the WeTalkie API fails."""


def is_positive_number(value: object) -> bool:
    """Tell whether a value is a positive ``int`` or ``float`` (``bool`` excluded).

    :param value: Value to check.
    :returns: ``True`` for a positive number.
    """
    return isinstance(value, int | float) and not isinstance(value, bool) and value > 0


def validate_timeout(timeout: Timeout | None) -> Timeout:
    """Check that a timeout is a positive number or a ``(connect, read)`` pair of them.

    :param timeout: Value given as ``timeout`` to the client or to one request.
    :returns: The timeout in seconds, as a ``float`` or a pair of ``float``.
    :raises ValueError: If the timeout is ``None``, not a number, not positive,
        or a tuple that is not a pair of positive numbers.
    """
    if isinstance(timeout, tuple):
        if len(timeout) == len(DEFAULT_TIMEOUT) and all(is_positive_number(value) for value in timeout):
            return (float(timeout[0]), float(timeout[1]))
    elif is_positive_number(timeout):
        return float(timeout)
    raise ValueError(f"timeout must be a positive number of seconds or a (connect, read) pair, got {timeout!r}")


def extract_token(response_data: dict[str, Any]) -> str | None:
    """Return the authentication token from a WeTalkie login response body.

    :param response_data: Decoded JSON body of the login response.
    :returns: The token, or ``None`` when no known key holds one.
    """
    for key in TOKEN_KEYS:
        if key in response_data:
            return response_data[key]
    data = response_data.get("data")
    if isinstance(data, dict) and isinstance(data.get("item"), dict) and "token" in data["item"]:
        return data["item"]["token"]
    return None


class ApiHandler:
    """Authenticated client for the WeTalkie API.

    Logs in on creation, sends the token on every request and, when a request
    answers 401, logs in again and repeats it once.
    """

    def __init__(  # noqa: PLR0913
        self,
        base_url: str,
        username: str,
        password: str,
        login_route: str = "users/login",
        token_type: str = "Bearer",
        *,
        timeout: Timeout = DEFAULT_TIMEOUT,
    ) -> None:
        """Create the client and log in.

        :param base_url: Base URL of the API, with or without a trailing slash.
        :param username: Login user.
        :param password: Login password.
        :param login_route: Path of the login endpoint, relative to ``base_url``.
        :param token_type: Scheme of the ``Authorization`` header.
        :param timeout: Default timeout of ``get``, ``post`` and ``put``, in
            seconds or as a ``(connect, read)`` pair.
        :raises ValueError: If ``timeout`` is not a valid timeout.
        :raises WetalkieAuthenticationError: If the login request fails.
        """
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.login_route = login_route.lstrip("/")
        self.token_type = token_type
        self.timeout = validate_timeout(timeout)
        self.token: str | None = None
        self.headers: dict[str, str] = {"Content-Type": "application/json"}

        self.login()

    def login(self) -> None:
        """Log in and store the token in the request headers.

        A response without a known token key is logged as a warning and keeps
        the previous headers, as the pipelines always did.

        :raises WetalkieAuthenticationError: If the login request fails or
            answers with an error status.
        """
        login_url = f"{self.base_url}/{self.login_route}"
        login_data = {"username": self.username, "password": self.password}

        try:
            response = requests.post(login_url, json=login_data, timeout=LOGIN_TIMEOUT)
            response.raise_for_status()
            response_data = response.json()
        except requests.RequestException as error:
            logger.error("Login failed: %s", error)
            raise WetalkieAuthenticationError(f"Failed to authenticate with API: {error}") from error

        logger.info("Login successful to %s", login_url)
        token = extract_token(response_data)
        if token:
            self.token = token
            self.headers["Authorization"] = f"{self.token_type} {token}"
            logger.info("Authentication token obtained successfully")
        else:
            logger.warning("No token found in login response; response keys: %s", list(response_data.keys()))

    def refresh_token_if_needed(self, response: requests.Response) -> bool:
        """Log in again when a response says the token expired.

        :param response: Response of the last request.
        :returns: ``True`` when the response was 401 and a new login was made.
        :raises WetalkieAuthenticationError: If the new login fails.
        """
        if response.status_code == UNAUTHORIZED:
            logger.info("Token expired, refreshing...")
            self.login()
            return True
        return False

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        """Send a request with the token and a timeout, repeating it once after a 401.

        :param method: HTTP method, such as ``"GET"``.
        :param path: Path relative to ``base_url``, with or without a leading slash.
        :param kwargs: Arguments of :func:`requests.request`. ``timeout``
            overrides the client default for this call and cannot be ``None``.
        :returns: The response of the request (of the repetition after a 401).
        :raises ValueError: If ``timeout`` is given and is not a valid timeout.
        :raises WetalkieAuthenticationError: If the new login after a 401 fails.
        :raises requests.RequestException: If the request fails or times out.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        kwargs["timeout"] = validate_timeout(kwargs.get("timeout", self.timeout))

        response = requests.request(method, url, headers=self.headers, **kwargs)
        if self.refresh_token_if_needed(response):
            response = requests.request(method, url, headers=self.headers, **kwargs)
        return response

    def get(self, path: str, params: dict | None = None, **kwargs: Any) -> requests.Response:
        """Send a GET request; see :meth:`request`.

        :param path: Path relative to ``base_url``.
        :param params: Query string parameters.
        :param kwargs: Other arguments of :func:`requests.request`.
        :returns: The response.
        """
        return self.request("GET", path, params=params, **kwargs)

    def post(
        self,
        path: str,
        json: dict | None = None,
        data: Any = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Send a POST request; see :meth:`request`.

        :param path: Path relative to ``base_url``.
        :param json: Body sent as JSON.
        :param data: Raw body.
        :param kwargs: Other arguments of :func:`requests.request`.
        :returns: The response.
        """
        return self.request("POST", path, json=json, data=data, **kwargs)

    def put(
        self,
        path: str,
        json: dict | None = None,
        data: Any = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Send a PUT request; see :meth:`request`.

        :param path: Path relative to ``base_url``.
        :param json: Body sent as JSON.
        :param data: Raw body.
        :param kwargs: Other arguments of :func:`requests.request`.
        :returns: The response.
        """
        return self.request("PUT", path, json=json, data=data, **kwargs)

    def get_token(self) -> str | None:
        """Return the current authentication token.

        :returns: The token, or ``None`` when the login returned none.
        """
        return self.token
