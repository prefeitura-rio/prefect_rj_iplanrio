"""The Discord webhook client for the load progress.

It keeps one message, made once and edited in place.

This file is the same in both Nota Carioca pipelines, and it imports nothing from
`pipelines.*`.

The golden rule: a notification problem NEVER fails or delays a load. Each failure
(network, 4xx and 5xx, 429, JSON) is logged once as a warning and swallowed. After a
failure the client waits longer before it tries again (`min_interval_seconds` doubles
at each failure in a row, up to 5 minutes), which limits the time lost to a Discord
that is down to the timeout of one request (10 s) for each try. The URL of the webhook
is a secret and never goes to the log.
"""

import time
from collections.abc import Callable, Mapping
from enum import StrEnum
from functools import wraps
from logging import INFO, WARNING, getLogger
from threading import Lock
from typing import cast, final

import httpx2
from prefect.runtime import deployment
from toolkit.telemetry import logger

from utils.records import dig, read_json
from utils.settings import NoticeSettings

STAGING_DEPLOYMENT_SUFFIX = "--staging"
WEBHOOK_ENV = "DISCORD_WEBHOOK_URL"
TIMEOUT_SECONDS = 10.0
DEFAULT_MIN_INTERVAL_SECONDS = 20.0
MAX_BACKOFF_SECONDS = 300.0
HTTP_NOT_FOUND = 404
HTTP_RATE_LIMITED = 429
HTTP_SUCCESS_MAX = 299
DEFAULT_RETRY_AFTER_SECONDS = 5.0
fallback_logger = getLogger(__name__)


def log_safely(message: str, level: str) -> None:
    """Write to the Prefect log, or to `logging` outside a run (tests, scripts)."""
    try:
        logger.info(message, level)
    except Exception:
        fallback_logger.log(WARNING if level == "warning" else INFO, message)


def warn(message: str) -> None:
    """Log a warning about a notification through the Prefect log."""
    log_safely(message, "warning")


def info(message: str) -> None:
    """Log an information about a notification through the Prefect log."""
    log_safely(message, "info")


@final
class Warner:
    """Emit one warning for each failure, and do not repeat the same message in a row.

    A network that is down then does not become a thousand lines.
    """

    def __init__(self, emit: Callable[[str], None] | None = None) -> None:
        """Keep the function that emits a warning, and the last message."""
        self.emit = emit
        self.last: str | None = None

    def __call__(self, message: str) -> None:
        """Avisa, a menos que seja igual ao último aviso."""
        if message != self.last:
            self.last = message
            (self.emit if self.emit is not None else warn)(message)

    def reset(self) -> None:
        """Esquece o último aviso; o próximo igual volta a aparecer."""
        self.last = None


def never_raises[**P](func: Callable[P, None]) -> Callable[P, None]:
    """Decorate a notification method to swallow any exception and log a warning.

    A bug in a notification then neither stops the load nor hides the original exception
    of the code that finishes it.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
        try:
            func(*args, **kwargs)
        except Exception as error:  # notificar nunca pode falhar a carga
            warn(f"Notificação do Discord falhou em {func.__name__}: {type(error).__name__}: {error}")

    return wrapper


class Environment(StrEnum):
    """The environment of the running deployment, which marks the messages."""

    PROD = "prod"
    STAGING = "staging"


def environment_from_deployment(name: str | None) -> Environment:
    """Give the environment of a deployment by its name.

    A name that ends in `--staging` is staging. Any other name, and a run with no
    deployment, is production.
    """
    return Environment.STAGING if name and name.endswith(STAGING_DEPLOYMENT_SUFFIX) else Environment.PROD


def current_environment() -> Environment:
    """Give the environment of the deployment of this flow run, and log it.

    A local run, with no deployment, is production. The webhook of each environment is
    in the secrets of that environment, so only the mark depends on this.
    """
    name = cast("str | None", deployment.name)
    environment = environment_from_deployment(name)
    info(f"Ambiente das notificações do Discord: {environment} (deployment {name or 'nenhum'}).")

    return environment


def webhook_from_env() -> str | None:
    """Read the URL of the webhook from `DISCORD_WEBHOOK_URL`.

    Return `None` if the variable does not exist or is empty.
    """
    value = NoticeSettings().DISCORD_WEBHOOK_URL.get_secret_value().strip()
    return value or None


def resolve_webhook() -> str | None:
    """Return the webhook URL from `DISCORD_WEBHOOK_URL`, or nothing if not set."""
    return webhook_from_env()


class DiscordStatusMessage:
    """One webhook message for each run: `publish` creates it, then edits it.

    The first `publish` is a POST and the next ones are a PATCH.

    The POST uses `?wait=true` to get the id of the message.

    Without a URL the client is off and makes no request. A deleted message (404 on the
    PATCH) is created again at the next `publish`. `force` ignores the minimum interval,
    the back-off after failures and the deadline of a 429, so the last update always
    tries once. A `webhook_url` of `None` or empty turns the client off.
    `min_interval_seconds` is the least time between edits without `force`, and
    `transport` is the `httpx2` transport (the tests pass a `MockTransport`).
    """

    def __init__(
        self,
        webhook_url: str | None,
        min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS,
        transport: httpx2.BaseTransport | None = None,
    ) -> None:
        """Keep the URL, the interval and the transport, and start with no message."""
        self.url: str | None = webhook_url.strip() if webhook_url and webhook_url.strip() else None
        self.interval: float = min_interval_seconds
        self.transport: httpx2.BaseTransport | None = transport
        self.message_id: str | None = None
        self.next_allowed: float = 0.0
        self.failures: int = 0
        self.warn: Warner = Warner()
        self.lock: Lock = Lock()
        if self.url is None:
            info(f"Notificações do Discord desligadas (parâmetro discord_notifications=false ou {WEBHOOK_ENV} vazia).")

    @property
    def enabled(self) -> bool:
        """Indica se há webhook configurado."""
        return self.url is not None

    def publish(self, payload: Mapping[str, object], force: bool = False) -> None:
        """Create the message on the first call, edit it later, and never raise.

        `payload` is the body of the webhook (`embeds`, `allowed_mentions`). `force`
        ignores the minimum interval and the back-off, for the new steps and the final
        state.
        """
        if self.url is None:
            return
        with self.lock:
            now = time.monotonic()
            if not force and now < self.next_allowed:
                return
            self.next_allowed = now + self.interval
            self.send(payload, now)

    def announce(self, payload: Mapping[str, object]) -> None:
        """Send a new, separate message (the one that notifies people), and never raise.

        `payload` is the body of the webhook, usually only `content`.
        """
        if self.url is None:
            return
        with self.lock:
            try:
                with httpx2.Client(timeout=TIMEOUT_SECONDS, transport=self.transport) as client:
                    response = client.post(self.url, params={"wait": "true"}, json=payload)
                if response.status_code > HTTP_SUCCESS_MAX:
                    self.warn(f"Discord recusou a mensagem final: HTTP {response.status_code}.")
            except Exception as error:
                self.warn(self.describe(error))

    def send(self, payload: Mapping[str, object], now: float) -> None:
        """Faz o POST ou o PATCH e atualiza o recuo; chamado com a trava."""
        if self.url is None:
            return
        try:
            with httpx2.Client(timeout=TIMEOUT_SECONDS, transport=self.transport) as client:
                if self.message_id is None:
                    response = client.post(self.url, params={"wait": "true"}, json=payload)
                else:
                    response = client.patch(f"{self.url}/messages/{self.message_id}", json=payload)
            self.handle(response, now)
        except Exception as error:
            self.failed(self.describe(error), now, 0.0)

    def handle(self, response: httpx2.Response, now: float) -> None:
        """Read the answer: keep the id, recreate a deleted message, respect a 429."""
        if response.status_code == HTTP_RATE_LIMITED:
            self.failed(
                "Discord limitou as requisições (HTTP 429); pulando até o prazo.", now, self.retry_after(response)
            )
        elif response.status_code == HTTP_NOT_FOUND and self.message_id is not None:
            self.message_id = None
            self.failed("A mensagem do Discord foi apagada; uma nova será criada na próxima atualização.", now, 0.0)
        elif response.status_code > HTTP_SUCCESS_MAX:
            self.failed(f"Discord respondeu HTTP {response.status_code}.", now, 0.0)
        elif self.message_id is None:
            message_id = dig(read_json(response), "id")
            if not isinstance(message_id, str):
                self.failed("Resposta do Discord sem o id da mensagem.", now, 0.0)
                return
            self.message_id = message_id
            self.succeeded()
        else:
            self.succeeded()

    @staticmethod
    def retry_after(response: httpx2.Response) -> float:
        """Read `retry_after` (seconds) from the JSON of a 429, or from the header."""
        try:
            return float(str(dig(read_json(response), "retry_after")))
        except ValueError:
            try:
                return float(response.headers["Retry-After"])
            except ValueError, KeyError:
                return DEFAULT_RETRY_AFTER_SECONDS

    def succeeded(self) -> None:
        """Clear the failures after a good request."""
        self.failures = 0
        self.warn.reset()

    def failed(self, message: str, now: float, retry_after: float) -> None:
        """Log the warning once and delay the next tries that have no `force`."""
        self.warn(message)
        self.failures += 1
        backoff: float = min(self.interval * 2.0**self.failures, MAX_BACKOFF_SECONDS)
        self.next_allowed = now + max(backoff, retry_after)

    def describe(self, error: Exception) -> str:
        """Descreve a falha sem vazar a URL do webhook."""
        text = str(error).replace(self.url or "", "<webhook>")
        return f"Falha ao falar com o Discord: {type(error).__name__}{': ' + text if text else ''}"
