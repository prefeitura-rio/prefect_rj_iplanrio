"""Cliente do webhook do Discord para o progresso das cargas: uma mensagem criada uma vez e editada no lugar.

Este arquivo é idêntico nas duas pipelines da Nota Carioca e não importa nada de ``pipelines.*``.

Regra de ouro: um problema de notificação NUNCA falha nem atrasa uma carga. Toda falha (rede, 4xx/5xx, 429, JSON) é
registrada uma vez em log de aviso e engolida; depois de uma falha o cliente espera mais antes de tentar de novo
(``min_interval_seconds`` dobrando a cada falha seguida, até 5 min), o que limita o tempo perdido com um Discord fora
do ar ao timeout de uma requisição (10 s) por tentativa. A URL do webhook é um segredo e nunca vai para o log.
"""

import logging
import os
import threading
import time
from collections.abc import Callable, Mapping
from functools import wraps

import httpx

from iplanrio.pipelines_utils.logging import log

WEBHOOK_ENV = "DISCORD_WEBHOOK_URL_NOTA_CARIOCA"
TIMEOUT_SECONDS = 10.0
DEFAULT_MIN_INTERVAL_SECONDS = 20.0
MAX_BACKOFF_SECONDS = 300.0
HTTP_NOT_FOUND = 404
HTTP_RATE_LIMITED = 429
HTTP_SUCCESS_MAX = 299
DEFAULT_RETRY_AFTER_SECONDS = 5.0
_fallback_logger = logging.getLogger(__name__)


def _log_safely(message: str, level: str) -> None:
    """Escreve no log do Prefect; fora de um run (testes, scripts) cai no ``logging`` padrão."""
    try:
        log(message, level)
    except Exception:
        _fallback_logger.log(logging.WARNING if level == "warning" else logging.INFO, message)


def warn(message: str) -> None:
    """Registra um aviso de notificação pelo log do Prefect (``iplanrio.pipelines_utils.logging.log``)."""
    _log_safely(message, "warning")


def info(message: str) -> None:
    """Registra uma informação de notificação pelo log do Prefect."""
    _log_safely(message, "info")


class Warner:
    """Emite um aviso por falha, sem repetir a mesma mensagem em seguida (rede fora do ar não vira mil linhas)."""

    def __init__(self, emit: Callable[[str], None] | None = None) -> None:
        self._emit = emit
        self._last: str | None = None

    def __call__(self, message: str) -> None:
        """Avisa, a menos que seja igual ao último aviso."""
        if message != self._last:
            self._last = message
            (self._emit if self._emit is not None else warn)(message)

    def reset(self) -> None:
        """Esquece o último aviso; o próximo igual volta a aparecer."""
        self._last = None


def never_raises[**P](func: Callable[P, None]) -> Callable[P, None]:
    """Decora um método de notificação para engolir qualquer exceção, registrando um aviso.

    Garante que um bug de notificação não derrube a carga nem esconda a exceção original de quem a finaliza.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
        try:
            func(*args, **kwargs)
        except Exception as error:  # notificar nunca pode falhar a carga
            warn(f"Notificação do Discord falhou em {func.__name__}: {type(error).__name__}: {error}")

    return wrapper


def webhook_from_env(environ: Mapping[str, str] | None = None) -> str | None:
    """Lê a URL do webhook de ``DISCORD_WEBHOOK_URL_NOTA_CARIOCA``.

    :param environ: Variáveis de ambiente; o padrão é ``os.environ``.
    :returns: A URL, ou ``None`` se a variável não existir ou estiver vazia.
    """
    value = (environ if environ is not None else os.environ).get(WEBHOOK_ENV, "").strip()
    return value or None


class DiscordStatusMessage:
    """Uma mensagem de webhook por execução: o primeiro ``publish`` cria (POST), os seguintes editam (PATCH).

    O POST usa ``?wait=true`` para receber o id da mensagem.

    Sem URL o cliente fica desligado e nenhuma requisição é feita. Mensagem apagada (404 no PATCH) é recriada no
    próximo ``publish``. ``force`` ignora o intervalo mínimo, o recuo depois de falhas e o prazo de um 429: a
    atualização final tenta sempre uma vez.

    :param webhook_url: URL do webhook; ``None`` ou vazia desliga o cliente.
    :param min_interval_seconds: Intervalo mínimo entre edições sem ``force``.
    :param transport: Transporte do ``httpx`` (os testes passam um ``MockTransport``).
    """

    def __init__(
        self,
        webhook_url: str | None,
        min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._url = webhook_url.strip() if webhook_url and webhook_url.strip() else None
        self._interval = min_interval_seconds
        self._transport = transport
        self._message_id: str | None = None
        self._next_allowed = 0.0
        self._failures = 0
        self._warn = Warner()
        self._lock = threading.Lock()
        if self._url is None:
            info(f"Notificações do Discord desligadas (parâmetro discord_notifications=false ou {WEBHOOK_ENV} vazia).")

    @property
    def enabled(self) -> bool:
        """Indica se há webhook configurado."""
        return self._url is not None

    def publish(self, payload: Mapping[str, object], force: bool = False) -> None:
        """Cria a mensagem na primeira chamada e a edita nas seguintes; nunca levanta exceção.

        :param payload: Corpo do webhook (``embeds``, ``allowed_mentions``).
        :param force: Ignora o intervalo mínimo e o recuo (etapas novas e o estado final).
        """
        if self._url is None:
            return
        with self._lock:
            now = time.monotonic()
            if not force and now < self._next_allowed:
                return
            self._next_allowed = now + self._interval
            self._send(payload, now)

    def announce(self, payload: Mapping[str, object]) -> None:
        """Envia uma mensagem nova e avulsa (a que notifica as pessoas); nunca levanta exceção.

        :param payload: Corpo do webhook, normalmente só ``content``.
        """
        if self._url is None:
            return
        with self._lock:
            try:
                with httpx.Client(timeout=TIMEOUT_SECONDS, transport=self._transport) as client:
                    response = client.post(self._url, params={"wait": "true"}, json=payload)
                if response.status_code > HTTP_SUCCESS_MAX:
                    self._warn(f"Discord recusou a mensagem final: HTTP {response.status_code}.")
            except Exception as error:
                self._warn(self._describe(error))

    def _send(self, payload: Mapping[str, object], now: float) -> None:
        """Faz o POST ou o PATCH e atualiza o recuo; chamado com a trava."""
        if self._url is None:
            return
        try:
            with httpx.Client(timeout=TIMEOUT_SECONDS, transport=self._transport) as client:
                if self._message_id is None:
                    response = client.post(self._url, params={"wait": "true"}, json=payload)
                else:
                    response = client.patch(f"{self._url}/messages/{self._message_id}", json=payload)
            self._handle(response, now)
        except Exception as error:
            self._failed(self._describe(error), now, 0.0)

    def _handle(self, response: httpx.Response, now: float) -> None:
        """Interpreta a resposta: guarda o id, recria a mensagem apagada, respeita o 429."""
        if response.status_code == HTTP_RATE_LIMITED:
            self._failed(
                "Discord limitou as requisições (HTTP 429); pulando até o prazo.", now, self._retry_after(response)
            )
        elif response.status_code == HTTP_NOT_FOUND and self._message_id is not None:
            self._message_id = None
            self._failed("A mensagem do Discord foi apagada; uma nova será criada na próxima atualização.", now, 0.0)
        elif response.status_code > HTTP_SUCCESS_MAX:
            self._failed(f"Discord respondeu HTTP {response.status_code}.", now, 0.0)
        elif self._message_id is None:
            message_id = response.json().get("id")
            if not isinstance(message_id, str):
                self._failed("Resposta do Discord sem o id da mensagem.", now, 0.0)
                return
            self._message_id = message_id
            self._succeeded()
        else:
            self._succeeded()

    @staticmethod
    def _retry_after(response: httpx.Response) -> float:
        """Lê ``retry_after`` (segundos) do JSON do 429, ou o cabeçalho ``Retry-After``."""
        try:
            return float(response.json()["retry_after"])
        except (ValueError, KeyError, TypeError):
            try:
                return float(response.headers["Retry-After"])
            except (ValueError, KeyError):
                return DEFAULT_RETRY_AFTER_SECONDS

    def _succeeded(self) -> None:
        self._failures = 0
        self._warn.reset()

    def _failed(self, message: str, now: float, retry_after: float) -> None:
        """Registra o aviso uma vez e adia as próximas tentativas sem ``force``."""
        self._warn(message)
        self._failures += 1
        backoff = min(self._interval * 2**self._failures, MAX_BACKOFF_SECONDS)
        self._next_allowed = now + max(backoff, retry_after)

    def _describe(self, error: Exception) -> str:
        """Descreve a falha sem vazar a URL do webhook."""
        text = str(error).replace(self._url or "", "<webhook>")
        return f"Falha ao falar com o Discord: {type(error).__name__}{': ' + text if text else ''}"
