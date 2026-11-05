"""Retry condition for tasks that call the Salesforce API.

Only retries transient errors: 5xx, 429, timeouts and connection drops.
Any other exception — `ValueError`, `KeyError`, `RuntimeError` from bad
config or SQL — fails immediately: retrying would not help, and with
`concurrency_limit=1`, three attempts at 30/60/120 s would hold the queue
for ~3.5 minutes.
"""

from httpx2 import ConnectError, HTTPStatusError, TimeoutException

MIN_SERVER_ERROR = 500
TOO_MANY_REQUESTS = 429
TRANSIENT = (TimeoutException, ConnectError)


def is_transient_error(_task: object, _task_run: object, state: object) -> bool:
    """Return `True` if the failed state should be retried."""
    try:
        state.result()  # type: ignore[union-attr]
    except Exception as exc:
        error: BaseException | None = exc
        while error is not None and not isinstance(error, (*TRANSIENT, HTTPStatusError)):
            error = error.__cause__

        if isinstance(error, HTTPStatusError):
            if error.response is None:
                return True
            status = error.response.status_code
            return status >= MIN_SERVER_ERROR or status == TOO_MANY_REQUESTS
        return isinstance(error, TRANSIENT)
    return False
