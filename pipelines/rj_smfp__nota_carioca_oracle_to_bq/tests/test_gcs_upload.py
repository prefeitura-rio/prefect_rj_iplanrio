# ruff: noqa: PLR2004
import pytest
import requests
from google.api_core import exceptions as api_exceptions
from google.resumable_media import InvalidResponse

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.gcs import (
    UPLOAD_MAX_ATTEMPTS,
    UploadAttempt,
    UploadConflictError,
    is_transport_error,
    run_upload,
)

LOCAL_SIZE = 1000
BLOB = "oracle_to_bq/DPS/run/chunk-000001.parquet"


class FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class FakeUpload:
    """Falha com os erros da fila, na ordem, e depois conclui; registra esperas e tentativas."""

    def __init__(self, failures: list[Exception], remote_size: int | None = None) -> None:
        self.failures = list(failures)
        self.remote = remote_size
        self.sends = 0
        self.sleeps: list[float] = []

    def send(self) -> None:
        self.sends += 1
        if self.failures:
            raise self.failures.pop(0)

    def remote_size(self) -> int | None:
        return self.remote

    def attempt(self) -> UploadAttempt:
        return UploadAttempt(self.send, self.remote_size, LOCAL_SIZE, BLOB)


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.ConnectionError("Connection aborted.", TimeoutError("The write operation timed out")),
        requests.exceptions.ReadTimeout("slow"),
        api_exceptions.ServiceUnavailable("503"),
        api_exceptions.InternalServerError("500"),
        api_exceptions.TooManyRequests("429"),
        InvalidResponse(FakeResponse(503), "unexpected"),
        InvalidResponse(FakeResponse(429), "unexpected"),
        InvalidResponse(FakeResponse(408), "unexpected"),
    ],
)
def test_transport_errors_are_retryable(error: Exception) -> None:
    # Given a transient network or service failure
    # When it is classified
    # Then it is retryable
    assert is_transport_error(error)


@pytest.mark.parametrize(
    "error",
    [
        api_exceptions.Forbidden("403"),
        api_exceptions.NotFound("404"),
        api_exceptions.PreconditionFailed("412"),
        InvalidResponse(FakeResponse(403), "unexpected"),
        ValueError("bug"),
    ],
)
def test_other_errors_are_not_retryable(error: Exception) -> None:
    # Given a permanent failure or a bug
    # When it is classified
    # Then it is not retryable
    assert not is_transport_error(error)


def test_upload_retries_transport_error_with_exponential_backoff_then_succeeds() -> None:
    # Given an upload that times out three times
    upload = FakeUpload([requests.exceptions.ConnectionError("aborted")] * 3)
    # When it runs
    run_upload(upload.attempt(), sleep=upload.sleeps.append)
    # Then it succeeds on the fourth attempt after waits of 5, 10 and 20 s
    assert upload.sends == 4
    assert upload.sleeps == [5.0, 10.0, 20.0]


def test_upload_gives_up_after_max_attempts_and_reraises() -> None:
    # Given an upload that always times out
    upload = FakeUpload([requests.exceptions.ConnectionError("aborted")] * 10)
    # When it runs
    with pytest.raises(requests.exceptions.ConnectionError):
        run_upload(upload.attempt(), sleep=upload.sleeps.append)
    # Then it stops at the attempt limit, sleeping between attempts only
    assert upload.sends == UPLOAD_MAX_ATTEMPTS == 5
    assert upload.sleeps == [5.0, 10.0, 20.0, 40.0]


def test_upload_does_not_retry_permanent_error() -> None:
    # Given an upload denied with 403
    upload = FakeUpload([api_exceptions.Forbidden("403")])
    # When it runs
    with pytest.raises(api_exceptions.Forbidden):
        run_upload(upload.attempt(), sleep=upload.sleeps.append)
    # Then it is attempted once
    assert upload.sends == 1


def test_precondition_failed_on_retry_with_same_size_is_success() -> None:
    # Given an attempt that timed out after the object was actually created
    upload = FakeUpload(
        [requests.exceptions.ConnectionError("aborted"), api_exceptions.PreconditionFailed("412")],
        remote_size=LOCAL_SIZE,
    )
    # When it runs
    run_upload(upload.attempt(), sleep=upload.sleeps.append)
    # Then the existing object counts as uploaded
    assert upload.sends == 2


def test_precondition_failed_on_retry_with_different_size_raises() -> None:
    # Given a retry whose 412 finds an object of another size
    upload = FakeUpload(
        [requests.exceptions.ConnectionError("aborted"), api_exceptions.PreconditionFailed("412")],
        remote_size=LOCAL_SIZE - 1,
    )
    # When it runs
    with pytest.raises(UploadConflictError):
        run_upload(upload.attempt(), sleep=upload.sleeps.append)
    # Then it is not retried further
    assert upload.sends == 2


def test_precondition_failed_on_first_attempt_raises_even_if_same_size() -> None:
    # Given an object that pre-existed the first attempt
    upload = FakeUpload([api_exceptions.PreconditionFailed("412")], remote_size=LOCAL_SIZE)
    # When it runs
    with pytest.raises(api_exceptions.PreconditionFailed):
        run_upload(upload.attempt(), sleep=upload.sleeps.append)
    # Then it is not treated as ours
    assert upload.sends == 1
