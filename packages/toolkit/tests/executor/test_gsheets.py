from asyncio import run as run_async
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest
from google.auth.exceptions import TransportError
from httpx2 import Client
from hypothesis import given
from hypothesis import strategies as st

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.sources.backends.gsheets import GSheetsSource, HttpxRequest

POLL_SECONDS = 0.01


@given(first=st.text(min_size=1), second=st.text(min_size=1))
def test_the_access_token_is_a_bound_value_that_never_reaches_the_statement_text(
    first, second, fake_engine
) -> None:
    """Bind the access token, so it never reaches the statement text."""
    engines = [fake_engine(), fake_engine()]
    for engine, token in zip(engines, (first, second), strict=True):
        run_async(GSheetsSource(token=lambda token=token: token).setup(engine))

    assert engines[0].statements == engines[1].statements
    assert [engine.params for engine in engines] == [[[first]], [[second]]]


@pytest.mark.extensions
async def test_the_secret_is_valid_for_the_real_extension() -> None:
    """Create a secret that the real extension accepts."""
    source = GSheetsSource(token=lambda: "token")

    async with DuckDBEngine(extensions=source.extensions) as engine:
        await source.setup(engine)
        dataframes = [
            f async for f in engine.query("SELECT type FROM duckdb_secrets()")
        ]

    assert dataframes[0].rows() == [("gsheet",)]


@pytest.fixture
def http_server() -> Iterator[str]:
    """Serve `GET` with a body, and echo the body of a `POST`."""

    class Handler(BaseHTTPRequestHandler):
        def reply(self, body: bytes) -> None:
            """Send a 200 response with the body and the `X-Seen` header."""
            self.send_response(200)
            self.send_header("X-Seen", self.headers.get("X-Send", ""))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            """Reply `hello`."""
            self.reply(b"hello")

        def do_POST(self) -> None:
            """Echo the body of the request."""
            self.reply(self.rfile.read(int(self.headers["Content-Length"])))

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    Thread(target=server.serve_forever, args=(POLL_SECONDS,), daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


@pytest.mark.parametrize(
    ("method", "body", "expected"),
    [
        pytest.param("GET", None, b"hello", id="get_reads_the_body"),
        pytest.param("POST", b"payload", b"payload", id="post_sends_the_body"),
    ],
)
def test_the_google_auth_transport_reads_the_response_of_an_httpx2_request(
    http_server, method, body, expected
) -> None:
    """Read the response of an `httpx2` request through the `google-auth` transport."""
    with Client() as client:
        response = HttpxRequest(client)(
            http_server, method, body, headers={"X-Send": "yes"}, timeout=5
        )

    assert (response.status, response.data) == (200, expected)
    assert response.headers["x-seen"] == "yes"


def test_a_failed_connection_is_a_transport_error() -> None:
    """Raise a transport error if the connection fails."""
    with Client() as client, pytest.raises(TransportError):
        HttpxRequest(client)("http://127.0.0.1:1", timeout=2)
