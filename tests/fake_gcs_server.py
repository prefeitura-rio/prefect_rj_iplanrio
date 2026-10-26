"""The server of the fake GCS, with only the standard library.

It runs in a process of its own and `fixtures.py` starts it. This module imports nothing
heavy, so the process starts in milliseconds and not in the 3 seconds that `fixtures.py`
takes.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import md5
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from multiprocessing.connection import Connection
from re import findall
from threading import Lock
from typing import cast, override
from urllib.parse import parse_qs, unquote, urlparse
from xml.sax.saxutils import escape, unescape

NOT_FOUND = 404
NO_CONTENT = 204
PRECONDITION_FAILED = 412
PARTIAL = 206
NOT_IMPLEMENTED = 501


@dataclass
class FakeBlob:
    """One stored object."""

    data: bytes
    generation: int
    modified: datetime

    @property
    def etag(self) -> str:
        """Return the entity tag of the content."""
        return md5(self.data, usedforsecurity=False).hexdigest()


@dataclass
class FakeBucket:
    """The objects of a fake GCS, and the lock that keeps a conditional write atomic."""

    blobs: dict[tuple[str, str], FakeBlob] = field(default_factory=dict)
    lock: Lock = field(default_factory=Lock)
    generation: int = 0
    writes: list[tuple[str, str | None]] = field(default_factory=list)


def byte_range(header: str, size: int) -> tuple[int, int]:
    """Return the first and the last byte that a `Range: bytes=a-b` header asks for."""
    start, _, end = header.removeprefix("bytes=").partition("-")

    if not start:
        return max(size - int(end), 0), size - 1

    return int(start), min(int(end), size - 1) if end else size - 1


def listing(blobs: Mapping[str, FakeBlob], bucket: str, query: Mapping[str, str]) -> bytes:
    """Return the XML of the `list-type=2` answer, with the `delimiter` folded into prefixes."""
    prefix, delimiter = query.get("prefix", ""), query.get("delimiter", "")
    after = query.get("start-after", "")
    contents: list[tuple[str, FakeBlob]] = []
    prefixes: set[str] = set()

    for key in sorted(blobs):
        rest = key[len(prefix) :]

        if not key.startswith(prefix) or key <= after:
            continue

        if delimiter and delimiter in rest:
            prefixes.add(prefix + rest.split(delimiter)[0] + delimiter)
        else:
            contents.append((key, blobs[key]))

    xml = [f"<ListBucketResult><Name>{bucket}</Name><Prefix>{escape(prefix)}</Prefix>"]
    xml.append(f"<KeyCount>{len(contents)}</KeyCount><IsTruncated>false</IsTruncated>")

    for key, blob in contents:
        stamp = blob.modified.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        xml.append(f"<Contents><Key>{escape(key)}</Key><LastModified>{stamp}</LastModified>")
        xml.append(f'<ETag>"{blob.etag}"</ETag><Size>{len(blob.data)}</Size></Contents>')

    xml.extend(f"<CommonPrefixes><Prefix>{escape(item)}</Prefix></CommonPrefixes>" for item in sorted(prefixes))
    xml.append("</ListBucketResult>")

    return "".join(xml).encode()


def make_handler(state: FakeBucket) -> type[BaseHTTPRequestHandler]:
    """Build the request handler of a fake GCS over its state."""

    class FakeHandler(BaseHTTPRequestHandler):
        """The XML API of GCS that `deltalake` and `obstore` call."""

        protocol_version = "HTTP/1.1"
        disable_nagle_algorithm = True

        @override
        def log_message(self, format: str, *args: object) -> None:
            """Keep the test output quiet."""

        def target(self) -> tuple[str, str, dict[str, str]]:
            """Return the bucket, the key and the query of the request."""
            url = urlparse(self.path)
            bucket, _, key = unquote(url.path).lstrip("/").partition("/")

            return (
                bucket,
                key,
                {name: values[0] for name, values in parse_qs(url.query, keep_blank_values=True).items()},
            )

        def answer(self, status: int, body: bytes = b"", headers: Mapping[str, str] | None = None) -> None:
            """Send a response with its body."""
            self.send_response(status)

            for name, value in (headers or {}).items():
                self.send_header(name, value)

            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def describe(self, blob: FakeBlob) -> dict[str, str]:
            """Return the headers that describe an object."""
            return {
                "ETag": f'"{blob.etag}"',
                "x-goog-generation": str(blob.generation),
                "Last-Modified": blob.modified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            }

        def do_PUT(self) -> None:
            """Store an object, or copy one, and honor `x-goog-if-generation-match`."""
            bucket, key, _ = self.target()
            size = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(size) if size else b""
            source = self.headers.get("x-goog-copy-source")
            match = self.headers.get("x-goog-if-generation-match")

            with state.lock:
                if source:
                    origin = state.blobs.get(cast("tuple[str, str]", tuple(unquote(source).lstrip("/").split("/", 1))))

                    if origin is None:
                        return self.answer(NOT_FOUND)

                    body = origin.data

                state.writes.append((key, match))
                current = state.blobs.get((bucket, key))

                if match is not None and int(match) != (current.generation if current else 0):
                    return self.answer(PRECONDITION_FAILED, b"<Error><Code>PreconditionFailed</Code></Error>")

                state.generation += 1
                blob = FakeBlob(body, state.generation, datetime.now(UTC))
                state.blobs[(bucket, key)] = blob

            return self.answer(200, b"", self.describe(blob))

        def do_GET(self) -> None:
            """Read an object, with a range if asked, or list a prefix."""
            bucket, key, query = self.target()

            if not key:
                with state.lock:
                    mine = {name: blob for (owner, name), blob in state.blobs.items() if owner == bucket}

                return self.answer(200, listing(mine, bucket, query), {"Content-Type": "application/xml"})

            with state.lock:
                blob = state.blobs.get((bucket, key))

            if blob is None:
                return self.answer(NOT_FOUND, b"<Error><Code>NoSuchKey</Code></Error>")

            data, status, headers = blob.data, 200, self.describe(blob)

            if span := self.headers.get("Range"):
                first, last = byte_range(span, len(data))
                headers["Content-Range"] = f"bytes {first}-{last}/{len(data)}"
                data, status = data[first : last + 1], PARTIAL

            return self.answer(status, data, headers)

        def do_HEAD(self) -> None:
            """Describe an object without its body."""
            bucket, key, _ = self.target()

            with state.lock:
                blob = state.blobs.get((bucket, key))

            if blob is None:
                self.send_response(NOT_FOUND)
                self.send_header("Content-Length", "0")
            else:
                self.send_response(200)

                for name, value in self.describe(blob).items():
                    self.send_header(name, value)

                self.send_header("Content-Length", str(len(blob.data)))

            self.end_headers()

        def do_POST(self) -> None:
            """Delete many objects with one request, as `obstore` does for a list of keys."""
            bucket, _, query = self.target()
            size = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(size).decode() if size else ""

            if "delete" not in query:
                return self.answer(NOT_IMPLEMENTED)

            keys = [unescape(key) for key in findall(r"<Key>(.*?)</Key>", body)]

            with state.lock:
                for key in keys:
                    state.blobs.pop((bucket, key), None)

            deleted = "".join(f"<Deleted><Key>{escape(key)}</Key></Deleted>" for key in keys)

            return self.answer(
                200, f"<DeleteResult>{deleted}</DeleteResult>".encode(), {"Content-Type": "application/xml"}
            )

        def do_DELETE(self) -> None:
            """Delete an object."""
            bucket, key, _ = self.target()

            with state.lock:
                existed = state.blobs.pop((bucket, key), None)

            self.answer(NO_CONTENT if existed else NOT_FOUND)

    return FakeHandler


def serve(connection: Connection) -> None:
    """Serve a fake GCS on a free port, and send the port to the parent.

    It runs in a process.
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(FakeBucket()))

    connection.send(server.server_port)
    server.serve_forever()
