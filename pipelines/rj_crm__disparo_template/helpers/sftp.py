"""Send a file to the SFTP of Salesforce.

The server is checked by its pinned host key: the connection is refused
(`HostKeyNotVerifiable`) if the key that the server shows is not the one of the setting.
The files carry the CPF and the phone of citizens, so a man in the middle would leak
personal data.

The server (Globalscape EFT) offers only the old host key algorithm `ssh-rsa`. Paramiko
5 dropped it and has no public way to allow it, so the client is asyncssh, which takes
it by a documented argument and still checks the signature of the server.
"""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol, final

from asyncssh import SFTPClient, SSHClientConnection, connect, import_public_key
from toolkit.telemetry import logger

from pipelines.rj_crm__disparo_template.constants import SFTP_KEEPALIVE, SFTP_TIMEOUT
from pipelines.rj_crm__disparo_template.types import Csv


@dataclass(frozen=True)
class SftpTarget:
    """The SFTP server, its login, its pinned host key (a line of `known_hosts`) and the folder of the files."""

    host: str
    user: str
    password: str
    host_key: str
    port: int = 22
    remote_path: str = "/"


class Uploader(Protocol):
    """A session of the SFTP that can put a local file on the server."""

    async def put(self, local: Path, remote: str) -> None:
        """Copy a local file to a path of the server."""
        ...


type Opener = Callable[[SftpTarget], AbstractAsyncContextManager[Uploader]]


def remote_file(target: SftpTarget, name: str) -> str:
    """Return the path of a file on the server: the folder of the target, with no slash at its end, and the name."""
    folder = target.remote_path.rstrip("/")

    return f"{folder}/{name}" if folder else name


@final
class AsyncSshSession:
    """An SFTP session of asyncssh, which closes the SFTP client and then the connection."""

    def __init__(self, target: SftpTarget) -> None:
        self.target = target
        self.connection: SSHClientConnection | None = None
        self.sftp: SFTPClient | None = None

    async def __aenter__(self) -> "AsyncSshSession":
        """Connect with the pinned key and start the SFTP client."""
        pinned = import_public_key(self.target.host_key)
        logger.info(
            "Connecting to the SFTP: host=%s port=%d user=%s", self.target.host, self.target.port, self.target.user
        )
        self.connection = await connect(
            self.target.host,
            port=self.target.port,
            username=self.target.user,
            password=self.target.password,
            server_host_key_algs=[pinned.get_algorithm()],
            known_hosts=([pinned], [], []),
            connect_timeout=SFTP_TIMEOUT,
            keepalive_interval=SFTP_KEEPALIVE,
        )
        self.sftp = await self.connection.start_sftp_client()

        return self

    async def __aexit__(self, *error: object) -> None:
        """Close the SFTP client and the connection."""
        if self.sftp is not None:
            self.sftp.exit()

        if self.connection is not None:
            self.connection.close()
            await self.connection.wait_closed()

    async def put(self, local: Path, remote: str) -> None:
        """Copy a local file to a path of the server."""
        if self.sftp is None:
            raise RuntimeError("The SFTP session is not open")

        await self.sftp.put(local, remote)


def open_session(target: SftpTarget) -> AbstractAsyncContextManager[Uploader]:
    """Return the real SFTP session of a target."""
    return AsyncSshSession(target)


async def upload(target: SftpTarget, csv: Csv, opener: Opener = open_session) -> str:
    """Send a file to the SFTP and return its path on the server.

    `opener` makes the session, and the tests give another one. Raise
    `asyncssh.HostKeyNotVerifiable` if the host key is not the pinned one.
    """
    remote = remote_file(target, csv.name)

    with TemporaryDirectory() as directory:
        local = Path(directory) / csv.name
        local.write_text(csv.text, encoding="utf-8")

        async with opener(target) as session:
            await session.put(local, remote)

    logger.info("File sent to the SFTP: remote=%s", remote)

    return remote
