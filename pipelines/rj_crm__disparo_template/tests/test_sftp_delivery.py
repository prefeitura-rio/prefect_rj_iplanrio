from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from re import fullmatch
from typing import final, override

import asyncssh
import polars as pl
import pytest

from pipelines.rj_crm__disparo_template import tasks
from pipelines.rj_crm__disparo_template.tests.conftest import NOW
from pipelines.rj_crm__disparo_template.types import Campaign, Delivery

STEM = "DE_CAMP"
CAMPAIGN = Campaign(name="camp", environment="staging")


@final
class OneUser(asyncssh.SSHServer):
    """A server that accepts one user with one password."""

    @override
    def begin_auth(self, username: str) -> bool:
        """Ask for a password, from any user."""
        del username
        return True

    @override
    def password_auth_supported(self) -> bool:
        """Accept the password login."""
        return True

    @override
    def validate_password(self, username: str, password: str) -> bool:
        """Accept only `sf` with `pw`."""
        return (username, password) == ("sf", "pw")


@final
class MarketingCloud:
    """The side of Marketing Cloud that reads the folder of the SFTP.

    It checks what an import of a data extension needs: the name of each file, one
    header, the same fields in every row, `LOCALE`, and one row for each CPF in all the
    files together.
    """

    def __init__(self, folder: Path, columns: Sequence[str], separator: str = ";") -> None:
        """Store the folder and the format that the data extension expects."""
        self.folder = folder
        self.header = ["telefone", "SubscriberKey", *columns, "LOCALE"]
        self.separator = separator

    def files(self) -> list[Path]:
        """Return the files of the folder, in the order of their names."""
        return sorted(self.folder.glob("*.csv"))

    def rows(self) -> list[dict[str, str]]:
        """Read every file, check its contract, and return the rows.

        Raise `AssertionError` for the first thing that the import would refuse.
        """
        names = [path.name for path in self.files()]
        assert len(names) == len(set(names))
        rows: list[dict[str, str]] = []

        for path in self.files():
            assert fullmatch(rf"{STEM}_\d{{14}}_\d{{3}}\.csv", path.name), path.name
            lines = path.read_text(encoding="utf-8").splitlines()
            assert lines[0].split(self.separator) == self.header, lines[0]
            for line in lines[1:]:
                values = line.split(self.separator)
                assert len(values) == len(self.header), line
                row = dict(zip(self.header, values, strict=True))
                assert row["LOCALE"] == "BR"
                assert row.get("NOME_ARQUIVO", path.name) == path.name
                rows.append(row)

        cpfs = [row["SubscriberKey"] for row in rows]
        assert len(cpfs) == len(set(cpfs)), "a CPF came twice"

        return rows


@pytest.fixture
async def sftp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Path]:
    """Start an SFTP server in a folder and point the pipeline at it."""
    key = asyncssh.generate_private_key("ssh-ed25519")
    folder = tmp_path / "sftp"
    folder.mkdir()

    def serve(channel: asyncssh.SSHServerChannel[bytes]) -> asyncssh.SFTPServer:
        """Serve the folder, with no way out of it."""
        return asyncssh.SFTPServer(channel, chroot=str(folder).encode())

    server = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_factory=OneUser,
        server_host_keys=[key],
        sftp_factory=serve,
    )
    environment = {
        "SFTP_HOST": "127.0.0.1",
        "SFTP_PORT": str(server.get_port()),
        "SFTP_USERNAME": "sf",
        "SFTP_PASSWORD": "pw",
        "SFTP_HOST_KEY": key.export_public_key().decode().strip(),
        "SFTP_PATH": "/",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    async def no_sleep(seconds: float) -> None:
        assert seconds >= 0

    monkeypatch.setattr(tasks, "sleep", no_sleep)
    monkeypatch.setattr(tasks, "now", lambda: NOW)

    yield folder

    server.close()
    await server.wait_closed()


def recipients(count: int) -> pl.DataFrame:
    """Return `count` recipients, each with its own CPF and phone."""
    return pl.DataFrame(
        {
            "cpf": [f"2000000000{number}" for number in range(count)],
            "telefone": [f"552190000000{number}" for number in range(count)],
            "nome": [f"Pessoa {number}" for number in range(count)],
        }
    )


async def test_batches_in_the_same_second_go_out_as_files_with_distinct_names(sftp: Path) -> None:
    """With a stopped clock, 5 recipients in batches of 2 give 3 files."""
    delivery = Delivery(data_extension=STEM, columns=["nome"], batch_size=2, sleep_minutes=0, batch_sleep_minutes=0)

    delivered = await tasks.deliver_task.fn(recipients(5), CAMPAIGN, delivery)

    cloud = MarketingCloud(sftp, ["nome"])
    assert [path.name for path in cloud.files()] == [
        "DE_CAMP_20260717093005_001.csv",
        "DE_CAMP_20260717093005_002.csv",
        "DE_CAMP_20260717093005_003.csv",
    ]
    assert len(cloud.rows()) == 5
    assert (delivered.batches, len(delivered.rows)) == (3, 5)


async def test_a_column_with_the_name_of_the_file_holds_the_name_of_each_file(sftp: Path) -> None:
    """The query has a `NOME_ARQUIVO` column: each file fills it with its own name."""
    delivery = Delivery(
        data_extension=STEM, columns=["nome", "NOME_ARQUIVO"], batch_size=2, sleep_minutes=0, batch_sleep_minutes=0
    )

    await tasks.deliver_task.fn(recipients(3).with_columns(pl.lit("").alias("NOME_ARQUIVO")), CAMPAIGN, delivery)

    rows = MarketingCloud(sftp, ["nome", "NOME_ARQUIVO"]).rows()
    assert sorted({row["NOME_ARQUIVO"] for row in rows}) == [
        "DE_CAMP_20260717093005_001.csv",
        "DE_CAMP_20260717093005_002.csv",
    ]


async def test_a_second_delivery_of_other_recipients_adds_files_and_keeps_the_first(
    sftp: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run one second later adds files, and the first file stays as it was."""
    delivery = Delivery(data_extension=STEM, columns=["nome"], sleep_minutes=0, batch_sleep_minutes=0)
    await tasks.deliver_task.fn(recipients(2), CAMPAIGN, delivery)
    first = (sftp / "DE_CAMP_20260717093005_001.csv").read_text(encoding="utf-8")
    other = recipients(2).with_columns(pl.Series("cpf", ["30000000000", "30000000001"]))
    monkeypatch.setattr(tasks, "now", lambda: NOW.replace(second=6))

    await tasks.deliver_task.fn(other, CAMPAIGN, delivery)

    assert (sftp / "DE_CAMP_20260717093005_001.csv").read_text(encoding="utf-8") == first
    assert len(MarketingCloud(sftp, ["nome"]).rows()) == 4
