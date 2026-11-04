from collections.abc import Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import final

import pytest

from utils import tasks
from utils.hsm import Hsm, Recipient, Sent, SentBatch
from utils.whitelist import Selection

SENT_AT = datetime(2026, 7, 17, 9, 30, 5, tzinfo=UTC)
HSM = Hsm(id_hsm=18, campaign_name="C", cost_center_id=71, chunk_size=2)
RECIPIENTS = [
    Recipient(to="1", external_id="a"),
    Recipient(to="2", external_id="b"),
    Recipient(to="3", external_id="c"),
]


@final
class Spies:
    """The doubles of what `run_hsm` calls: API, whitelist, pause and Discord."""

    def __init__(self, sent: Sent) -> None:
        """Store the arguments."""
        self.sent = sent
        self.sends: list[Sequence[Recipient]] = []
        self.whitelisted: list[tuple[list[str], Selection]] = []
        self.pauses: list[float] = []
        self.notices: list[str] = []

    async def send(self, hsm: Hsm, recipients: Sequence[Recipient]) -> tuple[Sent, datetime]:
        """Record the recipients and answer with the prepared result."""
        del hsm
        self.sends.append(recipients)

        return self.sent, SENT_AT

    async def whitelist(self, phones: Sequence[str], selection: Selection) -> int:
        """Record the phones and the selection."""
        self.whitelisted.append((list(phones), selection))

        return len(phones)

    async def pause(self, seconds: float) -> None:
        """Record a pause, and do not wait."""
        self.pauses.append(seconds)

    async def notice(self, message: str, *, errors: bool = False) -> None:
        """Record a notice."""
        del errors
        self.notices.append(message)


def install(monkeypatch: pytest.MonkeyPatch, sent: Sent) -> Spies:
    """Replace what `run_hsm` calls with the spies."""
    spies = Spies(sent)
    monkeypatch.setattr(tasks, "send_hsm", SimpleNamespace(fn=spies.send))
    monkeypatch.setattr(tasks, "whitelist_phones", SimpleNamespace(fn=spies.whitelist))
    monkeypatch.setattr(tasks, "sleep", spies.pause)
    monkeypatch.setattr(tasks, "post_notice", spies.notice)

    return spies


def two_batches() -> Sent:
    """Return a result where the two batches went out."""
    return Sent([SentBatch("C-2026-07-17-lote1", RECIPIENTS[:2]), SentBatch("C-2026-07-17-lote2", RECIPIENTS[2:])])


async def test_a_dispatch_waits_sends_and_posts_the_notice(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wait the minutes first, send once, return all batch rows, and post a notice.

    The notice has an example.
    """
    spies = install(monkeypatch, two_batches())

    run = await tasks.run_hsm.fn(HSM, RECIPIENTS, None, 5, test_mode=False)

    assert spies.pauses == [300]
    assert len(spies.sends) == 1
    assert run.error is None
    assert run.dispatch_date == "2026-07-17 09:30:05"
    assert [row["telefone"] for row in run.rows] == ["1", "2", "3"]
    assert [row["campaignName"] for row in run.rows] == [
        "C-2026-07-17-lote1",
        "C-2026-07-17-lote1",
        "C-2026-07-17-lote2",
    ]
    assert len(spies.notices) == 1
    assert "3 disparos" in spies.notices[0]
    assert "[TESTE]" not in spies.notices[0]


async def test_the_whitelist_gets_the_phones_before_the_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Put the phones on the whitelist with the selection, and skip it for 0 percent.

    No selection skips it too.
    """
    spies = install(monkeypatch, two_batches())

    await tasks.run_hsm.fn(HSM, RECIPIENTS, Selection(30, "group"), 0, test_mode=False)
    await tasks.run_hsm.fn(HSM, RECIPIENTS, Selection(0, "group"), 0, test_mode=False)
    await tasks.run_hsm.fn(HSM, RECIPIENTS, None, 0, test_mode=False)

    assert spies.whitelisted == [(["1", "2", "3"], Selection(30, "group"))]


async def test_a_failed_batch_keeps_the_rows_that_went_out_and_posts_no_success_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return the rows of batch 1 and the reason, so the flow records them.

    Then it raises.
    """
    sent = Sent([SentBatch("C-2026-07-17-lote1", RECIPIENTS[:2])], "Dispatch failed for batch 2: 500 boom")
    spies = install(monkeypatch, sent)

    run = await tasks.run_hsm.fn(HSM, RECIPIENTS, None, 0, test_mode=False)

    assert run.error == "Dispatch failed for batch 2: 500 boom"
    assert len(run.rows) == 2
    assert spies.notices == []


async def test_a_test_dispatch_says_so_in_its_notice(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mark the notice of a test."""
    spies = install(monkeypatch, two_batches())

    await tasks.run_hsm.fn(HSM, RECIPIENTS, None, 0, test_mode=True)

    assert "[TESTE]" in spies.notices[0]
