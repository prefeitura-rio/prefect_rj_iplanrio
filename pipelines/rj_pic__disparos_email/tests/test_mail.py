from collections.abc import Callable

import pytest
from httpx2 import Response

from pipelines.rj_pic__disparos_email.helpers.mail import Pacer, Relay, build_payload, send_mail
from pipelines.rj_pic__disparos_email.tests.conftest import FakeDataRelay, Timeline, unavailable
from pipelines.rj_pic__disparos_email.types import Mail


def test_the_payload_has_the_mail_and_the_options_of_the_relay(
    make_relay: Callable[..., Relay], make_mail: Callable[..., Mail]
) -> None:
    """Send the address as a list, the HTML and Gmail flags, and the sender if set."""
    plain = build_payload(make_relay(use_gmail_api=False), make_mail())
    from_sender = build_payload(make_relay(from_address="noreply@example.org"), make_mail())

    assert plain == {
        "to_addresses": ["ana@example.org"],
        "subject": "Assunto",
        "body": "<p>oi</p>",
        "is_html_body": True,
        "use_gmail_api": False,
    }
    assert from_sender["from_address"] == "noreply@example.org"


async def test_a_good_mail_goes_out_with_the_key_in_the_header(
    make_relay: Callable[..., Relay],
    make_pacer: Callable[[float], Pacer],
    make_mail: Callable[..., Mail],
    timeline: Timeline,
) -> None:
    """Post once with the key, and return true."""
    relay = FakeDataRelay()

    sent = await send_mail(relay.client, make_relay(), make_mail(), make_pacer(1), timeline.pause)

    assert sent is True
    assert len(relay.requests) == 1
    assert relay.requests[0].headers["x-api-key"] == "key"
    assert timeline.pauses == []


@pytest.mark.parametrize(
    ("replies", "calls", "pauses", "sent"),
    [
        pytest.param([Response(500), Response(200, json={"success": True})], 2, [5], True, id="a_server_error_then_ok"),
        pytest.param(
            [unavailable(), Response(200, json={"success": True})], 2, [5], True, id="a_network_error_then_ok"
        ),
        pytest.param(
            [Response(200, json={"success": False}), Response(200, json={"success": True})],
            2,
            [5],
            True,
            id="not_accepted_then_ok",
        ),
        pytest.param(
            [Response(200, content=b"not json"), Response(200, json={"success": True})],
            2,
            [5],
            True,
            id="a_bad_body_then_ok",
        ),
        pytest.param([Response(500)] * 3, 3, [5, 5], False, id="every_try_fails"),
    ],
)
async def test_a_failure_that_may_pass_is_tried_again(
    make_relay: Callable[..., Relay],
    make_pacer: Callable[[float], Pacer],
    make_mail: Callable[..., Mail],
    replies: list[Response | Exception],
    calls: int,
    pauses: list[float],
    sent: bool,
) -> None:
    """Try up to three times, with the retry delay between tries, and give up after."""
    relay = FakeDataRelay(*replies)
    waits: list[float] = []

    async def pause(seconds: float) -> None:
        waits.append(seconds)

    result = await send_mail(relay.client, make_relay(), make_mail(), make_pacer(0), pause)

    assert result is sent
    assert len(relay.requests) == calls
    assert waits == pauses


@pytest.mark.parametrize("status", [400, 401, 403])
async def test_a_refused_mail_is_not_tried_again(
    make_relay: Callable[..., Relay],
    make_pacer: Callable[[float], Pacer],
    make_mail: Callable[..., Mail],
    timeline: Timeline,
    status: int,
) -> None:
    """Stop at the first answer of 400, 401 or 403."""
    relay = FakeDataRelay(Response(status))

    sent = await send_mail(relay.client, make_relay(), make_mail(), make_pacer(1), timeline.pause)

    assert sent is False
    assert len(relay.requests) == 1


async def test_the_sends_keep_the_pace_of_the_throttle(
    make_relay: Callable[..., Relay],
    make_pacer: Callable[[float], Pacer],
    make_mail: Callable[..., Mail],
    timeline: Timeline,
) -> None:
    """Wait the rest of the interval before a send that is too soon.

    The first send does not wait.
    """
    relay = FakeDataRelay()
    pacer = make_pacer(2)

    await send_mail(relay.client, make_relay(), make_mail("a@example.org"), pacer, timeline.pause)
    timeline.now += 0.5
    await send_mail(relay.client, make_relay(), make_mail("b@example.org"), pacer, timeline.pause)
    timeline.now += 10
    await send_mail(relay.client, make_relay(), make_mail("c@example.org"), pacer, timeline.pause)

    assert timeline.pauses == [1.5]
    assert len(relay.requests) == 3
