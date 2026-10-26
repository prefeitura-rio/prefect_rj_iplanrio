from datetime import UTC, datetime
from typing import final

import pytest
from httpx2 import AsyncClient, ConnectError, MockTransport, Request, Response
from pydantic import JsonValue, TypeAdapter, ValidationError

from utils.hsm import (
    Hsm,
    Recipient,
    SentBatch,
    batch_name,
    build_payload,
    build_rows,
    drop_repeats,
    parse_recipients,
    results_notice,
    send_batches,
    split_batches,
    success_notice,
)
from utils.wetalkie import WetalkieApi

SENT_AT = datetime(2026, 7, 17, 9, 30, 5, tzinfo=UTC)
HSM = Hsm(id_hsm=18, campaign_name="SISREG", cost_center_id=71, chunk_size=2)
PAYLOAD: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])


@final
class FakeWetalkie:
    """The Wetalkie API: it logs in, answers `GET /` and records the batches that it gets.

    `replies` are the answers to the sends, in order. A send with no reply left answers
    201.
    """

    def __init__(self, replies: list[Response | Exception] | None = None, root_status: int = 200) -> None:
        """Store the arguments."""
        self.replies = list(replies or [])
        self.root_status = root_status
        self.logins = 0
        self.payloads: list[dict[str, JsonValue]] = []
        client = AsyncClient(transport=MockTransport(self.handle), base_url="https://w.test")
        self.api = WetalkieApi(client, "users/login", ("u", "p"))

    def handle(self, request: Request) -> Response:
        """Answer a request."""
        if request.url.path == "/users/login":
            self.logins += 1

            return Response(200, json={"token": f"t{self.logins}"})

        if request.method == "GET":
            return Response(self.root_status, json={})

        self.payloads.append(PAYLOAD.validate_json(request.content))
        reply = self.replies.pop(0) if self.replies else Response(201, json={})

        if isinstance(reply, Exception):
            raise reply

        return reply


def make_recipients(count: int) -> list[Recipient]:
    """Build `count` recipients with distinct phones."""
    return [Recipient(to=f"5521{number:08d}", external_id=f"cpf{number}") for number in range(count)]


def test_a_recipient_has_the_destination_format_of_the_api() -> None:
    """Send `to`, and `externalId` and `vars` only when the recipient has them."""
    full = Recipient(to=" 5521999 ", external_id="123", vars={"NOME": "Ana"})

    assert full.as_destination() == {"to": "5521999", "externalId": "123", "vars": {"NOME": "Ana"}}
    assert Recipient(to="5521888").as_destination() == {"to": "5521888"}


@pytest.mark.parametrize("phone", ["", "   "])
def test_a_recipient_with_no_phone_is_refused(phone: str) -> None:
    """Refuse a phone that is empty, or only spaces."""
    with pytest.raises(ValidationError):
        Recipient(to=phone)


@pytest.mark.parametrize(
    ("count", "size", "sizes"),
    [
        pytest.param(0, 2, [], id="none"),
        pytest.param(4, 2, [2, 2], id="exact"),
        pytest.param(5, 2, [2, 2, 1], id="the_last_is_smaller"),
        pytest.param(1, 1000, [1], id="one_batch"),
    ],
)
def test_the_recipients_are_cut_in_batches(count: int, size: int, sizes: list[int]) -> None:
    """Cut by size, with what is left in the last batch."""
    assert [len(batch) for batch in split_batches(make_recipients(count), size)] == sizes


def test_each_batch_is_a_campaign_with_its_number_and_the_day() -> None:
    """Name the campaign with the day and the number of the batch."""
    assert batch_name(HSM, "2026-07-17", 2) == "SISREG-2026-07-17-lote2"


def test_the_payload_has_the_campaign_the_cost_center_and_the_destinations() -> None:
    """Send the keys that the API expects."""
    payload = build_payload(HSM, "SISREG-x-lote1", [Recipient(to="1", external_id="9")])

    assert payload == {
        "campaignName": "SISREG-x-lote1",
        "costCenterId": 71,
        "destinations": [{"to": "1", "externalId": "9"}],
    }


async def test_every_batch_is_sent_with_its_own_campaign() -> None:
    """Send three batches of 2, 2 and 1."""
    fake = FakeWetalkie()

    sent = await send_batches(fake.api, HSM, make_recipients(5), SENT_AT)

    assert sent.error is None
    assert [len(batch.recipients) for batch in sent.batches] == [2, 2, 1]
    assert [payload["campaignName"] for payload in fake.payloads] == [
        "SISREG-2026-07-17-lote1",
        "SISREG-2026-07-17-lote2",
        "SISREG-2026-07-17-lote3",
    ]
    assert [batch.campaign_name for batch in sent.batches] == [payload["campaignName"] for payload in fake.payloads]


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        pytest.param(Response(500, text="boom"), "500 boom", id="an_error_status"),
        pytest.param(ConnectError("down"), "ConnectError", id="a_network_error"),
    ],
)
async def test_a_failed_batch_stops_the_run_and_keeps_the_batches_before_it(
    failure: Response | Exception, message: str
) -> None:
    """Keep batch 1, stop at batch 2, and say why. Batch 3 is not sent."""
    fake = FakeWetalkie([Response(201, json={}), failure])

    sent = await send_batches(fake.api, HSM, make_recipients(5), SENT_AT)

    assert len(sent.batches) == 1
    assert sent.error is not None
    assert sent.error.startswith("Dispatch failed for batch 2")
    assert message in sent.error
    assert len(fake.payloads) == 2


async def test_a_401_logs_in_again_and_sends_the_batch_once_more() -> None:
    """Log in again after a 401 and resend the same batch."""
    fake = FakeWetalkie([Response(401), Response(201, json={})])

    sent = await send_batches(fake.api, HSM, make_recipients(2), SENT_AT)

    assert sent.error is None
    assert len(sent.batches) == 1
    assert fake.logins == 2
    assert fake.payloads[0] == fake.payloads[1]


async def test_an_api_that_does_not_answer_the_root_sends_nothing() -> None:
    """Raise before the first batch if `GET /` is not 200."""
    fake = FakeWetalkie(root_status=503)

    with pytest.raises(RuntimeError, match="503"):
        await send_batches(fake.api, HSM, make_recipients(2), SENT_AT)

    assert fake.payloads == []


def test_a_destination_of_the_query_is_read_as_a_recipient() -> None:
    """Read the phone and the id by any of their names and in any case, and keep the vars."""
    texts = [
        '{"celular_disparo": "5521999", "externalId": "111", "vars": {"NOME": "Ana"}}',
        '{"telefone": "5521888", "CPF": " 222 "}',
        '{"to": "5521777"}',
    ]

    assert parse_recipients(texts) == [
        Recipient(to="5521999", external_id="111", vars={"NOME": "Ana"}),
        Recipient(to="5521888", external_id="222"),
        Recipient(to="5521777"),
    ]


def test_a_destination_with_no_phone_is_left_out_and_with_require_id_so_is_one_with_no_id() -> None:
    """Drop the invalid destinations, and keep the others."""
    texts = [
        '{"externalId": "1"}',
        '{"to": "5521999", "externalId": "2"}',
        '{"to": "5521888"}',
        '{"to": "5521777", "externalId": " "}',
    ]

    assert [recipient.to for recipient in parse_recipients(texts)] == ["5521999", "5521888", "5521777"]
    assert [recipient.to for recipient in parse_recipients(texts, require_id=True)] == ["5521999"]


def test_a_query_with_no_valid_destination_is_refused_but_an_empty_one_is_not() -> None:
    """Raise `ValueError` only when the query gave rows and none is valid."""
    assert parse_recipients([]) == []

    with pytest.raises(ValueError, match="No valid destination"):
        parse_recipients(['{"externalId": "1"}'])


def test_the_repeats_are_dropped_by_phone_or_by_id() -> None:
    """Keep the first recipient of each phone, or of each id."""
    recipients = [
        Recipient(to="1", external_id="a"),
        Recipient(to="1", external_id="b"),
        Recipient(to="2", external_id="a"),
        Recipient(to="3"),
        Recipient(to="4"),
    ]

    assert [recipient.to for recipient in drop_repeats(recipients)] == ["1", "2", "3", "4"]
    assert [recipient.to for recipient in drop_repeats(recipients, by_id=True)] == ["1", "1", "3", "4"]


def test_the_rows_of_the_table_of_dispatches_have_the_phone_the_cpf_and_the_partition() -> None:
    """Write a row for each recipient of each batch."""
    batches = [SentBatch("C-2026-07-17-lote1", [Recipient(to="1", external_id="9", vars={"A": "ç"})])]

    rows = build_rows(HSM, batches, SENT_AT)

    assert rows == [
        {
            "id_hsm": 18,
            "dispatch_date": "2026-07-17 09:30:05",
            "campaignName": "C-2026-07-17-lote1",
            "costCenterId": 71,
            "telefone": "1",
            "cpf": "9",
            "vars": '{"A": "ç"}',
            "ano_particao": "2026",
            "mes_particao": "07",
            "data_particao": "2026-07-17",
        }
    ]


def test_the_notices_of_a_dispatch_say_what_was_sent_and_its_status() -> None:
    """Write the numbers of the dispatch, and the share of each status."""
    success = success_notice(HSM, "2026-07-17 09:30:05", 5, {"to": "1"}, test_mode=True)
    results = results_notice(HSM, "2026-07-17 09:30:05", 5, [("READ", 3, 60.0)], test_mode=False)

    assert "[TESTE]" in success
    assert "5 disparos" in success
    assert "3 lotes" in success
    assert '"to": "1"' in success
    assert "**READ**: 3 CPFs (60.0%)" in results
    assert "Nenhum resultado" in results_notice(HSM, "d", 0, [], test_mode=False)
