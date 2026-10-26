from datetime import UTC, datetime
from json import dumps

from pipelines.rj_sms__sisreg_disparo_lembretes.helpers import build_rows, drop_duplicate_phones, parse_recipients
from utils.hsm import Hsm, Recipient, SentBatch

SENT_AT = datetime(2026, 7, 17, 9, 30, 5, tzinfo=UTC)
HSM = Hsm(id_hsm=18, campaign_name="SISREG", cost_center_id=71, chunk_size=2)


def test_the_query_text_gives_the_recipient_with_the_phone_and_the_vars() -> None:
    """Read `celular_disparo` as the phone, and keep the vars."""
    texts = [dumps({"celular_disparo": "5521999", "vars": {"NOME": "Ana"}}), dumps({"to": "5521888"})]

    assert parse_recipients(texts) == [
        Recipient(to="5521999", vars={"NOME": "Ana"}),
        Recipient(to="5521888"),
    ]


def test_a_phone_gets_one_recipient_in_the_order_of_the_first() -> None:
    """Keep the first recipient of each phone."""
    recipients = [Recipient(to="1", vars={"a": 1}), Recipient(to="2"), Recipient(to="1", vars={"a": 2})]

    assert drop_duplicate_phones(recipients) == [Recipient(to="1", vars={"a": 1}), Recipient(to="2")]


def test_the_rows_have_one_line_for_each_recipient_of_each_batch_with_its_campaign() -> None:
    """Record the HSM, the batch campaign and the other columns of each row."""
    batches = [
        SentBatch("SISREG-2026-07-17-lote1", [Recipient(to="1", external_id="e", vars={"NOME": "Ação"})]),
        SentBatch("SISREG-2026-07-17-lote2", [Recipient(to="2")]),
    ]

    rows = build_rows(HSM, batches, SENT_AT)

    assert rows[0] == {
        "id_hsm": 18,
        "dispatch_date": "2026-07-17 09:30:05",
        "campaignName": "SISREG-2026-07-17-lote1",
        "costCenterId": 71,
        "to": "1",
        "externalId": "e",
        "vars": '{"NOME": "Ação"}',
        "ano_particao": "2026",
        "mes_particao": "07",
        "data_particao": "2026-07-17",
    }
    assert [row["campaignName"] for row in rows] == ["SISREG-2026-07-17-lote1", "SISREG-2026-07-17-lote2"]
    assert rows[1]["vars"] is None
    assert rows[1]["externalId"] is None
