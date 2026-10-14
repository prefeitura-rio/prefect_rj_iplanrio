from json import loads

import pytest
from httpx2 import HTTPStatusError
from psycopg.sql import Literal
from pydantic import SecretStr
from toolkit.settings import settings

from utils import tasks
from utils.constants import DISCORD_LIMIT
from utils.tasks import dump_arcgis_to_gcs, notify_discord, request_json, run_bq_sql, write_records


async def test_a_get_sends_the_params_and_the_headers_and_returns_the_json(fake_api):
    """Send what was asked, and give back the JSON of the response."""
    fake_api.replies.append((200, {"items": [1, 2]}))

    data = await request_json.fn(f"{fake_api.url}/list", params={"page": "2"}, headers={"x-token": "abc"})

    [seen] = fake_api.requests
    assert data == {"items": [1, 2]}
    assert (seen.method, seen.path, seen.query, seen.headers["x-token"]) == ("GET", "/list", {"page": ["2"]}, "abc")


async def test_a_post_sends_its_body_as_json(fake_api):
    """Send the body with the method that was asked."""
    await request_json.fn(f"{fake_api.url}/send", method="POST", body={"name": "ana"})

    [seen] = fake_api.requests
    assert (seen.method, loads(seen.body)) == ("POST", {"name": "ana"})


async def test_a_client_error_fails_after_one_request(fake_api):
    """Raise at once on a 4xx: the same request would fail the same way."""
    fake_api.replies.append((404, {"error": "no"}))

    with pytest.raises(HTTPStatusError):
        await request_json.fn(f"{fake_api.url}/missing")

    assert len(fake_api.requests) == 1


async def test_records_are_written_as_one_json_object_for_each_line(tmp_path):
    """Write `records.json` with one object per line, ready for DuckDB."""
    folder = await write_records.fn([{"id": 1, "nome": "Ana"}, {"id": 2, "nome": "Bia"}], tmp_path)

    lines = (folder / "records.json").read_text().splitlines()
    assert [loads(line) for line in lines] == [{"id": 1, "nome": "Ana"}, {"id": 2, "nome": "Bia"}]


async def test_a_message_goes_to_the_webhook_as_the_content(fake_api):
    """Post `content`, cut to the limit that Discord accepts."""
    fake_api.replies.append((204, {}))

    await notify_discord.fn("x" * (DISCORD_LIMIT + 10), webhook=f"{fake_api.url}/hook")

    [seen] = fake_api.requests
    assert (seen.method, seen.path, len(loads(seen.body)["content"])) == ("POST", "/hook", DISCORD_LIMIT)


async def test_a_message_uses_the_webhook_of_the_settings_when_none_is_given(fake_api, monkeypatch):
    """Fall back to `DISCORD_WEBHOOK_URL`."""
    monkeypatch.setattr(settings, "DISCORD_WEBHOOK_URL", SecretStr(f"{fake_api.url}/set"))

    await notify_discord.fn("hello")

    assert [seen.path for seen in fake_api.requests] == ["/set"]


async def test_a_message_with_no_webhook_is_refused(monkeypatch):
    """Raise, so a missing setting fails the run instead of dropping the message."""
    monkeypatch.setattr(settings, "DISCORD_WEBHOOK_URL", SecretStr(""))

    with pytest.raises(ValueError, match="webhook"):
        await notify_discord.fn("hello")


async def test_a_refused_message_raises(fake_api):
    """Raise when Discord answers with an error, so a lost alert is not silent."""
    fake_api.replies.append((400, {}))

    with pytest.raises(HTTPStatusError):
        await notify_discord.fn("hello", webhook=f"{fake_api.url}/hook")


async def test_a_statement_runs_in_the_project_and_returns_the_changed_rows(bigquery):
    """Open BigQuery on the project, run the statement, and return its count."""
    changed = await run_bq_sql.fn("MERGE t USING s ON FALSE", project="p")

    assert changed == bigquery.changed
    assert bigquery.calls == [("connect", "p"), ("execute", "MERGE t USING s ON FALSE")]


@pytest.mark.parametrize(
    ("crs", "expected"),
    [
        pytest.param("EPSG:31983", Literal("31983"), id="a_crs_becomes_the_srid"),
        pytest.param(None, None, id="no_crs_no_srid"),
    ],
)
async def test_the_crs_of_a_layer_reaches_the_template_as_a_srid(monkeypatch, crs, expected):
    """Pass the EPSG number to the read, and nothing when there is no `crs`."""
    seen = []

    async def record(*args):
        """Keep the values of the read."""
        seen.append(args[1])

    monkeypatch.setattr(tasks, "load_to_delta", record)

    await dump_arcgis_to_gcs.fn({"dataset_id": "d", "table_id": "t"}, "https://x/layer", bucket="b", crs=crs)

    assert seen[0].get("srid") == expected
