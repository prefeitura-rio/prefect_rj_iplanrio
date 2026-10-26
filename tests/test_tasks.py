from json import loads
from os import environ
from types import SimpleNamespace

import pytest
from deltalake import DeltaTable, write_deltalake
from httpx2 import HTTPStatusError
from pyarrow import table as arrow_table
from toolkit.dbt import DbtRun

from utils import tasks
from utils.constants import DISCORD_LIMIT, USER_AGENT
from utils.delta import Destination
from utils.tasks import (
    dump_arcgis_to_gcs,
    maintain_delta_table,
    notify_discord,
    request_json,
    run_bq_sql,
    run_dbt,
    write_records,
)


async def test_a_get_sends_the_params_and_the_headers_and_returns_the_json(fake_api) -> None:
    """Send what was asked, and give back the JSON of the response."""
    fake_api.replies.append((200, {"items": [1, 2]}))

    data = await request_json.fn(f"{fake_api.url}/list", params={"page": "2"}, headers={"x-token": "abc"})

    [seen] = fake_api.requests
    assert data == {"items": [1, 2]}
    assert (seen.method, seen.path, seen.query, seen.headers["x-token"]) == ("GET", "/list", {"page": ["2"]}, "abc")


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        pytest.param(None, USER_AGENT, id="the_shared_user_agent_by_default"),
        pytest.param({"User-Agent": "mine"}, "mine", id="the_user_agent_of_the_caller_wins"),
    ],
)
async def test_a_request_sends_a_browser_user_agent_unless_the_caller_sets_one(fake_api, headers, expected) -> None:
    """Send the shared User-Agent, which servers such as INMET do not reset."""
    await request_json.fn(f"{fake_api.url}/list", headers=headers)

    [seen] = fake_api.requests
    assert seen.headers["user-agent"] == expected


async def test_a_post_sends_its_body_as_json(fake_api) -> None:
    """Send the body with the method that was asked."""
    await request_json.fn(f"{fake_api.url}/send", method="POST", body={"name": "ana"})

    [seen] = fake_api.requests
    assert (seen.method, loads(seen.body)) == ("POST", {"name": "ana"})


async def test_a_client_error_fails_after_one_request(fake_api) -> None:
    """Raise at once on a 4xx: the same request would fail the same way."""
    fake_api.replies.append((404, {"error": "no"}))

    with pytest.raises(HTTPStatusError):
        await request_json.fn(f"{fake_api.url}/missing")

    assert len(fake_api.requests) == 1


async def test_records_are_written_as_one_json_object_for_each_line(tmp_path) -> None:
    """Write `records.json` with one object per line, ready for DuckDB."""
    folder = await write_records.fn([{"id": 1, "nome": "Ana"}, {"id": 2, "nome": "Bia"}], tmp_path)

    lines = (folder / "records.json").read_text().splitlines()
    assert [loads(line) for line in lines] == [{"id": 1, "nome": "Ana"}, {"id": 2, "nome": "Bia"}]


async def test_a_message_goes_to_the_webhook_as_the_content(fake_api) -> None:
    """Post `content`, cut to the limit that Discord accepts."""
    fake_api.replies.append((204, {}))

    await notify_discord.fn("x" * (DISCORD_LIMIT + 10), webhook=f"{fake_api.url}/hook")

    [seen] = fake_api.requests
    assert (seen.method, seen.path, len(loads(seen.body)["content"])) == ("POST", "/hook", DISCORD_LIMIT)


async def test_a_message_uses_the_webhook_of_the_settings_when_none_is_given(fake_api, monkeypatch) -> None:
    """Fall back to `DISCORD_WEBHOOK_URL`."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", f"{fake_api.url}/set")

    await notify_discord.fn("hello")

    assert [seen.path for seen in fake_api.requests] == ["/set"]


async def test_a_message_with_no_webhook_is_refused(monkeypatch) -> None:
    """Raise, so a missing setting fails the run instead of dropping the message."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "")

    with pytest.raises(ValueError, match="webhook"):
        await notify_discord.fn("hello")


async def test_a_refused_message_raises(fake_api) -> None:
    """Raise when Discord answers with an error, so a lost alert is not silent."""
    fake_api.replies.append((400, {}))

    with pytest.raises(HTTPStatusError):
        await notify_discord.fn("hello", webhook=f"{fake_api.url}/hook")


async def test_a_statement_runs_in_the_project_and_returns_the_changed_rows(bigquery) -> None:
    """Open BigQuery on the project, run the statement, and return its count."""
    changed = await run_bq_sql.fn("MERGE t USING s ON FALSE", project="p")

    assert changed == bigquery.changed
    assert bigquery.calls == [("connect", "p"), ("execute", "MERGE t USING s ON FALSE")]


@pytest.mark.parametrize(
    ("crs", "expected"),
    [
        pytest.param("EPSG:31983", "31983", id="a_crs_becomes_the_srid"),
        pytest.param(None, None, id="no_crs_no_srid"),
    ],
)
async def test_the_crs_of_a_layer_reaches_the_read_as_a_srid(monkeypatch, crs, expected) -> None:
    """Pass the EPSG number to the read, and nothing when there is no `crs`."""
    seen = []

    async def record(*args) -> None:
        """Keep the layer of the read."""
        seen.append(args[0].origin)

    monkeypatch.setattr(tasks, "load_to_delta", record)

    await dump_arcgis_to_gcs.fn({"dataset_id": "d", "table_id": "t"}, "https://x/layer", bucket="b", crs=crs)

    assert seen[0].srid == expected


async def test_maintenance_merges_the_small_files_and_keeps_the_rows(tmp_path, monkeypatch) -> None:
    """Report fewer files after the run, with the same rows and a sound layout."""
    uri = str(tmp_path / "table")
    monkeypatch.setattr(Destination, "uri", property(lambda _: uri))
    for number in range(3):
        rows = arrow_table({"id": [number], "batch": ["2024-03-01"]})
        write_deltalake(uri, rows, mode="append", partition_by=["batch"])

    result = await maintain_delta_table.fn({"dataset_id": "ds", "table_id": "tb"}, bucket="b")

    assert (result["table"], result["before"]["files"], result["after"]["files"]) == ("ds.tb", 3, 1)
    assert result["problems"] == []
    assert set(DeltaTable(uri).to_pyarrow_table().to_pydict()["id"]) == {0, 1, 2}


async def test_dbt_runs_deps_then_the_command_in_the_cloned_project(fake_runner, tmp_path) -> None:
    """Regression: the clone was ignored, so dbt ran in the working directory."""
    await run_dbt.fn(DbtRun(repository="https://example.invalid/q.git", select="vendas"))

    [runner] = fake_runner.instances
    assert runner.settings.project_dir == tmp_path / "queries"
    assert runner.invoked == [["deps"], ["run", "--target", "dev", "--select", "vendas"]]


@pytest.mark.parametrize("has_profile", [True, False])
async def test_regression_the_profile_of_the_repository_is_used_when_it_has_one(
    fake_runner, tmp_path, has_profile
) -> None:
    """Regression: dbt ignored the `profiles.yml` of the repository."""
    if has_profile:
        (tmp_path / "queries" / "profiles.yml").write_text("p: {}\n")

    await run_dbt.fn(DbtRun(repository="https://example.invalid/q.git"))

    [runner] = fake_runner.instances
    assert (runner.settings.profiles_dir == tmp_path / "queries") is has_profile
    assert "DBT_PROFILES_DIR" not in environ


async def test_a_node_that_fails_raises_with_its_name(fake_runner, dbt_node) -> None:
    """Fail the run, and name the nodes that failed. A warning does not fail it."""
    fake_runner.result = SimpleNamespace(
        success=False,
        result=SimpleNamespace(results=[dbt_node("a", "success"), dbt_node("b", "error"), dbt_node("c", "warn")]),
    )

    with pytest.raises(RuntimeError, match=r"dbt run failed: b$"):
        await run_dbt.fn(DbtRun(repository="https://example.invalid/q.git"))


async def test_the_report_goes_to_discord_with_the_nodes_that_went_wrong(
    fake_runner, fake_api, monkeypatch, dbt_node
) -> None:
    """Post the result, also for a failed run, when `report` and a webhook are set."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", f"{fake_api.url}/hook")
    fake_runner.result = SimpleNamespace(
        success=False, result=SimpleNamespace(results=[dbt_node("b", "error", "boom")])
    )

    with pytest.raises(RuntimeError, match="failed"):
        await run_dbt.fn(
            DbtRun(repository="https://example.invalid/queries-x.git", command="build", target="prod"), report=True
        )

    [seen] = fake_api.requests
    assert loads(seen.body)["content"].splitlines() == [
        ":x: **[queries-x]** `dbt build` finished with errors",
        "- Target: `prod`",
        ":red_circle: `b` error: boom",
    ]


async def test_no_report_is_posted_when_it_is_off(fake_runner, fake_api, monkeypatch) -> None:
    """Post nothing without `report`, even with a webhook."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", f"{fake_api.url}/hook")

    await run_dbt.fn(DbtRun(repository="https://example.invalid/q.git"))

    assert fake_runner.instances
    assert fake_api.requests == []
