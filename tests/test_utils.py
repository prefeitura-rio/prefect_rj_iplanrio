from datetime import timedelta
from types import SimpleNamespace

import pytest
from httpx2 import ConnectError, ConnectTimeout, HTTPStatusError, Request, Response
from toolkit.dbt import DbtRun

from utils.constants import DISCORD_LIMIT
from utils.types import DbtNode, DbtOutcome
from utils.utils import capture_time, failure_message, is_transient, read_nodes, summarize_dbt


def status_error(status: int) -> HTTPStatusError:
    """Return the error that `raise_for_status` gives for a status code."""
    request = Request("GET", "https://x")
    response = Response(status, request=request)
    return HTTPStatusError("failed", request=request, response=response)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(status_error(500), True, id="a_server_error"),
        pytest.param(status_error(503), True, id="a_service_that_is_down"),
        pytest.param(status_error(429), True, id="too_many_requests"),
        pytest.param(status_error(404), False, id="a_missing_page_stays_missing"),
        pytest.param(status_error(401), False, id="a_wrong_token_stays_wrong"),
        pytest.param(ConnectTimeout("slow"), True, id="a_timeout"),
        pytest.param(ConnectError("down"), True, id="a_network_error"),
        pytest.param(ValueError("bug"), False, id="a_bug_is_not_a_transient_error"),
        pytest.param(None, False, id="no_error"),
    ],
)
def test_only_a_transient_error_is_sent_again(error, expected):
    """Send a request again after a timeout, a network error, a 429 or a 5xx."""
    assert is_transient(error) is expected


@pytest.mark.parametrize(
    ("url", "tail"),
    [
        pytest.param("https://p/runs/1", "\nhttps://p/runs/1", id="with_a_link"),
        pytest.param(None, "boom", id="without_a_link"),
    ],
)
def test_the_failure_message_names_the_flow_the_run_and_the_reason(url, tail):
    """Name the flow and the run, give the reason, and add the link if there is one."""
    message = failure_message("dump", "orders", "boom", url)

    assert message.startswith(":red_circle: **dump** `orders` failed: boom")
    assert message.endswith(tail)


def test_regression_a_long_reason_never_goes_over_the_limit_of_discord():
    """Regression: Discord refuses a message of more than 2000 characters."""
    message = failure_message("f", "r", "x" * 5000, "https://p")

    assert len(message) == DISCORD_LIMIT


def test_the_capture_time_is_the_scheduled_start_in_sao_paulo():
    """Give a time with the offset of São Paulo, which is not UTC."""
    assert capture_time().utcoffset() == timedelta(hours=-3)


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        pytest.param(
            DbtOutcome(success=True, nodes=[]),
            [
                ":white_check_mark: **[queries-x]** `dbt build` finished without errors",
                "- Target: `prod`",
                "- Select: `tag:daily`",
            ],
            id="a_clean_run_lists_no_node",
        ),
        pytest.param(
            DbtOutcome(
                success=False,
                nodes=[DbtNode(name="a", status="fail", message="m1"), DbtNode(name="b", status="warn", message="m2")],
            ),
            [
                ":x: **[queries-x]** `dbt build` finished with errors",
                "- Target: `prod`",
                "- Select: `tag:daily`",
                ":red_circle: `a` fail: m1",
                ":warning: `b` warn: m2",
            ],
            id="the_nodes_that_went_wrong_are_listed",
        ),
    ],
)
def test_the_dbt_message_names_the_repository_and_never_its_url(outcome, expected):
    """Show the name of the repository and the nodes, with no URL (it can hold a token)."""
    request = DbtRun(
        repository="https://TOKEN@github.com/org/queries-x.git", command="build", target="prod", select="tag:daily"
    )

    message = summarize_dbt(request, outcome)

    assert message.splitlines() == expected
    assert "TOKEN" not in message


def test_the_dbt_message_stays_within_the_limit_of_discord():
    """Cut a long list of nodes to what Discord accepts."""
    nodes = [DbtNode(name=f"n{number}", status="error", message="x" * 100) for number in range(100)]

    message = summarize_dbt(DbtRun(repository="https://x/y.git"), DbtOutcome(success=False, nodes=nodes))

    assert len(message) == DISCORD_LIMIT


def test_only_the_nodes_that_failed_or_warned_are_read_from_a_dbt_result(dbt_node):
    """Skip the nodes that passed, and take a node name or a plain unique id."""
    result = SimpleNamespace(
        result=SimpleNamespace(
            results=[
                dbt_node("ok", "success"),
                dbt_node("bad", "error", "boom"),
                dbt_node("late", "runtime error"),
                SimpleNamespace(status="fail", message=None, unique_id="operation.x.y"),
            ]
        )
    )

    assert read_nodes(result) == [
        DbtNode(name="bad", status="error", message="boom"),
        DbtNode(name="late", status="runtime error", message=""),
        DbtNode(name="operation.x.y", status="fail", message=""),
    ]


def test_a_dbt_result_with_no_nodes_has_none_to_read():
    """Give an empty list for a command such as `debug`."""
    assert read_nodes(SimpleNamespace(success=True)) == []
