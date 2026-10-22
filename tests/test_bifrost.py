from json import loads

import pytest
from httpx2 import AsyncClient, ConnectError, MockTransport, Request, Response
from pydantic import JsonValue

from utils.bifrost import Bifrost, BifrostError, parse_json_list, read_text, scale_output_tokens, strip_code_fence


def answer(*parts: dict[str, JsonValue]) -> JsonValue:
    """Build the body of a good answer with the parts."""
    return {"candidates": [{"content": {"parts": list(parts)}}]}


def gateway(*replies: Response | Exception, attempts: int = 3) -> tuple[Bifrost, list[Request]]:
    """Build a client whose gateway gives the replies in order, with the requests that it saw."""
    seen: list[Request] = []
    queue = list(replies)

    def handler(request: Request) -> Response:
        seen.append(request)
        reply = queue.pop(0)

        if isinstance(reply, Exception):
            raise reply

        return reply

    client = AsyncClient(base_url="http://gateway", transport=MockTransport(handler))

    return Bifrost(client, "m", wait=0, attempts=attempts), seen


@pytest.mark.parametrize(
    ("batch", "expected"),
    [
        pytest.param(10, 8192, id="a_small_batch_keeps_the_floor"),
        pytest.param(50, 8192, id="the_calibrated_batch"),
        pytest.param(100, 16384, id="a_double_batch_doubles_the_limit"),
        pytest.param(75, 12288, id="a_batch_in_between"),
    ],
)
def test_the_output_limit_grows_with_the_batch(batch: int, expected: int) -> None:
    """Scale from the batch that was measured."""
    assert scale_output_tokens(batch) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param('[{"a": 1}]', '[{"a": 1}]', id="no_fence"),
        pytest.param('```json\n[{"a": 1}]\n```', '[{"a": 1}]', id="a_json_fence"),
        pytest.param("```\n[1]\n```", "[1]", id="a_fence_without_a_language"),
        pytest.param("  ```JSON\n[1]\n```  ", "[1]", id="a_fence_in_capitals_with_spaces"),
    ],
)
def test_a_code_fence_is_removed(text: str, expected: str) -> None:
    """Return the JSON inside the fence."""
    assert strip_code_fence(text) == expected


def test_a_json_list_keeps_its_objects() -> None:
    """Drop what is not an object, and allow a line break inside a string."""
    assert parse_json_list('```json\n[{"a": "x\ny"}, 1, "z"]\n```') == [{"a": "x\ny"}]


@pytest.mark.parametrize("text", ["not json", '{"a": 1}', ""])
def test_an_answer_that_is_not_a_json_list_is_refused(text: str) -> None:
    """Raise with a part of the answer."""
    with pytest.raises(ValueError, match="answer"):
        parse_json_list(text)


def test_the_text_of_an_answer_skips_the_thoughts() -> None:
    """Join the parts that are not thoughts."""
    assert read_text(answer({"text": "think", "thought": True}, {"text": "a"}, {"text": "b"})) == "ab"


@pytest.mark.parametrize(
    ("body", "message"),
    [
        pytest.param(
            {"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "slow down"}}, "429", id="an_error_body"
        ),
        pytest.param({"candidates": []}, "no candidate", id="no_candidate"),
        pytest.param({}, "no candidate", id="an_empty_body"),
    ],
)
def test_an_answer_with_an_error_or_no_parts_is_refused(body: JsonValue, message: str) -> None:
    """Raise `BifrostError`."""
    with pytest.raises(BifrostError, match=message):
        read_text(body)


async def test_a_question_sends_the_prompt_and_returns_the_text() -> None:
    """Send the system instruction, the prompt and the limits."""
    bifrost, seen = gateway(Response(200, json=answer({"text": "hi"})))

    assert await bifrost.ask("question", system="sys", max_output_tokens=99) == "hi"

    [request] = seen
    body = loads(request.content)
    assert request.url.path == "/genai/v1beta/models/m:generateContent"
    assert body["contents"][0]["parts"][0]["text"] == "question"
    assert body["systemInstruction"]["parts"][0]["text"] == "sys"
    assert body["generationConfig"]["maxOutputTokens"] == 99


async def test_a_failed_question_is_asked_again_until_it_works() -> None:
    """Retry after an error status, a network error and an error body."""
    bifrost, seen = gateway(
        Response(503, json={"unrelated": 1}),
        ConnectError("down"),
        Response(200, json=answer({"text": "ok"})),
    )

    assert await bifrost.ask("q") == "ok"
    assert len(seen) == 3


async def test_a_question_that_keeps_failing_raises_after_the_last_try() -> None:
    """Stop after the attempts and raise the last error."""
    bifrost, seen = gateway(Response(500, content=b"oops"), Response(500, content=b"oops"), attempts=2)

    with pytest.raises(BifrostError, match="500"):
        await bifrost.ask("q")

    assert len(seen) == 2


async def test_a_good_status_with_a_body_that_is_not_json_is_an_error() -> None:
    """Raise, so the text is never taken from HTML."""
    bifrost, _ = gateway(Response(200, content=b"<html>"), attempts=1)

    with pytest.raises(BifrostError, match="not JSON"):
        await bifrost.ask("q")
