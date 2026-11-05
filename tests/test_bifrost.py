from json import loads

import pytest
from google import genai
from google.genai import types
from httpx import AsyncClient, ConnectError, MockTransport, Request, Response
from pydantic import JsonValue

from utils.bifrost import (
    Bifrost,
    BifrostError,
    parse_json_list,
    parse_json_object,
    scale_output_tokens,
    strip_code_fence,
)
from utils.constants import BIFROST_RETRY_STATUSES, BIFROST_THINKING

TINY = 1e-9


def answer(*parts: dict[str, JsonValue], usage: dict[str, JsonValue] | None = None) -> JsonValue:
    """Build the body of a good answer with the parts."""
    return {"candidates": [{"content": {"parts": list(parts)}}], **({"usageMetadata": usage} if usage else {})}


def gateway(*replies: Response | Exception, attempts: int = 3) -> tuple[Bifrost, list[Request]]:
    """Build a client whose gateway gives the replies in order, and keeps requests."""
    seen: list[Request] = []
    queue = list(replies)

    def handler(request: Request) -> Response:
        seen.append(request)
        reply = queue.pop(0)

        if isinstance(reply, Exception):
            raise reply

        return reply

    retry = types.HttpRetryOptions(
        attempts=attempts,
        initial_delay=TINY,
        max_delay=TINY,
        exp_base=1.0,
        jitter=TINY,
        http_status_codes=list(BIFROST_RETRY_STATUSES),
    )
    options = types.HttpOptions(
        base_url="http://gateway/genai",
        api_version="v1beta",
        httpx_async_client=AsyncClient(transport=MockTransport(handler)),
        retry_options=retry,
    )

    return Bifrost(genai.Client(api_key="key", http_options=options), "vertex/m"), seen


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


async def test_a_question_sends_the_prompt_and_returns_the_text() -> None:
    """Send the instruction, the prompt, the limits and the thinking switch."""
    bifrost, seen = gateway(Response(200, json=answer({"text": "hi"})))

    assert await bifrost.ask("question", system="sys", max_output_tokens=99) == "hi"

    [request] = seen
    body = loads(request.content)
    assert request.url.path == "/genai/v1beta/models/vertex/m:generateContent"
    assert body["contents"][0]["parts"][0]["text"] == "question"
    assert body["systemInstruction"]["parts"][0]["text"] == "sys"
    assert body["generationConfig"]["maxOutputTokens"] == 99
    assert body["generationConfig"]["temperature"] == 0.0
    assert body["generationConfig"]["thinkingConfig"] == BIFROST_THINKING["generationConfig"]["thinkingConfig"]
    assert request.headers["x-goog-api-key"] == "key"


async def test_the_text_of_an_answer_skips_the_thoughts() -> None:
    """Join the parts that are not thoughts."""
    bifrost, _ = gateway(Response(200, json=answer({"text": "think", "thought": True}, {"text": "a"}, {"text": "b"})))

    assert await bifrost.ask("q") == "ab"


async def test_an_answer_with_no_text_is_an_empty_text() -> None:
    """Give an empty text, which the caller then refuses as not JSON, and asks again."""
    bifrost, _ = gateway(Response(200, json={"candidates": [{"content": {"parts": []}, "finishReason": "MAX_TOKENS"}]}))

    assert await bifrost.ask("q") == ""


async def test_a_failed_question_is_asked_again_until_it_works() -> None:
    """Retry after an error status and a network error."""
    bifrost, seen = gateway(
        Response(503, json={"error": {"code": 503, "message": "busy", "status": "UNAVAILABLE"}}),
        ConnectError("down"),
        Response(200, json=answer({"text": "ok"})),
    )

    assert await bifrost.ask("q") == "ok"
    assert len(seen) == 3


async def test_a_question_that_keeps_failing_raises_after_the_last_try() -> None:
    """Stop after the attempts and raise the last error."""
    error = {"error": {"code": 500, "message": "oops", "status": "INTERNAL"}}
    bifrost, seen = gateway(Response(500, json=error), Response(500, json=error), attempts=2)

    with pytest.raises(BifrostError, match="500"):
        await bifrost.ask("q")

    assert len(seen) == 2


async def test_a_refused_model_raises_at_once() -> None:
    """A 403 is not asked again: the key does not allow the model."""
    refusal = {"error": {"code": 403, "message": "Model 'm' is not allowed for virtual key", "status": ""}}
    bifrost, seen = gateway(Response(403, json=refusal))

    with pytest.raises(BifrostError, match="not allowed"):
        await bifrost.ask("q")

    assert len(seen) == 1


async def test_the_answer_carries_the_tokens_of_the_gateway() -> None:
    """Read the usage next to the text, and give nothing for a count that is missing."""
    usage: dict[str, JsonValue] = {"promptTokenCount": 10, "candidatesTokenCount": 5, "cachedContentTokenCount": 3}
    bifrost, _ = gateway(Response(200, json=answer({"text": "x"}, usage=usage)))

    result = await bifrost.answer("q")

    assert (result.text, result.prompt_tokens, result.output_tokens, result.total_tokens, result.cache_tokens) == (
        "x",
        10,
        5,
        None,
        3,
    )


async def test_embeddings_are_batched_and_normalized() -> None:
    """Send embedding batches and return one unit vector for each text."""
    bifrost, seen = gateway(
        Response(200, json={"embeddings": [{"values": [3.0, 4.0]}, {"values": [0.0, 2.0]}]}),
        Response(200, json={"embeddings": [{"values": [1.0, 0.0]}]}),
    )

    vectors = await bifrost.embed(["a", "b", "c"], model="embed", batch_size=2)

    assert vectors == [[0.6, 0.8], [0.0, 1.0], [1.0, 0.0]]
    assert [request.url.path for request in seen] == ["/genai/v1beta/models/embed:batchEmbedContents"] * 2
    assert [len(loads(request.content)["requests"]) for request in seen] == [2, 1]


async def test_a_model_with_a_provider_keeps_it_in_the_url() -> None:
    """Route by `vertex/text-embedding-005` in the URL."""
    bifrost, seen = gateway(Response(200, json={"embeddings": [{"values": [1.0, 0.0]}]}))

    await bifrost.embed(["a"], model="vertex/text-embedding-005")

    assert seen[0].url.path == "/genai/v1beta/models/vertex/text-embedding-005:batchEmbedContents"


async def test_a_failed_embedding_request_is_asked_again() -> None:
    """Retry a network error and an error status like a question."""
    bifrost, seen = gateway(
        ConnectError("down"),
        Response(503, json={"error": {"code": 503, "message": "busy", "status": "UNAVAILABLE"}}),
        Response(200, json={"embeddings": [{"values": [0.0, 3.0]}]}),
    )

    assert await bifrost.embed(["a"], model="embed") == [[0.0, 1.0]]
    assert len(seen) == 3


async def test_an_embedding_request_that_keeps_failing_raises() -> None:
    """Stop after the attempts and raise the gateway error."""
    error = {"error": {"code": 500, "message": "oops", "status": "INTERNAL"}}
    bifrost, seen = gateway(Response(500, json=error), Response(500, json=error), attempts=2)

    with pytest.raises(BifrostError, match="500"):
        await bifrost.embed(["a"], model="embed")

    assert len(seen) == 2


async def test_an_embedding_answer_with_the_wrong_size_is_refused() -> None:
    """Raise when the gateway omits an embedding."""
    bifrost, _ = gateway(Response(200, json={"embeddings": []}), attempts=1)

    with pytest.raises(BifrostError, match="0 embeddings, expected 1"):
        await bifrost.embed(["a"], model="embed")


async def test_a_zero_embedding_is_refused() -> None:
    """A vector of zeros has no direction, so it never becomes a unit vector."""
    bifrost, _ = gateway(Response(200, json={"embeddings": [{"values": [0.0, 0.0]}]}))

    with pytest.raises(BifrostError, match="zero"):
        await bifrost.embed(["a"], model="embed")


def test_a_json_object_answer_is_parsed_and_a_list_is_refused() -> None:
    """Strip the fence, and raise for what is not an object."""
    assert parse_json_object('```json\n{"a": "x\ny"}\n```') == {"a": "x\ny"}

    with pytest.raises(ValueError, match="object"):
        parse_json_object("[1]")

    with pytest.raises(ValueError, match="not JSON"):
        parse_json_object("nope")
