"""A client of the Bifrost AI gateway, which serves Gemini."""

from asyncio import sleep
from dataclasses import dataclass
from json import JSONDecodeError, loads
from math import ceil
from re import IGNORECASE
from re import sub as replace_pattern
from types import TracebackType
from typing import Self, final

from httpx2 import AsyncClient, HTTPError
from pydantic import JsonValue, TypeAdapter, ValidationError

from .constants import (
    BIFROST_ATTEMPTS,
    BIFROST_BASE_URL,
    BIFROST_CALIBRATED_BATCH,
    BIFROST_CALIBRATED_TOKENS,
    BIFROST_FIRST_WAIT_SECONDS,
    BIFROST_MODEL,
)
from .records import dig
from .settings import BifrostSettings

JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class BifrostError(RuntimeError):
    """The gateway failed: a network error, an error status, an error in the body, or a body with no text."""


def scale_output_tokens(batch_size: int) -> int:
    """Return the limit of output tokens for a batch: it grows with the batch, and never goes under 8192.

    Without it, a batch bigger than the one that was measured risks an answer cut in the
    middle, which is a JSON that does not parse and a batch that fails.
    """
    return max(BIFROST_CALIBRATED_TOKENS, ceil(BIFROST_CALIBRATED_TOKENS * batch_size / BIFROST_CALIBRATED_BATCH))


def strip_code_fence(text: str) -> str:
    """Return the text without the ```` ```json ```` fence that the model sometimes adds to a JSON answer."""
    text = text.strip()

    return (
        replace_pattern(r"^```(\w+)?\s*|\s*```$", "", text, flags=IGNORECASE).strip()
        if text.startswith("```")
        else text
    )


def parse_json_list(text: str) -> list[dict[str, JsonValue]]:
    """Return the objects of a JSON list in an answer of the model, without its code fence.

    Anything in the list that is not an object is dropped. Raise `ValueError` if the
    answer is not a JSON list.
    """
    try:
        parsed = JSON.validate_python(loads(strip_code_fence(text), strict=False))
    except (JSONDecodeError, ValidationError) as error:
        raise ValueError(f"The answer is not JSON (raw: {text[:200]!r})") from error

    if not isinstance(parsed, list):
        raise ValueError(f"The answer is not a JSON list (raw: {text[:200]!r})")

    return [item for item in parsed if isinstance(item, dict)]


@dataclass(frozen=True)
class Answer:
    """What the model said, and the tokens that the question and the answer cost, when the gateway tells them."""

    text: str
    prompt_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


def parse_json_object(text: str) -> dict[str, JsonValue]:
    """Return the JSON object in an answer of the model, without its code fence.

    Raise `ValueError` if the answer is not a JSON object.
    """
    try:
        parsed = JSON.validate_python(loads(strip_code_fence(text), strict=False))
    except (JSONDecodeError, ValidationError) as error:
        raise ValueError(f"The answer is not JSON (raw: {text[:200]!r})") from error

    if not isinstance(parsed, dict):
        raise ValueError(f"The answer is not a JSON object (raw: {text[:200]!r})")

    return parsed


def read_count(value: JsonValue) -> int | None:
    """Return a token count of the gateway as a whole number, or nothing if it is not one."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def read_answer(body: JsonValue) -> Answer:
    """Return the answer of the gateway: the text of its parts that are not thoughts, and the usage.

    Raise `BifrostError` if the body is an error or has no part.
    """
    if error := dig(body, "error"):
        code, status, message = dig(error, "code"), dig(error, "status"), dig(error, "message")
        raise BifrostError(f"The gateway answered with the error {code} ({status}): {message}")

    parts = dig(body, "candidates")
    parts = dig(parts[0], "content", "parts") if isinstance(parts, list) and parts else None

    if not isinstance(parts, list):
        raise BifrostError("The answer of the gateway has no candidate with parts")

    text = "".join(
        str(dig(part, "text"))
        for part in parts
        if isinstance(part, dict) and not part.get("thought") and "text" in part
    )

    return Answer(
        text,
        read_count(dig(body, "usageMetadata", "promptTokenCount")),
        read_count(dig(body, "usageMetadata", "candidatesTokenCount")),
        read_count(dig(body, "usageMetadata", "totalTokenCount")),
    )


def read_text(body: JsonValue) -> str:
    """Return the text of an answer of the gateway.

    Raise `BifrostError` if the body is an error or has no part.
    """
    return read_answer(body).text


@final
class Bifrost:
    """A client of the gateway for a model, that asks again with a growing wait when a question fails."""

    def __init__(
        self,
        client: AsyncClient,
        model: str,
        wait: float = BIFROST_FIRST_WAIT_SECONDS,
        attempts: int = BIFROST_ATTEMPTS,
    ) -> None:
        """Wrap a client with the address and the key of the gateway, the model, the first wait and the tries."""
        self.client = client
        self.model = model
        self.wait = wait
        self.attempts = attempts

    @classmethod
    def connect(cls, model: str = BIFROST_MODEL, timeout: float = 120) -> Self:
        """Return a client with the key of the settings, to use with `async with`."""
        key = BifrostSettings().BIFROST_API_KEY.get_secret_value()

        return cls(AsyncClient(base_url=BIFROST_BASE_URL, headers={"x-goog-api-key": key}, timeout=timeout), model)

    async def __aenter__(self) -> Self:
        """Return the client."""
        return self

    async def __aexit__(
        self, error_type: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None
    ) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def send(self, payload: dict[str, JsonValue]) -> Answer:
        """Send one question and return the answer.

        Raise `BifrostError` for a network error, an error status, an error in the body,
        or no text.
        """
        try:
            response = await self.client.post(f"/genai/v1beta/models/{self.model}:generateContent", json=payload)
        except HTTPError as error:
            raise BifrostError(f"The request to the gateway failed: {error}") from error

        try:
            body = JSON.validate_json(response.content)
        except ValidationError as error:
            if response.is_error:
                raise BifrostError(f"The gateway answered {response.status_code} with no JSON") from error

            raise BifrostError(f"The gateway answered {response.status_code} with a body that is not JSON") from error

        if response.is_error and not dig(body, "error"):
            raise BifrostError(f"The gateway answered {response.status_code}")

        return read_answer(body)

    async def answer(
        self,
        prompt: str,
        *,
        system: str = "Responda em PT-BR.",
        max_output_tokens: int = 512,
        thinking_budget: int = 0,
    ) -> Answer:
        """Ask the model and return its answer with the usage, trying `attempts` times with a growing wait.

        The wait doubles between the tries.

        The temperature is 0, so the same question gets the same answer as far as the
        model allows.

        Raise `BifrostError` after the last try.
        """
        payload: dict[str, JsonValue] = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": max_output_tokens,
                "temperature": 0.0,
                "thinkingConfig": {"thinkingBudget": thinking_budget, "includeThoughts": True},
            },
        }

        for attempt in range(1, self.attempts):
            try:
                return await self.send(payload)
            except BifrostError:
                await sleep(self.wait * (1 << (attempt - 1)))

        return await self.send(payload)

    async def ask(self, prompt: str, *, system: str = "Responda em PT-BR.", max_output_tokens: int = 512) -> str:
        """Ask the model and return the text of its answer.

        See `answer` for the retries.
        """
        return (await self.answer(prompt, system=system, max_output_tokens=max_output_tokens)).text
