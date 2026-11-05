"""A client of the Bifrost AI gateway for Gemini, over the `google-genai` SDK."""

from collections.abc import Sequence
from dataclasses import dataclass
from json import JSONDecodeError, loads
from math import ceil, sqrt
from re import IGNORECASE
from re import sub as replace_pattern
from types import TracebackType
from typing import Self, final

from google import genai
from google.genai import errors, types
from httpx import HTTPError
from pydantic import JsonValue, TypeAdapter, ValidationError

from .constants import (
    BIFROST_ATTEMPTS,
    BIFROST_CALIBRATED_BATCH,
    BIFROST_CALIBRATED_TOKENS,
    BIFROST_FIRST_WAIT_SECONDS,
    BIFROST_GENAI_URL,
    BIFROST_MODEL,
    BIFROST_RETRY_STATUSES,
    BIFROST_THINKING,
    EMBEDDING_BATCH,
    EMBEDDING_MODEL,
)
from .settings import BifrostSettings

JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class BifrostError(RuntimeError):
    """The gateway failed: a network error, an error status, or a wrong answer."""


def scale_output_tokens(batch_size: int) -> int:
    """Return the output token limit of a batch.

    It grows with the batch and never goes below 8192.

    Without it, a batch bigger than the one that was measured risks an answer cut in the
    middle, which is a JSON that does not parse and a batch that fails.
    """
    return max(BIFROST_CALIBRATED_TOKENS, ceil(BIFROST_CALIBRATED_TOKENS * batch_size / BIFROST_CALIBRATED_BATCH))


def strip_code_fence(text: str) -> str:
    """Return the text without the json code fence that the model may add."""
    text = text.strip()

    return (
        replace_pattern(r"^```(\w+)?\s*|\s*```$", "", text, flags=IGNORECASE).strip()
        if text.startswith("```")
        else text
    )


def parse_json(text: str) -> JsonValue:
    """Return the JSON value in a model answer, without its code fence.

    Raise `ValueError` if the answer is not JSON.
    """
    try:
        return JSON.validate_python(loads(strip_code_fence(text), strict=False))
    except (JSONDecodeError, ValidationError) as error:
        raise ValueError(f"The answer is not JSON (raw: {text[:200]!r})") from error


def parse_json_list(text: str) -> list[dict[str, JsonValue]]:
    """Return the objects of a JSON list in a model answer, without its code fence.

    Anything in the list that is not an object is dropped. Raise `ValueError` if the
    answer is not a JSON list.
    """
    parsed = parse_json(text)

    if not isinstance(parsed, list):
        raise ValueError(f"The answer is not a JSON list (raw: {text[:200]!r})")

    return [item for item in parsed if isinstance(item, dict)]


def parse_json_object(text: str) -> dict[str, JsonValue]:
    """Return the JSON object in an answer of the model, without its code fence.

    Raise `ValueError` if the answer is not a JSON object.
    """
    parsed = parse_json(text)

    if not isinstance(parsed, dict):
        raise ValueError(f"The answer is not a JSON object (raw: {text[:200]!r})")

    return parsed


@dataclass(frozen=True)
class Answer:
    """What the model said, and the tokens that the question and answer cost.

    The gateway tells them when it can.
    """

    text: str
    prompt_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cache_tokens: int | None = None


@final
class Bifrost:
    """A gateway client for a model.

    The SDK asks again with a growing wait when a question fails with a status that
    `BIFROST_RETRY_STATUSES` lists.
    """

    def __init__(self, client: genai.Client, model: str) -> None:
        """Wrap an SDK client and the model that it asks."""
        self.client = client
        self.model = model

    @classmethod
    def connect(cls, model: str = BIFROST_MODEL, timeout: float = 120) -> Self:
        """Return a client with the key of the settings, to use with `async with`."""
        retry = types.HttpRetryOptions(
            attempts=BIFROST_ATTEMPTS,
            initial_delay=BIFROST_FIRST_WAIT_SECONDS,
            exp_base=2.0,
            http_status_codes=list(BIFROST_RETRY_STATUSES),
        )
        options = types.HttpOptions(
            base_url=BIFROST_GENAI_URL, api_version="v1beta", timeout=round(timeout * 1000), retry_options=retry
        )
        client = genai.Client(api_key=BifrostSettings().BIFROST_API_KEY.get_secret_value(), http_options=options)

        return cls(client, model)

    async def __aenter__(self) -> Self:
        """Return the client."""
        return self

    async def __aexit__(
        self, error_type: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None
    ) -> None:
        """Close the HTTP client."""
        await self.client.aio.aclose()

    async def answer(self, prompt: str, *, system: str = "Responda em PT-BR.", max_output_tokens: int = 512) -> Answer:
        """Ask the model and return its answer with the usage.

        The temperature is 0, so the same question gets the same answer as far as the
        model allows. The text has no thought of the model.

        Raise `BifrostError` after the last try.
        """
        config = types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=max_output_tokens,
            temperature=0.0,
            http_options=types.HttpOptions(extra_body=BIFROST_THINKING),
        )

        try:
            response = await self.client.aio.models.generate_content(model=self.model, contents=prompt, config=config)
        except (errors.APIError, HTTPError, ValueError) as error:
            raise BifrostError(f"The gateway failed: {error}") from error

        usage = response.usage_metadata

        return Answer(
            response.text or "",
            usage.prompt_token_count if usage else None,
            usage.candidates_token_count if usage else None,
            usage.total_token_count if usage else None,
            usage.cached_content_token_count if usage else None,
        )

    async def embed(
        self, texts: Sequence[str], *, model: str = EMBEDDING_MODEL, batch_size: int = EMBEDDING_BATCH
    ) -> list[list[float]]:
        """Return one unit vector for each text, in batches of `batch_size`.

        Raise `BifrostError` after the last try, for a missing embedding, or for a zero
        vector.
        """
        result: list[list[float]] = []

        for start in range(0, len(texts), batch_size):
            batch: list[types.ContentUnion] = [*texts[start : start + batch_size]]

            try:
                response = await self.client.aio.models.embed_content(model=model, contents=batch)
            except (errors.APIError, HTTPError, ValueError) as error:
                raise BifrostError(f"The embedding request failed: {error}") from error

            embeddings = response.embeddings or []

            if len(embeddings) != len(batch):
                raise BifrostError(
                    f"The gateway returned {len(embeddings)} embeddings, expected {len(batch)} embeddings"
                )

            for embedding in embeddings:
                values = embedding.values or []
                norm = sqrt(sum(value * value for value in values))

                if norm == 0:
                    raise BifrostError("The gateway returned a zero embedding vector")

                result.append([value / norm for value in values])

        return result

    async def ask(self, prompt: str, *, system: str = "Responda em PT-BR.", max_output_tokens: int = 512) -> str:
        """Ask the model and return the text of its answer."""
        return (await self.answer(prompt, system=system, max_output_tokens=max_output_tokens)).text
