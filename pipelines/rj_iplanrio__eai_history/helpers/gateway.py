"""LangGraph messages as the messages of the gateway."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import md5
from json import JSONDecodeError, loads
from uuid import uuid4

from langchain_core.load.dump import dumpd
from langchain_core.messages import BaseMessage
from pydantic import JsonValue, TypeAdapter

from pipelines.rj_iplanrio__eai_history.helpers.whatsapp import markdown_to_whatsapp
from pipelines.rj_iplanrio__eai_history.types import Message

JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
SESSION_ID_LENGTH = 16


def new_uuid() -> str:
    """Return a new random id."""
    return str(uuid4())


def as_dict(value: JsonValue) -> dict[str, JsonValue]:
    """Return a value if it is an object, else an empty one."""
    return value if isinstance(value, dict) else {}


def as_list(value: JsonValue) -> list[JsonValue]:
    """Return a value if it is a list, else an empty one."""
    return value if isinstance(value, list) else []


def as_text(value: JsonValue, default: str = "") -> str:
    """Return a value if it is text, else the default."""
    return value if isinstance(value, str) else default


def as_count(value: JsonValue) -> int:
    """Return a value as a whole number, or 0 if it is not a number."""
    if isinstance(value, bool | int | float):
        return int(value)

    try:
        return int(value) if isinstance(value, str) else 0
    except ValueError:
        return 0


def parse_timestamp(text: str | None) -> datetime | None:
    """Return an ISO time, with `Z` as UTC, or nothing if it is empty or not a time."""
    if not text:
        return None

    try:
        return datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError:
        return None


def seconds_between(current: str | None, last: str | None) -> float | None:
    """Return the seconds from the last time to the current one, or nothing if one is missing or not a time."""
    now, before = parse_timestamp(current), parse_timestamp(last)

    return (now - before).total_seconds() if now and before else None


def make_session_id(timestamp: str | None, thread_id: str | None) -> str:
    """Return an id for the session that starts at a time in a thread: the same input gives the same id."""
    return md5(f"{timestamp}_{thread_id or 'unknown'}".encode(), usedforsecurity=False).hexdigest()[:SESSION_ID_LENGTH]


def serialize_message(message: BaseMessage) -> Message:
    """Return a LangChain message as JSON, with the usage of its response when it has one."""
    raw = as_dict(JSON.validate_python(dumpd(message)))
    usage = as_dict(JSON.validate_python(message.response_metadata)).get("usage_metadata")

    if usage is not None:
        raw["usage_metadata"] = usage

    return raw


def message_metadata(kwargs: Message) -> Message:
    """Return the model, the finish and the token counts of a message: empty for a human or a tool message."""
    if kwargs.get("type") in ("human", "tool"):
        return {"model_name": None, "finish_reason": None, "avg_logprobs": None, "usage_metadata": None}

    response = as_dict(kwargs.get("response_metadata"))
    usage = as_dict(response.get("usage_metadata"))

    return {
        "model_name": as_text(response.get("model_name")),
        "finish_reason": as_text(response.get("finish_reason")),
        "avg_logprobs": response.get("avg_logprobs"),
        "usage_metadata": {
            key: as_count(usage.get(key))
            for key in (
                "prompt_token_count",
                "candidates_token_count",
                "total_token_count",
                "thoughts_token_count",
                "cached_content_token_count",
            )
        },
    }


def read_ai_content(raw: JsonValue) -> tuple[str, str]:
    """Return the text and the thinking of the content of an AI message, which is text or a list of parts."""
    if isinstance(raw, str):
        return raw, ""

    text = ""
    thinking = ""

    for item in as_list(raw):
        if isinstance(item, str):
            text += item
        elif isinstance(item, dict):
            kind = item.get("type")
            text += as_text(item.get("text")) if kind == "text" else ""
            thinking += as_text(item.get("thinking")) if kind == "thinking" else ""

    return text, thinking


def parse_arguments(args: JsonValue) -> JsonValue:
    """Return the arguments of a tool call as JSON: text is parsed, and text that is not JSON stays as it is."""
    if not isinstance(args, str):
        return args

    try:
        return JSON.validate_python(loads(args))
    except JSONDecodeError, ValueError:
        return args


def parse_tool_return(content: JsonValue) -> JsonValue:
    """Return the result of a tool as JSON when it is text that starts as an object or a list."""
    if isinstance(content, str) and content.strip().startswith(("{", "[")):
        try:
            return JSON.validate_python(loads(content))
        except JSONDecodeError, ValueError:
            return content

    return content


@dataclass
class Formatter:
    """The state of the formatting of one thread: its session, its step and the names of its tool calls."""

    thread_id: str | None
    timeout: int | None
    whatsapp: bool
    new_id: Callable[[], str] = new_uuid
    session_id: str | None = None
    last_timestamp: str | None = None
    tool_names: dict[str, str] = field(default_factory=dict)
    step_id: str = ""
    messages: list[Message] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Start the first step."""
        self.step_id = f"step-{self.new_id()}"

    def update_session(self, timestamp: str | None, since_last: float | None) -> None:
        """Start a session at the first message, and after a pause longer than the timeout.

        Without a timeout there is no session. A message with a time becomes the last one.
        """
        if self.timeout is None:
            self.session_id = None
        elif self.session_id is None or (since_last is not None and since_last > self.timeout):
            self.session_id = make_session_id(timestamp, self.thread_id)

        if timestamp:
            self.last_timestamp = timestamp

    def build_base(self, kwargs: Message, timestamp: str | None, since_last: float | None) -> Message:
        """Return the fields that every message of the gateway has."""
        original = as_text(kwargs.get("id")).replace("run--", "")

        return {
            "id": original or f"message-{self.new_id()}",
            "date": timestamp,
            "session_id": self.session_id,
            "time_since_last_message": since_last,
            "name": None,
            "otid": self.new_id(),
            "sender_id": None,
            "step_id": self.step_id,
            "is_err": None,
            **message_metadata(kwargs),
        }

    def format_tool_call(self, call: Message, base: Message) -> Message:
        """Return a tool call of an AI message as a message."""
        call_id = as_text(call.get("id")) or self.new_id()

        return {
            **base,
            "id": call_id,
            "message_type": "tool_call_message",
            "tool_call": {
                "name": as_text(call.get("name"), "unknown"),
                "arguments": parse_arguments(call.get("args", {})),
                "tool_call_id": call_id,
            },
        }

    def format_ai(self, kwargs: Message, base: Message) -> list[Message]:
        """Return an AI message as its reasoning, its tool calls and its answer, as many messages as it has."""
        text, thinking = read_ai_content(kwargs.get("content", ""))
        calls = [as_dict(call) for call in as_list(kwargs.get("tool_calls", []))]
        usage = as_dict(as_dict(kwargs.get("response_metadata")).get("usage_metadata"))
        reasoning_tokens = as_count(as_dict(usage.get("output_token_details")).get("reasoning"))
        messages: list[Message] = []

        if thinking:
            messages.append({**base, "id": f"thinking-{base['id']}", **self.reasoning(thinking)})
        elif reasoning_tokens > 0:
            reason = "Processando..."

            if calls:
                reason = f"Processando chamada para ferramenta {as_text(calls[0].get('name'), 'unknown')}"
            elif text:
                reason = "Processando resposta para o usuário"

            messages.append({**base, "id": f"reasoning-{base['id']}", **self.reasoning(reason)})

        for call in calls:
            if call_id := as_text(call.get("id")):
                self.tool_names[call_id] = as_text(call.get("name"), "unknown")

        messages += [self.format_tool_call(call, base) for call in calls]

        if text:
            content = markdown_to_whatsapp(text) if self.whatsapp else text
            messages.append({**base, "message_type": "assistant_message", "content": content})

        return messages

    @staticmethod
    def reasoning(text: str) -> Message:
        """Return the fields of a reasoning message."""
        return {"message_type": "reasoning_message", "source": "reasoner_model", "reasoning": text, "signature": None}

    def format_tool(self, kwargs: Message, base: Message) -> Message:
        """Return the result of a tool as a message, and start a new step."""
        failed = kwargs.get("status") == "error"
        call_id = as_text(kwargs.get("tool_call_id"))
        result = parse_tool_return(kwargs.get("content", ""))
        self.step_id = f"step-{self.new_id()}"

        return {
            **base,
            "name": self.tool_names.get(call_id) or as_text(kwargs.get("name")) or "unknown_tool",
            "message_type": "tool_return_message",
            "is_err": failed,
            "tool_return": result,
            "status": "error" if failed else "success",
            "tool_call_id": call_id,
            "stdout": None,
            "stderr": result if failed else None,
        }

    def add_message(self, message: Message) -> None:
        """Format a serialized message, and keep the result in order."""
        kwargs = as_dict(message.get("kwargs"))
        timestamp = as_dict(kwargs.get("additional_kwargs")).get("timestamp")
        timestamp = timestamp if isinstance(timestamp, str) else None
        since_last = seconds_between(timestamp, self.last_timestamp) if timestamp else None
        self.update_session(timestamp, since_last)
        base = self.build_base(kwargs, timestamp, since_last)
        kind = kwargs.get("type")

        if kind == "human":
            self.messages.append({**base, "message_type": "user_message", "content": kwargs.get("content", "")})
        elif kind == "ai":
            self.messages += self.format_ai(kwargs, base)
        elif kind == "tool":
            self.messages.append(self.format_tool(kwargs, base))

    def build_usage(self, serialized: Sequence[Message]) -> Message:
        """Return the usage of the thread: the tokens of every message, the steps and the models."""
        prompt = output = total = thoughts = 0
        models: set[str] = set()

        for message in serialized:
            response = as_dict(as_dict(message.get("kwargs")).get("response_metadata"))
            usage = as_dict(response.get("usage_metadata"))
            counts = [
                as_count(usage.get(key))
                for key in ("prompt_token_count", "candidates_token_count", "thoughts_token_count")
            ]
            prompt += counts[0]
            output += counts[1]
            thoughts += counts[2]
            total += as_count(usage.get("total_token_count")) or sum(counts)

            if name := as_text(response.get("model_name")):
                models.add(name)

        names: list[JsonValue] = [*sorted(models)]

        return {
            "message_type": "usage_statistics",
            "completion_tokens": output + thoughts,
            "thoughts_tokens": thoughts,
            "prompt_tokens": prompt,
            "total_tokens": total,
            "step_count": len({as_text(message.get("step_id")) for message in self.messages if message.get("step_id")}),
            "steps_messages": None,
            "run_ids": None,
            "agent_id": self.thread_id,
            "processed_at": datetime.now(UTC).isoformat(),
            "status": "done",
            "model_names": names,
        }


def to_gateway_format(
    messages: Sequence[BaseMessage | Message],
    thread_id: str | None = None,
    timeout: int | None = None,
    *,
    whatsapp: bool = True,
) -> Message:
    """Return the messages of a LangGraph thread in the format of the gateway.

    The result has `status` and `data.messages`: a message for each user message, reasoning, tool call,
    tool result and answer, with the session and the step of each, then the usage of the thread.
    With a `timeout` in seconds, a pause longer than it starts a new session. With `whatsapp`, the
    answers go through `markdown_to_whatsapp`.
    """
    formatter = Formatter(thread_id, timeout, whatsapp)
    serialized = [serialize_message(message) if isinstance(message, BaseMessage) else message for message in messages]

    for message in serialized:
        formatter.add_message(message)

    formatter.messages.append(formatter.build_usage(serialized))

    return {"status": "completed", "data": {"messages": list(formatter.messages)}}
