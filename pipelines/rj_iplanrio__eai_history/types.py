"""Types of rj_iplanrio__eai_history."""

from typing import Literal, NotRequired, TypedDict

from pydantic import JsonValue

type Row = dict[str, JsonValue]
type Environment = Literal["staging", "prod"]
type Message = dict[str, JsonValue]


class Options(TypedDict):
    """How a run reads the history and what it runs after the load.

    `last_update` replaces the cursor that the table gives, `session_timeout_seconds` ends a session
    after that many seconds without a message, and `sql_limit` caps the threads of a run.
    """

    last_update: NotRequired[str]
    session_timeout_seconds: NotRequired[int | None]
    use_whatsapp_format: NotRequired[bool]
    max_user_save_limit: NotRequired[int]
    sql_limit: NotRequired[int]
    dbt_select: NotRequired[str]


class Thread(TypedDict):
    """The latest checkpoint of a thread: its id, its time, and its messages as an encoded blob."""

    user_id: str
    checkpoint_id: str
    ts: str
    blob_type: str
    blob_b64: str
