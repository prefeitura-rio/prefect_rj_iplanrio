"""The history of the agent, in the Cloud SQL of the checkpoints."""

from base64 import b64decode
from collections.abc import Sequence
from json import dumps
from typing import cast

from langchain_core.messages import BaseMessage
from langchain_google_cloud_sql_pg import PostgresEngine, PostgresLoader, PostgresSaver
from langgraph.checkpoint.serde.base import SerializerProtocol
from pydantic import JsonValue, TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__eai_history.constants import ENGINE_ARGS, INVALID_USER_IDS
from pipelines.rj_iplanrio__eai_history.helpers.gateway import as_dict, as_list, to_gateway_format
from pipelines.rj_iplanrio__eai_history.settings import Settings
from pipelines.rj_iplanrio__eai_history.types import Row, Thread

THREADS: TypeAdapter[list[Thread]] = TypeAdapter(list[Thread])


async def connect() -> tuple[PostgresEngine, PostgresSaver]:
    """Open the Cloud SQL of the settings and return its engine and its checkpointer."""
    settings = Settings()
    engine = await PostgresEngine.afrom_instance(
        project_id=settings.EAI_PROJECT_ID,
        region=settings.EAI_LOCATION,
        instance=settings.EAI_INSTANCE,
        database=settings.EAI_DATABASE,
        user=settings.EAI_DATABASE_USER,
        password=settings.EAI_DATABASE_PASSWORD.get_secret_value(),
        engine_args=ENGINE_ARGS,
    )

    return engine, await PostgresSaver.create(engine=engine)


async def load_threads(engine: PostgresEngine, sql: str) -> list[Thread]:
    """Run the query of the threads and return the latest checkpoint of each."""
    loader = await PostgresLoader.create(engine=engine, query=sql)
    documents = await loader.aload()

    return THREADS.validate_python(
        [
            {
                "user_id": document.page_content,
                "checkpoint_id": document.metadata.get("checkpoint_id", ""),
                "ts": document.metadata.get("ts", ""),
                "blob_type": document.metadata.get("blob_type", ""),
                "blob_b64": document.metadata.get("blob_b64", ""),
            }
            for document in documents
        ]
    )


def build_row(
    serde: SerializerProtocol, thread: Thread, environment: str, timeout: int | None, *, whatsapp: bool
) -> Row | None:
    """Return the row of a thread: the messages of its latest checkpoint in the format of the gateway.

    A thread with an invalid id gives nothing.
    """
    if thread["user_id"] in INVALID_USER_IDS:
        logger.warning("Invalid user id: %r", thread["user_id"])
        return None

    messages = cast("list[BaseMessage]", serde.loads_typed((thread["blob_type"], b64decode(thread["blob_b64"]))))
    payload = to_gateway_format(messages, thread["user_id"], timeout, whatsapp=whatsapp)
    formatted: list[JsonValue] = as_list(as_dict(payload["data"]).get("messages"))

    return {
        "environment": environment,
        "last_update": thread["ts"],
        "checkpoint_id": thread["checkpoint_id"],
        "user_id": thread["user_id"],
        "messages": dumps(formatted, ensure_ascii=False, indent=2),
    }


def build_rows(
    serde: SerializerProtocol, threads: Sequence[Thread], environment: str, timeout: int | None, *, whatsapp: bool
) -> list[Row]:
    """Return the rows of the threads. A thread that fails is logged and skipped."""
    rows: list[Row] = []
    failures: list[Exception] = []

    for thread in threads:
        try:
            row = build_row(serde, thread, environment, timeout, whatsapp=whatsapp)
        except Exception as error:
            failures.append(error)
            continue

        if row is not None:
            rows.append(row)

    for error in failures[:3]:
        logger.error("A thread failed: %s: %s", type(error).__name__, error)

    if failures:
        logger.warning("Threads that failed: %d of %d", len(failures), len(threads))

    return rows
