import asyncio
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pandas as pd
from langchain_core.version import VERSION as langchain_version
from langchain_google_cloud_sql_pg import PostgresEngine, PostgresLoader, PostgresSaver
from langchain_google_cloud_sql_pg.version import __version__ as gc_sql_pg_version

# from langchain_core.runnables import RunnableConfig
from langgraph.version import __version__ as langraph_version
from prefect.logging import get_logger

from pipelines.rj_iplanrio__eai_history.settings import Settings
from pipelines.rj_iplanrio__eai_history.message_formatter import to_gateway_format

logger = get_logger("toolkit")


class GoogleAgentEngineHistory:
    def __init__(self, checkpointer: PostgresSaver, environment: str):
        self._checkpointer = checkpointer
        self._environment = environment

    @classmethod
    async def create(cls, environment: str) -> "GoogleAgentEngineHistory":
        """Factory method para criar uma instância com checkpointer inicializado"""
        settings = Settings()

        engine = await PostgresEngine.afrom_instance(
            project_id=settings.EAI_PROJECT_ID,
            region=settings.EAI_LOCATION,
            instance=settings.EAI_INSTANCE,
            database=settings.EAI_DATABASE,
            user=settings.EAI_DATABASE_USER,
            password=settings.EAI_DATABASE_PASSWORD.get_secret_value(),
            engine_args={"pool_pre_ping": True, "pool_recycle": 300},
        )
        checkpointer = await PostgresSaver.create(engine=engine)
        logger.info(f"Checkpointer inicializado para project_id: {environment} | {settings.EAI_PROJECT_ID}")
        logger.info(
            f"Version Control\nlanggraph: {langraph_version}\nlangchain-google-cloud-sql-pg: {gc_sql_pg_version}\nlangchain: {langchain_version}"
        )
        return cls(checkpointer=checkpointer, environment=environment)

    async def get_checkpointer(self) -> PostgresSaver:
        return self._checkpointer

    async def _get_single_user_history(
        self,
        user_id: str,
        checkpoint_id: str,
        last_update: str,
        blob_type: str,
        blob_b64: str,
        save_path: str,
        session_timeout_seconds: Optional[int] = 3600,
        use_whatsapp_format: bool = True,
    ):
        """Método auxiliar para processar histórico de um único usuário"""

        if user_id in ["", " ", "/"]:
            logger.warning(f"Invalid user_id: {user_id}")
            return

        # LangGraph v4: messages blob was fetched in the bulk query and base64-encoded.
        # Decode it here using the checkpointer's serde (handles msgpack and json).
        import base64
        blob_bytes = base64.b64decode(blob_b64)
        messages = self._checkpointer.serde.loads_typed((blob_type, blob_bytes))

        payload = to_gateway_format(
            messages=messages,
            thread_id=user_id,
            session_timeout_seconds=session_timeout_seconds,
            use_whatsapp_format=use_whatsapp_format,
        )
        messages = payload.get("data", {}).get("messages", [])
        bq_payload = [
            {
                "environment": self._environment,
                "last_update": str(last_update),
                "checkpoint_id": checkpoint_id,
                "user_id": user_id,
                "messages": json.dumps(messages, ensure_ascii=False, indent=2),
            }
        ]

        dataframe = pd.DataFrame(data=bq_payload)

        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        Path(save_path).mkdir(parents=True, exist_ok=True)
        dataframe.to_csv(Path(save_path) / f"{now}_{user_id}.csv", index=False)

    async def get_history_bulk_from_last_update(
        self,
        last_update: str = "1970-01-01T00:00:00+00:00",
        session_timeout_seconds: Optional[int] = 3600,
        use_whatsapp_format: bool = True,
        max_user_save_limit: int = 100,
        sql_limit: Optional[int] = None,
    ) -> Optional[str]:
        """
        Get history bulk for all users updated after last_update (ISO 8601 timestamp).
        """

        logger.info(f"last_update used as filter: '{last_update}'")

        engine = self._checkpointer._engine

        # LangGraph v4: channel_values in the checkpoint row is empty {}.
        # Messages live in checkpoint_blobs as (thread_id, channel='messages', version, type, blob).
        # We join checkpoints with checkpoint_blobs in one bulk query, returning the
        # blob as base64 so PostgresLoader (text-only) can carry it. Python decodes
        # each blob with the checkpointer's serde (handles both msgpack and json).
        query = f"""
        SELECT DISTINCT ON (c.thread_id)
            c.thread_id,
            c.checkpoint_id,
            c.checkpoint->>'ts'                                      AS ts,
            cb.type                                                   AS blob_type,
            encode(cb.blob, 'base64')                                AS blob_b64
        FROM "public"."checkpoints" c
        JOIN "public"."checkpoint_blobs" cb
          ON  cb.thread_id = c.thread_id
          AND cb.channel   = 'messages'
          AND cb.version   = c.checkpoint->'channel_versions'->>'messages'
        WHERE checkpoint_ts_immutable(c.checkpoint) > '{last_update}'::timestamptz
        ORDER BY c.thread_id, checkpoint_ts_immutable(c.checkpoint) DESC
        {f"LIMIT {sql_limit}" if sql_limit else ""}
        """

        loader = await PostgresLoader.create(engine=engine, query=query)
        docs = await loader.aload()
        users_infos = [
            {
                "user_id": doc.page_content,
                "checkpoint_id": doc.metadata.get("checkpoint_id", ""),
                "ts": doc.metadata.get("ts", ""),
                "blob_type": doc.metadata.get("blob_type", ""),
                "blob_b64": doc.metadata.get("blob_b64", ""),
            }
            for doc in docs
        ]
        if not users_infos:
            logger.info("No data to save")
            return None
        else:
            logger.info(f"Found {len(users_infos)} users to process")
            logger.info(
                f"First 3 users: {[d['user_id'] for d in users_infos[:3]]}"
            )

        save_path = str(Path(f"/tmp/data/{uuid4()}"))
        logger.info(f"Data will be saved to: {save_path}")
        batch_size = max_user_save_limit
        user_id_chunks = [
            users_infos[i : i + batch_size]
            for i in range(0, len(users_infos), batch_size)
        ]
        total_batches = len(user_id_chunks)
        all_results = []

        logger.info(
            f"Starting processing of {len(users_infos)} users in {total_batches} batches of up to {batch_size} users each."  # noqa
        )
        for batch_num, user_chunk in enumerate(user_id_chunks, 1):
            tasks_for_this_batch = [
                self._get_single_user_history(
                    user_id=user_info["user_id"],
                    checkpoint_id=user_info["checkpoint_id"],
                    last_update=user_info["ts"],
                    blob_type=user_info["blob_type"],
                    blob_b64=user_info["blob_b64"],
                    save_path=save_path,
                    session_timeout_seconds=session_timeout_seconds,
                    use_whatsapp_format=use_whatsapp_format,
                )
                for user_info in user_chunk
            ]

            results_of_batch = await asyncio.gather(
                *tasks_for_this_batch, return_exceptions=True
            )
            all_results.extend(results_of_batch)

            progress = 100 * (batch_num / total_batches)
            logger.info(f"Processed batch {batch_num} / {total_batches} - {progress}%")

        errors = [res for res in all_results if isinstance(res, Exception)]
        if errors:
            logger.warning(
                f"Finished processing with {len(errors)} errors out of {len(users_infos)} users. Logging details for the first 3 errors:",
            )
            logger.critical(f"First 3 exceptions encountered: {errors[:3]}")

            for i, err in enumerate(errors[:3], 1):
                # Formata a exceção completa (tipo, mensagem e traceback) em uma string
                tb_str = "".join(
                    traceback.format_exception(type(err), err, err.__traceback__)
                )

                # Loga a string formatada
                logger.critical(f"--- Error #{i} Details ---\n{tb_str}")
        else:
            logger.info("Finished processing all batches successfully.")
        logger.info(f"Data fetching and processing complete. Returning save path: {save_path}")
        return str(save_path)
