# -*- coding: utf-8 -*-
import asyncio
import json
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import to_partitions
from langchain_core.runnables import RunnableConfig
from langchain_google_cloud_sql_pg import PostgresEngine, PostgresLoader, PostgresSaver

from pipelines.rj_iplanrio__eai_history import env
from pipelines.rj_iplanrio__eai_history.message_formatter import to_gateway_format


class GoogleAgentEngineHistory:
    def __init__(self, checkpointer: PostgresSaver, environment: str):
        self._checkpointer = checkpointer
        self._environment = environment

    @classmethod
    async def create(cls, environment: str) -> "GoogleAgentEngineHistory":
        """Factory method para criar uma instância com checkpointer inicializado"""
        if environment == "staging":
            project_id = env.PROJECT_ID or ""
            region = env.LOCATION or ""
            instance = env.INSTANCE or ""
            database = env.DATABASE or ""
            user = env.DATABASE_USER or ""
            password = env.DATABASE_PASSWORD or ""
        elif environment == "prod":
            project_id = env.PROJECT_ID_PROD or ""
            region = env.LOCATION_PROD or ""
            instance = env.INSTANCE_PROD or ""
            database = env.DATABASE_PROD or ""
            user = env.DATABASE_USER_PROD or ""
            password = env.DATABASE_PASSWORD_PROD or ""
        else:
            raise (ValueError("enviromet must be prod or staging"))

        engine = await PostgresEngine.afrom_instance(
            project_id=project_id,
            region=region,
            instance=instance,
            database=database,
            user=user,
            password=password,
            engine_args={"pool_pre_ping": True, "pool_recycle": 300},
        )
        checkpointer = await PostgresSaver.create(engine=engine)
        log(f"Checkpointer inicializado para project_id: {environment} | {project_id}")
        return cls(checkpointer=checkpointer, environment=environment)

    async def get_checkpointer(self) -> PostgresSaver:
        return self._checkpointer

    async def _get_single_user_history(
        self,
        user_id: str,
        last_update: str,
        save_path: str,
        session_timeout_seconds: Optional[int] = 3600,
        use_whatsapp_format: bool = True,
    ):
        """Método auxiliar para processar histórico de um único usuário"""

        if user_id in ["", " ", "/"]:
            log(f"Invalid user_id: {user_id}", level="warning")
            return

        config = RunnableConfig(configurable={"thread_id": user_id})

        state = await self._checkpointer.aget(config=config)
        if not state:
            log(f"No state found for user_id: {user_id}", level="warning")
            return

        messages = state.get("channel_values", {}).get("messages", [])
        # logger.info(messages)

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
                "last_update": last_update,
                "user_id": user_id,
                "messages": json.dumps(messages, ensure_ascii=False, indent=2),
            }
        ]

        dataframe = pd.DataFrame(data=bq_payload)
        to_partitions(
            data=dataframe,
            partition_columns=["environment", "user_id"],
            savepath=str(save_path),
        )

    async def get_history_bulk_from_last_update(
        self,
        last_update: str = "2025-07-25",
        session_timeout_seconds: Optional[int] = 3600,
        use_whatsapp_format: bool = True,
        max_user_save_limit: int = 100,
    ) -> Optional[str]:
        """
        CREATE VIEW "public"."thread_ids" AS (
                    WITH tb AS (
                    SELECT
                        thread_id,
                        encode(checkpoint, 'hex') as checkpoint_hex
                    FROM "public"."checkpoints"
                    ),
                    extracted_hex AS (
                    SELECT
                        thread_id,
                        (regexp_matches(
                        checkpoint_hex,
                        '((3[0-9]){4}2d(3[0-9]){2}2d(3[0-9]){2}54(3[0-9]){2}3a(3[0-9]){2}3a(3[0-9]){2}2e(3[0-9])+(2b|2d)(3[0-9]){2}3a(3[0-9]){2})'
                        ))[1] AS timestamp_hex
                    FROM tb
                    ),
                    final_tb AS (
                    SELECT DISTINCT
                    thread_id,
                    (convert_from(decode(timestamp_hex, 'hex'), 'UTF8'))::timestamptz AS checkpoint_ts
                    FROM extracted_hex
                    WHERE timestamp_hex IS NOT NULL
                    )

                    SELECT DISTINCT ON (thread_id)
                    thread_id,
                    checkpoint_ts
                    FROM final_tb
                    ORDER BY thread_id, checkpoint_ts DESC
        );
        """

        query = f"""
            SELECT
                thread_id,
                checkpoint_ts::text
            FROM "public"."thread_ids"
            WHERE checkpoint_ts >='{last_update}'
        """

        engine = self._checkpointer._engine
        loader = await PostgresLoader.create(engine=engine, query=query)
        docs = await loader.aload()
        users_infos = [
            {
                "user_id": doc.page_content,
                "last_update": doc.metadata["checkpoint_ts"][:19].replace(" ", "T"),
            }
            for doc in docs
        ]
        if not users_infos:
            log(msg="No data to save")
            return None
        else:
            log(f"Found {len(users_infos)} users to process")

        save_path = str(Path(f"/tmp/data/{uuid4()}"))
        batch_size = max_user_save_limit
        user_id_chunks = [users_infos[i : i + batch_size] for i in range(0, len(users_infos), batch_size)]
        total_batches = len(user_id_chunks)
        all_results = []

        log(
            msg=f"Starting processing of {len(users_infos)} users in {total_batches} batches of up to {batch_size} users each."  # noqa
        )

        for batch_num, user_chunk in enumerate(user_id_chunks, 1):
            tasks_for_this_batch = [
                self._get_single_user_history(
                    user_id=user_info["user_id"],
                    last_update=user_info["last_update"],
                    save_path=save_path,
                    session_timeout_seconds=session_timeout_seconds,
                    use_whatsapp_format=use_whatsapp_format,
                )
                for user_info in user_chunk
            ]

            results_of_batch = await asyncio.gather(*tasks_for_this_batch, return_exceptions=True)
            all_results.extend(results_of_batch)

            progress = 100 * (batch_num / total_batches)
            log(f"Processed batch {batch_num} / {total_batches} - {progress}%")

        errors = [res for res in all_results if isinstance(res, Exception)]
        if errors:
            log(
                msg=f"Finished processing with {len(errors)} errors out of {len(users_infos)} users.",
                level="warning",
            )
        else:
            log("Finished processing all batches successfully.")

        return str(save_path)
