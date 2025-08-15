# -*- coding: utf-8 -*-
import asyncio
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import to_partitions
from langchain_core.runnables import RunnableConfig
from langchain_google_cloud_sql_pg import PostgresEngine, PostgresLoader, PostgresSaver

from pipelines.rj_iplanrio__eai_history import env
from pipelines.rj_iplanrio__eai_history.message_formatter import to_gateway_format


class GoogleAgentEngineHistory:
    def __init__(self, checkpointer: PostgresSaver):
        self._checkpointer = checkpointer

    @classmethod
    async def create(cls) -> "GoogleAgentEngineHistory":
        """Factory method para criar uma instância com checkpointer inicializado"""
        engine = await PostgresEngine.afrom_instance(
            project_id=env.PROJECT_ID,
            region=env.LOCATION,
            instance=env.INSTANCE,
            database=env.DATABASE,
            user=env.DATABASE_USER,
            password=env.DATABASE_PASSWORD,
            engine_args={"pool_pre_ping": True, "pool_recycle": 300},
        )
        checkpointer = await PostgresSaver.create(engine=engine)
        log("Checkpointer inicializado")
        return cls(checkpointer)

    async def get_checkpointer(self) -> PostgresSaver:
        return self._checkpointer

    async def _get_single_user_history(
        self,
        user_id: str,
        session_timeout_seconds: Optional[int] = 3600,
        use_whatsapp_format: bool = True,
    ) -> tuple[str, list]:
        """Método auxiliar para processar histórico de um único usuário"""
        config = RunnableConfig(configurable={"thread_id": user_id})

        state = await self._checkpointer.aget(config=config)
        if not state:
            return user_id, []

        messages = state.get("channel_values", {}).get("messages", [])
        # logger.info(messages)

        letta_payload = to_gateway_format(
            messages=messages,
            thread_id=user_id,
            session_timeout_seconds=session_timeout_seconds,
            use_whatsapp_format=use_whatsapp_format,
        )

        return user_id, letta_payload.get("data", {}).get("messages", [])

    async def get_history_bulk(
        self,
        user_ids: List[str],
        session_timeout_seconds: Optional[int] = 3600,
        use_whatsapp_format: bool = True,
    ) -> Dict[str, list]:
        """Método otimizado com async concorrente para buscar histórico de múltiplos usuários"""
        tasks = [
            self._get_single_user_history(
                user_id=user_id,
                session_timeout_seconds=session_timeout_seconds,
                use_whatsapp_format=use_whatsapp_format,
            )
            for user_id in user_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        result = {}
        for item in results:
            if isinstance(item, Exception):
                log(msg=f"Erro ao processar histórico: {item}", level="error")
                continue
            if isinstance(item, tuple) and len(item) == 2:
                user_id, messages = item
                result[user_id] = messages

        return result

    async def get_history_bulk_from_last_update(
        self,
        last_update: str = "2025-07-25",
        session_timeout_seconds: Optional[int] = 3600,
        use_whatsapp_format: bool = True,
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
                thread_id
            FROM "public"."thread_ids"
            WHERE checkpoint_ts >='{last_update}'
        """

        engine = self._checkpointer._engine
        loader = await PostgresLoader.create(engine=engine, query=query)
        docs = await loader.aload()
        user_ids = [doc.page_content for doc in docs]

        if not user_ids:
            log(msg="No data to save")
            return None

        history_to_save = await self.get_history_bulk(
            user_ids=user_ids,
            session_timeout_seconds=session_timeout_seconds,
            use_whatsapp_format=use_whatsapp_format,
        )

        bq_payload = [
            {
                "last_update": last_update,
                "user_id": user_id,
                "messages": history_to_save[user_id],
            }
            for user_id in history_to_save.keys()
        ]

        dataframe = pd.DataFrame(data=bq_payload)
        save_path = Path(f"/tmp/data/{uuid4()}")
        to_partitions(data=dataframe, partition_columns=["user_id"], savepath=str(save_path))

        return str(save_path)
