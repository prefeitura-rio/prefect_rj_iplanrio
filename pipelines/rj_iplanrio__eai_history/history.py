# -*- coding: utf-8 -*-
import asyncio
import json
from pathlib import Path
from typing import Optional
from uuid import uuid4
import traceback

from datetime import datetime
import pandas as pd

from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import to_partitions

# from langchain_core.runnables import RunnableConfig
from langgraph.version import __version__ as langraph_version
from langchain_google_cloud_sql_pg import PostgresEngine, PostgresLoader, PostgresSaver
from langchain_google_cloud_sql_pg.version import __version__ as gc_sql_pg_version
from langchain_core.version import VERSION as langchain_version

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
        log(
            f"Version Control\nlanggraph: {langraph_version}\nlangchain-google-cloud-sql-pg: {gc_sql_pg_version}\nlangchain: {langchain_version}"
        )
        return cls(checkpointer=checkpointer, environment=environment)

    async def get_checkpointer(self) -> PostgresSaver:
        return self._checkpointer

    async def _get_single_user_history(
        self,
        user_id: str,
        checkpoint_id: str,
        checkpoint_bytes: bytes,
        checkpoint_type: str,
        save_path: str,
        session_timeout_seconds: Optional[int] = 3600,
        use_whatsapp_format: bool = True,
    ):
        """Método auxiliar para processar histórico de um único usuário"""

        if user_id in ["", " ", "/"]:
            log(f"Invalid user_id: {user_id}", level="warning")
            return

        # config = RunnableConfig(configurable={"thread_id": user_id})
        # state = await self._checkpointer.aget(config=config)

        # use checkpoint_bytes from the query instead of making multiple aget requests
        state = self._checkpointer.serde.loads_typed(
            (checkpoint_type, checkpoint_bytes)
        )
        if not state:
            log(f"No state found for user_id: {user_id}", level="warning")
            return

        messages = state.get("channel_values", {}).get("messages", [])
        last_update = state.get("ts", None)

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
        to_partitions(
            data=dataframe,
            partition_columns=["environment", "user_id"],
            savepath=str(save_path),
            suffix=now,
        )

    async def get_history_bulk_from_last_checkpoint_id(
        self,
        last_checkpoint_id: str = "0",
        session_timeout_seconds: Optional[int] = 3600,
        use_whatsapp_format: bool = True,
        max_user_save_limit: int = 100,
    ) -> Optional[str]:
        """
        Get history bulk from last update.
        """

        query = f"""
        WITH new_checkpoints AS (
            -- Passo 1: Busca SUPER RÁPIDA usando o índice para pegar todas as linhas novas.
            -- Esta parte continua igual.
            SELECT
                thread_id,
                type,
                checkpoint,
                checkpoint_id
            FROM "public"."checkpoints"
            WHERE checkpoint_id > '{last_checkpoint_id}'
        )

        -- Dentro do lote de novidades, seleciona APENAS o checkpoint mais recente
        -- de cada thread ANTES de qualquer processamento pesado.
        -- Esta operação é muito rápida.
        SELECT DISTINCT ON (thread_id)
            thread_id,
            type,
            checkpoint,
            checkpoint_id
        FROM new_checkpoints
        ORDER BY thread_id, checkpoint_id DESC
        """

        engine = self._checkpointer._engine
        loader = await PostgresLoader.create(engine=engine, query=query)
        docs = await loader.aload()
        users_infos = [
            {
                "user_id": doc.page_content,
                "checkpoint_id": doc.metadata["checkpoint_id"],
                "checkpoint_bytes": doc.metadata["checkpoint"],  # <<< EXTRAIA OS BYTES
                "checkpoint_type": doc.metadata["type"],  # <<< EXTRAIA O TYPE
            }
            for doc in docs
        ]
        if not users_infos:
            log(msg="No data to save")
            return None
        else:
            log(f"Found {len(users_infos)} users to process")
            user_info_log = [
                {
                    "user_id": doc["user_id"],
                    "checkpoint_id": doc["checkpoint_id"],
                    "checkpoint_type": doc["checkpoint_type"],
                }
                for doc in users_infos[:3]
            ]
            log(
                f"First 3 users: {json.dumps(user_info_log, ensure_ascii=False, indent=2)}"
            )

        save_path = str(Path(f"/tmp/data/{uuid4()}"))
        log(f"Data will be saved to: {save_path}")
        batch_size = max_user_save_limit
        user_id_chunks = [
            users_infos[i : i + batch_size]
            for i in range(0, len(users_infos), batch_size)
        ]
        total_batches = len(user_id_chunks)
        all_results = []

        log(
            msg=f"Starting processing of {len(users_infos)} users in {total_batches} batches of up to {batch_size} users each."  # noqa
        )
        for batch_num, user_chunk in enumerate(user_id_chunks, 1):
            tasks_for_this_batch = [
                self._get_single_user_history(
                    user_id=user_info["user_id"],
                    checkpoint_id=user_info["checkpoint_id"],
                    checkpoint_bytes=user_info["checkpoint_bytes"],
                    checkpoint_type=user_info["checkpoint_type"],
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
            log(f"Processed batch {batch_num} / {total_batches} - {progress}%")

        errors = [res for res in all_results if isinstance(res, Exception)]
        if errors:
            log(
                msg=f"Finished processing with {len(errors)} errors out of {len(users_infos)} users. Logging details for the first 3 errors:",
                level="warning",
            )
            log(f"First 3 exceptions encountered: {errors[:3]}", level="critical")

            for i, err in enumerate(errors[:3], 1):
                # Formata a exceção completa (tipo, mensagem e traceback) em uma string
                tb_str = "".join(
                    traceback.format_exception(type(err), err, err.__traceback__)
                )

                # Loga a string formatada
                log(f"--- Error #{i} Details ---\n{tb_str}", level="critical")
        else:
            log("Finished processing all batches successfully.")
        log(f"Data fetching and processing complete. Returning save path: {save_path}")
        return str(save_path)
