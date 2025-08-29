# -*- coding: utf-8 -*-
# ruff: noqa

import asyncio
import shutil
import traceback
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import List, Set, Tuple, Union
from uuid import uuid4

import basedosdados as bd
from iplanrio.pipelines_utils.bd import _delete_prod_dataset
from iplanrio.pipelines_utils.gcs import (
    delete_blobs_list,
    list_blobs_with_prefix,
)
from iplanrio.pipelines_utils.logging import log, log_mod
from iplanrio.pipelines_utils.pandas import (
    batch_to_dataframe,
    build_query_new_columns,
    clean_dataframe,
    dataframe_to_csv,
    dataframe_to_parquet,
    dump_header_to_file,
    parse_date_columns,
    remove_columns_accents,
    to_partitions,
)
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from .utils_database import database_execute, database_get_db


def _process_single_query(
    # Parâmetros de Conexão
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    charset: str,
    # Parâmetros de Batch e Tabela
    query: str,
    batch_size: int,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    partition_columns: List[str],
    batch_data_type: str,
    biglake_table: bool,
    log_number_of_batches: int,
    # Estado e Informações
    cleared_partitions: Set[str],
    cleared_table: bool,
    log_prefix: str,
    only_staging_dataset: bool = False,
) -> Tuple[Set[str], bool, int, int]:
    # Keep track of cleared stuff
    prepath = f"data/{uuid4()}/"
    db_object = database_get_db(
        database_type=database_type,
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
    )

    database_execute(
        database=db_object,
        query=query,
    )

    # Get data columns
    columns = db_object.get_columns()
    log(f"{log_prefix}: Got columns: {columns}")

    new_query_cols = build_query_new_columns(table_columns=columns)
    log(f"{log_prefix}: New query columns without accents: {new_query_cols}")

    prepath = Path(prepath)

    if not partition_columns or partition_columns[0] == "":
        partition_column = None
    else:
        partition_column = partition_columns[0]

    if not partition_column:
        log(f"{log_prefix}: NO partition column specified! Writing unique files")
    else:
        log(f"{log_prefix}: Partition column: {partition_column} FOUND!! Write to partitioned files")

    # Now loop until we have no more data.
    batch = db_object.fetch_batch(batch_size)
    idx = 0
    batchs_len = 0
    while len(batch) > 0:
        prepath.mkdir(parents=True, exist_ok=True)
        # Log progress each 100 batches.
        log_mod(
            msg=f"{log_prefix}: Dumping batch {idx + 1} with size {len(batch)}",
            index=idx,
            mod=log_number_of_batches,
        )
        batchs_len += len(batch)

        # Dump batch to file.
        dataframe = batch_to_dataframe(batch=batch, columns=columns)
        old_columns = dataframe.columns.tolist()
        dataframe.columns = remove_columns_accents(dataframe)
        new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist(), strict=False))
        dataframe = clean_dataframe(dataframe)
        saved_files = []
        if partition_column:
            dataframe, date_partition_columns = parse_date_columns(dataframe, new_columns_dict[partition_column])
            partitions = date_partition_columns + [new_columns_dict[col] for col in partition_columns[1:]]
            saved_files = to_partitions(
                data=dataframe,
                partition_columns=partitions,
                savepath=prepath,
                data_type=batch_data_type,
                suffix=f"{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            )
        elif batch_data_type == "csv":
            fname = prepath / f"{uuid4()}.csv"
            dataframe_to_csv(dataframe, fname)
            saved_files = [fname]
        elif batch_data_type == "parquet":
            fname = prepath / f"{uuid4()}.parquet"
            dataframe_to_parquet(dataframe, fname)
            saved_files = [fname]
        else:
            raise ValueError(f"Unknown data type: {batch_data_type}")

        # Log progress each 100 batches.

        log_mod(
            msg=f"{log_prefix}: Batch generated {len(saved_files)} files. Will now upload.",
            index=idx,
            mod=log_number_of_batches,
        )

        # Upload files.
        tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
        table_staging = f"{tb.table_full_name['staging']}"
        st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
        storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
        storage_path_link = (
            f"https://console.cloud.google.com/storage/browser/{st.bucket_name}/staging/{dataset_id}/{table_id}"
        )
        dataset_is_public = tb.client["bigquery_prod"].project == "datario"
        # If we have a partition column
        if partition_column:
            # Extract the partition from the filenames
            partitions = []
            for saved_file in saved_files:
                # Remove the prepath and filename. This is the partition.
                partition = str(saved_file).replace(str(prepath), "")
                partition = partition.replace(saved_file.name, "")
                # Strip slashes from beginning and end.
                partition = partition.strip("/")
                # Add to list.
                partitions.append(partition)
            # Remove duplicates.
            partitions = list(set(partitions))
            log_mod(
                msg=f"{log_prefix}: Got partitions: {partitions}",
                index=idx,
                mod=log_number_of_batches,
            )
            # Loop through partitions and delete files from GCS.
            blobs_to_delete = []
            for partition in partitions:
                if partition not in cleared_partitions:
                    blobs = list_blobs_with_prefix(
                        bucket_name=st.bucket_name,
                        prefix=f"staging/{dataset_id}/{table_id}/{partition}",
                        mode="staging",
                    )
                    blobs_to_delete.extend(blobs)
                cleared_partitions.add(partition)
            if blobs_to_delete:
                delete_blobs_list(bucket_name=st.bucket_name, blobs=blobs_to_delete)
                log_mod(
                    msg=f"{log_prefix}: Deleted {len(blobs_to_delete)} blobs from GCS: {blobs_to_delete}",
                    index=idx,
                    mod=log_number_of_batches,
                )
        if dump_mode == "append":
            if tb.table_exists(mode="staging"):
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE APPEND: Table ALREADY EXISTS:"
                        + f"\n{table_staging}"
                        + f"\n{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
            else:
                # the header is needed to create a table when dosen't exist
                log_mod(
                    msg=f"{log_prefix}: MODE APPEND: Table DOESN'T EXISTS\nStart to CREATE HEADER file",
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    msg=f"{log_prefix}: MODE APPEND: Created HEADER file:\n{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                    set_biglake_connection_permissions=False,
                )

                log_mod(
                    msg=(
                        f"{log_prefix}: MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                        + f"{table_staging}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )

                if not cleared_table:
                    st.delete_table(
                        mode="staging",
                        bucket_name=st.bucket_name,
                        not_found_ok=True,
                    )
                    log_mod(
                        msg=(
                            f"{log_prefix}: MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                            + f"{storage_path}\n"
                            + f"{storage_path_link}"
                        ),
                        index=idx,
                        mod=log_number_of_batches,
                    )
                    cleared_table = True
        elif dump_mode == "overwrite":
            if tb.table_exists(mode="staging") and not cleared_table:
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
                st.delete_table(
                    mode="staging",
                    bucket_name=st.bucket_name,
                    not_found_ok=True,
                )
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
                # delete only staging table and let DBT overwrite the prod table
                tb.delete(mode="staging")
                log_mod(
                    msg=(f"{log_prefix}: MODE OVERWRITE: Sucessfully DELETED TABLE:\n" + f"{table_staging}\n"),
                    index=idx,
                    mod=log_number_of_batches,
                )

            if not cleared_table:
                # the header is needed to create a table when dosen't exist
                # in overwrite mode the header is always created
                st.delete_table(
                    mode="staging",
                    bucket_name=st.bucket_name,
                    not_found_ok=True,
                )
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )

                log_mod(
                    msg=f"{log_prefix}: MODE OVERWRITE: Table DOSEN'T EXISTS\nStart to CREATE HEADER file",
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    f"{log_prefix}: MODE OVERWRITE: Created HEADER file:\n{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                    set_biglake_connection_permissions=False,
                )

                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Sucessfully CREATED TABLE\n"
                        + f"{table_staging}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )

                st.delete_table(
                    mode="staging",
                    bucket_name=st.bucket_name,
                    not_found_ok=True,
                )
                log_mod(
                    msg=(
                        f"{log_prefix}: MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
                cleared_table = True

        if only_staging_dataset:
            _delete_prod_dataset(
                only_staging_dataset=only_staging_dataset,
                dataset_id=dataset_id,
            )

        log_mod(
            msg="STARTING UPLOAD TO GCS",
            index=idx,
            mod=log_number_of_batches,
        )
        if tb.table_exists(mode="staging"):
            # Upload them all at once
            tb.append(filepath=prepath, if_exists="replace")
            log_mod(
                msg=f"{log_prefix}: STEP UPLOAD: Sucessfully uploaded batch {idx + 1} file with size {len(batch)} to Storage",
                index=idx,
                mod=log_number_of_batches,
            )
            for saved_file in saved_files:
                # Delete the files
                saved_file.unlink()
        else:
            log_mod(
                msg=f"{log_prefix}: STEP UPLOAD: Table does not exist in STAGING, need to create first",
                index=idx,
                mod=log_number_of_batches,
            )
        # Get next batch.
        batch = db_object.fetch_batch(batch_size)
        idx += 1

        # delete batch data from prepath
        shutil.rmtree(prepath)
    log(msg=f"{log_prefix}: --- Batchs: {idx}, Rows: {batchs_len} ---")

    return cleared_partitions, cleared_table, idx, batchs_len


def dump_upload_batch(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    queries: List[dict],
    batch_size: int,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    charset: str = "NOT_SET",
    partition_columns: List[str] = [],
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    retry_dump_upload_attempts: int = 2,
    max_concurrency: int = 1,  # Novo parâmetro para definir o limite do semáforo
    only_staging_dataset: bool = False,
):
    """
    Ponto de entrada síncrono que, internamente, cria um loop de eventos asyncio
    para executar múltiplas queries concorrentemente com um semáforo.
    """
    bd_version = bd.__version__
    log(f"Using basedosdados@{bd_version}")

    # --- Início da lógica assíncrona interna ---
    retry_attempts = retry_dump_upload_attempts
    retry_delay_seconds = 30

    async def _run_query_with_retries(
        semaphore: asyncio.Semaphore,
        log_prefix: str,
        **kwargs,
    ) -> Union[Tuple[Set[str], bool, int, int], Exception]:
        """
        Wrapper que gerencia o semáforo e a lógica de retry para uma única query.
        """
        for attempt in range(retry_attempts):
            try:
                async with semaphore:
                    # O log de início da query agora usa o prefixo, tornando-o mais claro
                    log(f"{log_prefix}: Iniciando processamento.")
                    log(f"{log_prefix}: Tentativa {attempt + 1}/{retry_attempts}.")

                    # Adiciona o `log_prefix` aos argumentos que serão passados para a função trabalhadora
                    kwargs["log_prefix"] = log_prefix  # NOVO

                    func_to_run = partial(_process_single_query, **kwargs)
                    result = await run_sync_in_worker_thread(func_to_run)

                    log(f"{log_prefix}: Processamento concluído com sucesso na tentativa {attempt + 1}.")
                    return result
            except Exception as e:
                log(f"{log_prefix}: Falha na tentativa {attempt + 1}. Erro: {e}")
                if attempt == retry_attempts - 1:
                    log(f"{log_prefix}: Todas as {retry_attempts} tentativas falharam. Registrando o erro final.")
                    return e
                await asyncio.sleep(retry_delay_seconds)

        return RuntimeError(f"{log_prefix}: A lógica de retry terminou inesperadamente.")

    async def _main_async_runner():
        """A corrotina principal que orquestra a execução concorrente."""
        semaphore = asyncio.Semaphore(max_concurrency)
        log(f"Controle de concorrência ativado. Máximo de {max_concurrency} tarefas simultâneas.")

        tasks_to_run = []
        initial_cleared_partitions = set()
        initial_cleared_table = False
        total_queries = len(queries)

        for n_query, query_info in enumerate(queries):
            progress = round(100 * (n_query + 1) / total_queries, 2)
            log_prefix = f"[Query {n_query + 1}/{total_queries} ({progress}%) | Datas: {query_info.get('start_date')} a {query_info.get('end_date')}]"
            task_args = {
                "database_type": database_type,
                "hostname": hostname,
                "port": port,
                "user": user,
                "password": password,
                "database": database,
                "charset": charset,
                "query": query_info["query"],
                "batch_size": batch_size,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "dump_mode": dump_mode,
                "partition_columns": partition_columns,
                "batch_data_type": batch_data_type,
                "biglake_table": biglake_table,
                "log_number_of_batches": log_number_of_batches,
                "cleared_partitions": initial_cleared_partitions,
                "cleared_table": initial_cleared_table,
                "only_staging_dataset": only_staging_dataset,
            }
            # Cria a tarefa com o wrapper de retry
            task = _run_query_with_retries(semaphore, log_prefix=log_prefix, **task_args)
            tasks_to_run.append(task)

        log(f"Iniciando a execução de {len(tasks_to_run)} queries em paralelo...")
        # `asyncio.gather` com `return_exceptions=True` é uma alternativa, mas retornar a exceção
        # no nosso wrapper nos dá mais controle sobre a lógica de retry.
        return await asyncio.gather(*tasks_to_run)

    # Inicia o loop de eventos asyncio a partir do nosso contexto síncrono.
    results = asyncio.run(_main_async_runner())

    # --- Agregação de resultados e tratamento de erros ---
    total_idx = 0
    total_batchs_len = 0
    final_cleared_partitions = set()
    failed_queries = []

    # `zip` garante a associação correta entre a query original e seu resultado/erro.
    for query_info, result in zip(queries, results, strict=False):
        if isinstance(result, Exception):
            # Coleta as informações da query que falhou e a exceção.
            failed_queries.append(
                {
                    "start_date": query_info.get("start_date"),
                    "end_date": query_info.get("end_date"),
                    "error": str(result),
                    "traceback": traceback.format_exc(),
                }
            )
        else:
            # Processa o resultado de sucesso
            cleared_parts, _, idx, batchs_len = result
            total_idx += idx
            total_batchs_len += batchs_len
            final_cleared_partitions.update(cleared_parts)

    # --- Passo Final: Levantar erro se houver falhas ---
    if failed_queries:
        error_summary = "\n".join(
            [f"  - Datas: {fq['start_date']} a {fq['end_date']}\n  Erro: {fq['error']}" for fq in failed_queries]
        )
        error_message = f"{len(failed_queries)} de {len(queries)} queries falharam após {retry_attempts} tentativas.\nResumo das falhas:\n{error_summary}"
        # Levanta uma única exceção com todas as informações.
        raise RuntimeError(error_message)

    log(
        msg=f"SUCESSO: Todas as {len(queries)} queries foram executadas. Total de Batchs: {total_idx}, Rows: {total_batchs_len}"
    )
