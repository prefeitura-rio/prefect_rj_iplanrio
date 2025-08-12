# -*- coding: utf-8 -*-
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4
import asyncio
import traceback
from functools import partial

import basedosdados as bd
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread


from iplanrio.pipelines_utils.bd import get_storage_blobs
from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.database_sql import (
    Database,
    MySql,
    Oracle,
    Postgres,
    SqlServer,
)
from iplanrio.pipelines_utils.gcs import (
    delete_blobs_list,
    list_blobs_with_prefix,
    parse_blobs_to_partition_dict,
)
from iplanrio.pipelines_utils.io import (
    extract_last_partition_date,
    remove_tabs_from_query,
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
from iplanrio.pipelines_utils.prefect import rename_current_task_run_task


def parse_comma_separated_string_to_list(text: str) -> List[str]:
    """
    Parses a comma separated string to a list.

    Args:
        text: The text to parse.

    Returns:
        A list of strings.
    """
    if text is None or not text:
        return []
    # Remove extras.
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\t", "")
    while ",," in text:
        text = text.replace(",,", ",")
    while text.endswith(","):
        text = text[:-1]
    result = [x.strip() for x in text.split(",")]
    result = [item for item in result if item != "" and item is not None]
    return result


def database_get_db(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    charset: str = NOT_SET,
) -> Database:
    """
    Returns a database object.

    Args:
        database_type: The type of the database.
        hostname: The hostname of the database.
        port: The port of the database.
        user: The username of the database.
        password: The password of the database.
        database: The database name.

    Returns:
        A database object.
    """

    DATABASE_MAPPING: Dict[str, type[Database]] = {
        "mysql": MySql,
        "oracle": Oracle,
        "postgres": Postgres,
        "sql_server": SqlServer,
    }

    if database_type not in DATABASE_MAPPING:
        raise ValueError(f"Unknown database type: {database_type}")
    return DATABASE_MAPPING[database_type](
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset if charset != NOT_SET else None,
    )


def database_execute(
    database,
    query: str,
) -> None:
    """
    Executes a query on the database.

    Args:
        database: The database object.
        query: The query to execute.
    """
    log(f"Query parsed: {query}")
    query = remove_tabs_from_query(query)
    log(f"Executing query line: {query}")
    database.execute_query(query)


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

    database_execute(  # pylint: disable=invalid-name
        database=db_object,
        query=query,
    )

    # Get data columns
    columns = db_object.get_columns()
    log(f"Got columns: {columns}")

    new_query_cols = build_query_new_columns(table_columns=columns)
    log(f"New query columns without accents: {new_query_cols}")

    prepath = Path(prepath)

    if not partition_columns or partition_columns[0] == "":
        partition_column = None
    else:
        partition_column = partition_columns[0]

    if not partition_column:
        log("NO partition column specified! Writing unique files")
    else:
        log(f"Partition column: {partition_column} FOUND!! Write to partitioned files")

    # Now loop until we have no more data.
    batch = db_object.fetch_batch(batch_size)
    idx = 0
    batchs_len = 0
    while len(batch) > 0:
        prepath.mkdir(parents=True, exist_ok=True)
        # Log progress each 100 batches.
        log_mod(
            msg=f"Dumping batch {idx+1} with size {len(batch)}",
            index=idx,
            mod=log_number_of_batches,
        )
        batchs_len += len(batch)

        # Dump batch to file.
        dataframe = batch_to_dataframe(batch=batch, columns=columns)
        old_columns = dataframe.columns.tolist()
        dataframe.columns = remove_columns_accents(dataframe)
        new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
        dataframe = clean_dataframe(dataframe)
        saved_files = []
        if partition_column:
            dataframe, date_partition_columns = parse_date_columns(
                dataframe, new_columns_dict[partition_column]
            )
            partitions = date_partition_columns + [
                new_columns_dict[col] for col in partition_columns[1:]
            ]
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
            msg=f"Batch generated {len(saved_files)} files. Will now upload.",
            index=idx,
            mod=log_number_of_batches,
        )

        # Upload files.
        tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
        table_staging = f"{tb.table_full_name['staging']}"
        st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
        storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
        storage_path_link = (
            f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
            f"/staging/{dataset_id}/{table_id}"
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
                msg=f"Got partitions: {partitions}",
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
                    msg=f"Deleted {len(blobs_to_delete)} blobs from GCS: {blobs_to_delete}",  # noqa
                    index=idx,
                    mod=log_number_of_batches,
                )
        if dump_mode == "append":
            if tb.table_exists(mode="staging"):
                log_mod(
                    msg=(
                        "MODE APPEND: Table ALREADY EXISTS:"
                        + f"\n{table_staging}"
                        + f"\n{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
            else:
                # the header is needed to create a table when dosen't exist
                log_mod(
                    msg="MODE APPEND: Table DOESN'T EXISTS\nStart to CREATE HEADER file",  # noqa
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    msg="MODE APPEND: Created HEADER file:\n" f"{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                )

                log_mod(
                    msg=(
                        "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                        + f"{table_staging}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301

                if not cleared_table:
                    st.delete_table(
                        mode="staging",
                        bucket_name=st.bucket_name,
                        not_found_ok=True,
                    )
                    log_mod(
                        msg=(
                            "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"  # noqa
                            + f"{storage_path}\n"
                            + f"{storage_path_link}"
                        ),
                        index=idx,
                        mod=log_number_of_batches,
                    )  # pylint: disable=C0301
                    cleared_table = True
        elif dump_mode == "overwrite":
            if tb.table_exists(mode="staging") and not cleared_table:
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301
                st.delete_table(
                    mode="staging",
                    bucket_name=st.bucket_name,
                    not_found_ok=True,
                )
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301
                # delete only staging table and let DBT overwrite the prod table
                tb.delete(mode="staging")
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                        + f"{table_staging}\n"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301

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
                        "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301

                log_mod(
                    msg="MODE OVERWRITE: Table DOSEN'T EXISTS\nStart to CREATE HEADER file",  # noqa
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    "MODE OVERWRITE: Created HEADER file:\n" f"{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                )

                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
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
                        "MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"  # noqa
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301
                cleared_table = True

        log_mod(
            msg="STARTING UPLOAD TO GCS",
            index=idx,
            mod=log_number_of_batches,
        )
        if tb.table_exists(mode="staging"):
            # Upload them all at once
            tb.append(filepath=prepath, if_exists="replace")
            log_mod(
                msg=f"STEP UPLOAD: Sucessfully uploaded batch {idx +1} file with size {len(batch)} to Storage",
                index=idx,
                mod=log_number_of_batches,
            )
            for saved_file in saved_files:
                # Delete the files
                saved_file.unlink()
        else:
            # pylint: disable=C0301
            log_mod(
                msg="STEP UPLOAD: Table does not exist in STAGING, need to create first",  # noqa
                index=idx,
                mod=log_number_of_batches,
            )
        # Get next batch.
        batch = db_object.fetch_batch(batch_size)
        idx += 1

        # delete batch data from prepath
        shutil.rmtree(prepath)
    log(msg=f"--- Batchs: {idx}, Rows: {batchs_len} ---")

    return cleared_partitions, cleared_table, idx, batchs_len


@task(
    name="dump_upload_batch_mappable_task",
    description="Orchestrates dumping data. Runs concurrently for multiple queries using an internal asyncio loop.",
)
def dump_upload_batch_mappable_task(
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
    charset: str = NOT_SET,
    partition_columns: List[str] = [],
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    retry_dump_upload_attempts: int = 2,
    max_concurrency: int = 1,  # Novo parâmetro para definir o limite do semáforo
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
        query_info: Dict,
        n_query: int,
        total_queries: int,
        **kwargs,
    ) -> Union[Tuple[Set[str], bool, int, int], Exception]:
        """
        Wrapper que gerencia o semáforo e a lógica de retry para uma única query.
        Retorna o resultado da função ou a exceção em caso de falha persistente.
        """
        for attempt in range(retry_attempts):
            try:
                async with semaphore:
                    progress_percentage = round(100 * (n_query + 1) / total_queries, 2)
                    log(
                        f"Iniciando query {n_query + 1} de {total_queries} ({progress_percentage}%) | "
                    )

                    log(
                        f"Tentativa {attempt + 1}/{retry_attempts} para a query com datas {query_info['start_date']} a {query_info['end_date']}."
                    )
                    # Pega o loop de eventos atual
                    loop = asyncio.get_running_loop()

                    # Prepara a função TRABALHADORA (síncrona) para ser executada.
                    # Assumindo que a trabalhadora se chama `_process_single_query` (e agora é `def`)
                    func_to_run = partial(_process_single_query, **kwargs)

                    # Envia a função bloqueante para um thread separado e aguarda o resultado
                    result = await run_sync_in_worker_thread(func_to_run)

                    return result
            except Exception as e:
                log(
                    f"Falha na tentativa {attempt + 1} para a query {query_info['start_date']}-{query_info['end_date']}. Erro: {e}"
                )
                if attempt == retry_attempts - 1:
                    log(
                        f"Todas as {retry_attempts} tentativas falharam. Registrando o erro final."
                    )
                    return e  # Retorna a exceção em vez de levantá-la

                await asyncio.sleep(retry_delay_seconds)
        # Este ponto não deve ser alcançado, mas por segurança:
        return RuntimeError(
            "A lógica de retry terminou sem retornar um resultado ou exceção."
        )

    async def _main_async_runner():
        """A corrotina principal que orquestra a execução concorrente."""
        semaphore = asyncio.Semaphore(max_concurrency)
        log(
            f"Controle de concorrência ativado. Máximo de {max_concurrency} tarefas simultâneas."
        )

        tasks_to_run = []
        initial_cleared_partitions = set()
        initial_cleared_table = False
        total_queries = len(queries)

        for n_query, query_info in enumerate(queries):
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
            }
            # Cria a tarefa com o wrapper de retry
            # Passamos n_query e total_queries para o wrapper
            task = _run_query_with_retries(
                semaphore, query_info, n_query, total_queries, **task_args
            )
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
    for query_info, result in zip(queries, results):
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
            [
                f"  - Datas: {fq['start_date']} a {fq['end_date']}\n  Erro: {fq['error']}"
                for fq in failed_queries
            ]
        )
        error_message = f"{len(failed_queries)} de {len(queries)} queries falharam após {retry_attempts} tentativas.\nResumo das falhas:\n{error_summary}"
        # Levanta uma única exceção com todas as informações.
        raise RuntimeError(error_message)

    log(
        msg=f"SUCESSO: Todas as {len(queries)} queries foram executadas. Total de Batchs: {total_idx}, Rows: {total_batchs_len}"
    )


# @task(
#     name="dump_upload_batch_subimit_task",
#     description="Orchestrates dumping data. Runs sequentially for one query, or in parallel for multiple queries (forcing append).",
# )
# def dump_upload_batch_subimit_task(
#     database_type: str,
#     hostname: str,
#     port: int,
#     user: str,
#     password: str,
#     database: str,
#     queries: List[dict],
#     batch_size: int,
#     dataset_id: str,
#     table_id: str,
#     dump_mode: str,
#     charset: str = NOT_SET,
#     partition_columns: List[str] = [],
#     batch_data_type: str = "csv",
#     biglake_table: bool = True,
#     log_number_of_batches: int = 100,
#     retry_dump_upload_attempts: int = 2,
# ):
#     """
#     Ponto de entrada principal com passagem de variáveis explícita e logs detalhados.
#     """
#     bd_version = bd.__version__
#     log(msg=f"Usando basedosdados@{bd_version}")
#     queries_strings = [query["query"] for query in queries]
#     start_dates = [query["start_date"] for query in queries]
#     end_dates = [query["end_date"] for query in queries]

#     # Substitua o .map() e .wait() por um laço for
#     num_queries = len(queries_strings)
#     cleared_partitions = set()
#     cleared_table = False
#     total_idx = 0
#     total_batchs_len = 0
#     for i in range(num_queries):
#         log(
#             f"query {i+1} of {len(queries_strings)} |{ round(100 * (i+1) / len(queries_strings), 2)}"
#         )
#         # Chame a task trabalhadora diretamente. A execução vai parar aqui e esperar.
#         future = _process_single_query.submit(
#             # Parâmetros de Conexão
#             database_type=database_type,
#             hostname=hostname,
#             port=port,
#             user=user,
#             password=password,
#             database=database,
#             charset=charset,
#             # Parâmetros de Batch e Tabela
#             query=queries_strings[i],
#             batch_size=batch_size,
#             dataset_id=dataset_id,
#             table_id=table_id,
#             dump_mode=dump_mode,
#             partition_columns=partition_columns,
#             batch_data_type=batch_data_type,
#             biglake_table=biglake_table,
#             log_number_of_batches=log_number_of_batches,
#             # Estado e Informações
#             cleared_partitions=cleared_partitions,
#             cleared_table=cleared_table,
#         )
#         future.wait()
#         cleared_partitions, cleared_table, idx, batchs_len = future.result()

#         log(msg=f"--- Task {i+1} concluída. Batchs: {idx}, Rows: {batchs_len} ---")
#         total_idx += idx
#         total_batchs_len += batchs_len
#     log(
#         msg=f"SUCESSO: {num_queries} queries foram executadas sequencialmente. Total de Batchs: {total_idx}, Rows: {total_batchs_len}"
#     )


def format_partitioned_query(
    query: str,
    dataset_id: str,
    table_id: str,
    database_type: str,
    partition_columns: Optional[List[str]] = None,
    lower_bound_date: Optional[str] = None,
    date_format: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    wait: Optional[str] = None,  # pylint: disable=unused-argument
) -> List[dict]:
    """
    Formats a query for fetching partitioned data.
    """
    if not partition_columns or partition_columns[0] == "":
        log("NO partition column specified. Returning query as is")
        return [{"query": query, "start_date": None, "end_date": None}]

    partition_column = partition_columns[0]
    last_partition_date = get_last_partition_date(dataset_id, table_id, date_format)

    if last_partition_date is None:
        log("NO partition blob was found.")

    # Check if the table already exists in BigQuery.
    table = bd.Table(dataset_id, table_id)

    # If it doesn't, return the query as is, so we can fetch the whole table.
    if not table.table_exists(mode="staging"):
        log("NO tables was found.")

    if not break_query_frequency:
        return [
            build_single_partition_query(
                query=query,
                partition_column=partition_column,
                lower_bound_date=lower_bound_date,
                last_partition_date=last_partition_date,
                date_format=date_format,
                database_type=database_type,
            )
        ]

    return build_chunked_queries(
        query=query,
        partition_column=partition_column,
        date_format=date_format,
        database_type=database_type,
        break_query_start=break_query_start,
        break_query_end=break_query_end,
        break_query_frequency=break_query_frequency,
        lower_bound_date=lower_bound_date,
        last_partition_date=last_partition_date,
    )


def get_last_partition_date(
    dataset_id: str, table_id: str, date_format: Optional[str]
) -> Optional[str]:
    blobs = get_storage_blobs(dataset_id=dataset_id, table_id=table_id)
    storage_partitions_dict = parse_blobs_to_partition_dict(blobs=blobs)
    return extract_last_partition_date(
        partitions_dict=storage_partitions_dict, date_format=date_format
    )


def get_last_date(
    lower_bound_date: Optional[str], date_format: str, last_partition_date: str
) -> str:
    now: datetime = datetime.now()
    if lower_bound_date == "current_year":
        return now.replace(month=1, day=1).strftime(date_format)
    elif lower_bound_date == "current_month":
        return now.replace(day=1).strftime(date_format)
    elif lower_bound_date == "current_day":
        return now.strftime(date_format)
    elif lower_bound_date:
        if last_partition_date:
            return min(
                datetime.strptime(lower_bound_date, date_format),
                datetime.strptime(last_partition_date, date_format),
            ).strftime(date_format)
        else:
            return datetime.strptime(lower_bound_date, date_format).strftime(
                date_format
            )
    return datetime.strptime(last_partition_date, date_format).strftime(date_format)


def build_single_partition_query(
    query: str,
    partition_column: str,
    lower_bound_date: Optional[str],
    last_partition_date: str,
    date_format: str,
    database_type: str,
) -> dict:
    last_date = get_last_date(
        lower_bound_date=lower_bound_date,
        date_format=date_format,
        last_partition_date=last_partition_date,
    )
    aux_name = f"a{uuid4().hex}"[:8]

    log(
        f"Partitioned DETECTED: {partition_column}, returning a NEW QUERY with partitioned columns and filters"  # noqa
    )

    if database_type == "oracle":
        oracle_date_format = "YYYY-MM-DD" if date_format == "%Y-%m-%d" else date_format
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where {partition_column} >= TO_DATE('{last_date}', '{oracle_date_format}')
        """

    query = f"""
    with {aux_name} as ({query})
    select * from {aux_name}
    where CONVERT(DATE, {partition_column}) >= '{last_date}'
    """

    return {
        "query": query,
        "start_date": last_date,
        "end_date": last_date,
    }


def build_chunked_queries(
    query: str,
    partition_column: str,
    date_format: str,
    database_type: str,
    break_query_start: Optional[str],
    break_query_end: Optional[str],
    break_query_frequency: Optional[str],
    lower_bound_date: Optional[str],
    last_partition_date: str,
) -> List[dict]:
    start_date_str = get_last_date(
        lower_bound_date=break_query_start,
        date_format=date_format,
        last_partition_date=None,
    )
    end_date_str = get_last_date(
        lower_bound_date=break_query_end,
        date_format=date_format,
        last_partition_date=None,
    )
    end_date = datetime.strptime(end_date_str, date_format)

    if break_query_end == "current_month":
        end_date = get_last_day_of_month(date=end_date)
        end_date_str = end_date.strftime(date_format)
    elif break_query_end == "current_year":
        end_date = get_last_day_of_year(year=end_date.year)
        end_date_str = end_date.strftime(date_format)

    log("Breaking query into multiple chunks based on frequency")
    log(f"    break_query_frequency: {break_query_frequency}")
    log(f"    break_query_start: {start_date_str}")
    log(f"    break_query_end: {end_date_str}")

    current_start = datetime.strptime(start_date_str, date_format)
    end_date = datetime.strptime(end_date_str, date_format)
    queries = []

    while current_start <= end_date:
        current_end = calculate_end_date(
            current_start=current_start,
            end_date=end_date,
            break_query_frequency=break_query_frequency,
        )
        queries.append(
            build_chunk_query(
                query=query,
                partition_column=partition_column,
                date_format=date_format,
                database_type=database_type,
                current_start=current_start,
                current_end=current_end,
            )
        )
        current_start = get_next_start_date(
            current_start=current_start, break_query_frequency=break_query_frequency
        )

    log(f"Total queries created: {len(queries)}")
    return queries


def calculate_end_date(
    current_start: datetime, end_date: datetime, break_query_frequency: Optional[str]
) -> datetime:
    if break_query_frequency.lower() == "month":
        return min(get_last_day_of_month(date=current_start), end_date)
    elif break_query_frequency.lower() == "year":
        return min(get_last_day_of_year(year=current_start.year), end_date)
    elif break_query_frequency.lower() == "day":
        return min(current_start, end_date)
    elif break_query_frequency.lower() == "week":
        return min(current_start + timedelta(days=6), end_date)
    elif break_query_frequency.lower() == "bimester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=2)),
            end_date,
        )
    elif break_query_frequency.lower() == "trimester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=3)),
            end_date,
        )
    elif break_query_frequency.lower() == "quadrimester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=4)),
            end_date,
        )
    elif break_query_frequency.lower() == "semester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=6)),
            end_date,
        )
    else:
        raise ValueError(
            f"Unsupported break_query_frequency: {break_query_frequency}. Use one of the following: year, month, day, week, bimester, trimester, quadrimester and semester"  # noqa
        )


def build_chunk_query(
    query: str,
    partition_column: str,
    date_format: str,
    database_type: str,
    current_start: datetime,
    current_end: datetime,
) -> dict:
    aux_name = f"a{uuid4().hex}"[:8]

    if database_type == "oracle":
        oracle_date_format: str = (
            "YYYY-MM-DD" if date_format == "%Y-%m-%d" else date_format
        )
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where {partition_column} >= TO_DATE('{current_start.strftime(date_format)}', '{oracle_date_format}')
            and {partition_column} <= TO_DATE('{current_end.strftime(date_format)}', '{oracle_date_format}')
        """

    query = f"""
    with {aux_name} as ({query})
    select * from {aux_name}
    where CONVERT(DATE, {partition_column}) >= '{current_start.strftime(date_format)}'
        and CONVERT(DATE, {partition_column}) <= '{current_end .strftime(date_format)}'
    """

    return {
        "query": query,
        "start_date": current_start.strftime(date_format),
        "end_date": current_end.strftime(date_format),
    }


def get_next_start_date(
    current_start: datetime, break_query_frequency: Optional[str]
) -> datetime:
    if break_query_frequency.lower() == "month":
        return add_months(start_date=current_start, months=1)
    elif break_query_frequency.lower() == "year":
        return datetime(current_start.year + 1, 1, 1)
    elif break_query_frequency.lower() == "day":
        return current_start + timedelta(days=1)
    elif break_query_frequency.lower() == "week":
        return current_start + timedelta(days=7)
    elif break_query_frequency.lower() in [
        "bimester",
        "trimester",
        "quadrimester",
        "semester",
    ]:
        months_to_add = {
            "bimester": 2,
            "trimester": 3,
            "quadrimester": 4,
            "semester": 6,
        }
        return add_months(
            start_date=current_start,
            months=months_to_add[break_query_frequency.lower()],
        )
    return current_start


def get_last_day_of_month(date: datetime) -> datetime:
    next_month = date.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def get_last_day_of_year(year: int) -> datetime:
    return datetime(year, 12, 31)


def add_months(start_date: datetime, months: int) -> datetime:
    new_month = start_date.month + months
    year_increment = (new_month - 1) // 12
    new_month = (new_month - 1) % 12 + 1
    new_year = start_date.year + year_increment
    return datetime(new_year, new_month, start_date.day)


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
    charset: str = NOT_SET,
    partition_columns: List[str] = [],
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    retry_dump_upload_attempts: int = 2,
):
    """
    This task will dump and upload batches of data, sequentially.
    """
    # Log BD version
    bd_version = bd.__version__
    log(f"Using basedosdados@{bd_version}")

    # Keep track of cleared stuff
    prepath = f"data/{uuid4()}/"
    cleared_partitions = set()
    cleared_table = False

    wait_seconds = 30
    total_idx = 0
    total_batchs_len = 0
    queries_strings = [query["query"] for query in queries]
    for n_query, query in enumerate(queries_strings):
        attempts = retry_dump_upload_attempts
        while attempts >= 0:
            try:
                log(f"Attempt: { retry_dump_upload_attempts - attempts}")
                log(
                    f"query {n_query+1} of {len(queries_strings)} |{ round(100 * (n_query+1) / len(queries_strings), 2)}"
                )

                db_object = database_get_db(
                    database_type=database_type,
                    hostname=hostname,
                    port=port,
                    user=user,
                    password=password,
                    database=database,
                    charset=charset,
                )

                database_execute(  # pylint: disable=invalid-name
                    database=db_object,
                    query=query,
                )

                # Get data columns
                columns = db_object.get_columns()
                log(f"Got columns: {columns}")

                new_query_cols = build_query_new_columns(table_columns=columns)
                log(f"New query columns without accents: {new_query_cols}")

                prepath = Path(prepath)

                if not partition_columns or partition_columns[0] == "":
                    partition_column = None
                else:
                    partition_column = partition_columns[0]

                if not partition_column:
                    log("NO partition column specified! Writing unique files")
                else:
                    log(
                        f"Partition column: {partition_column} FOUND!! Write to partitioned files"
                    )

                # Now loop until we have no more data.
                batch = db_object.fetch_batch(batch_size)
                idx = 0
                batchs_len = 0
                while len(batch) > 0:
                    prepath.mkdir(parents=True, exist_ok=True)
                    # Log progress each 100 batches.
                    log_mod(
                        msg=f"Dumping batch {idx+1} with size {len(batch)}",
                        index=idx,
                        mod=log_number_of_batches,
                    )
                    batchs_len += len(batch)

                    # Dump batch to file.
                    dataframe = batch_to_dataframe(batch=batch, columns=columns)
                    old_columns = dataframe.columns.tolist()
                    dataframe.columns = remove_columns_accents(dataframe)
                    new_columns_dict = dict(
                        zip(old_columns, dataframe.columns.tolist())
                    )
                    dataframe = clean_dataframe(dataframe)
                    saved_files = []
                    if partition_column:
                        dataframe, date_partition_columns = parse_date_columns(
                            dataframe, new_columns_dict[partition_column]
                        )
                        partitions = date_partition_columns + [
                            new_columns_dict[col] for col in partition_columns[1:]
                        ]
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
                        msg=f"Batch generated {len(saved_files)} files. Will now upload.",
                        index=idx,
                        mod=log_number_of_batches,
                    )

                    # Upload files.
                    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
                    table_staging = f"{tb.table_full_name['staging']}"
                    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
                    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
                    storage_path_link = (
                        f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
                        f"/staging/{dataset_id}/{table_id}"
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
                            msg=f"Got partitions: {partitions}",
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
                            delete_blobs_list(
                                bucket_name=st.bucket_name, blobs=blobs_to_delete
                            )
                            log_mod(
                                msg=f"Deleted {len(blobs_to_delete)} blobs from GCS: {blobs_to_delete}",  # noqa
                                index=idx,
                                mod=log_number_of_batches,
                            )
                    if dump_mode == "append":
                        if tb.table_exists(mode="staging"):
                            log_mod(
                                msg=(
                                    "MODE APPEND: Table ALREADY EXISTS:"
                                    + f"\n{table_staging}"
                                    + f"\n{storage_path_link}"
                                ),
                                index=idx,
                                mod=log_number_of_batches,
                            )
                        else:
                            # the header is needed to create a table when dosen't exist
                            log_mod(
                                msg="MODE APPEND: Table DOESN'T EXISTS\nStart to CREATE HEADER file",  # noqa
                                index=idx,
                                mod=log_number_of_batches,
                            )
                            header_path = dump_header_to_file(data_path=saved_files[0])
                            log_mod(
                                msg="MODE APPEND: Created HEADER file:\n"
                                f"{header_path}",
                                index=idx,
                                mod=log_number_of_batches,
                            )

                            tb.create(
                                path=header_path,
                                if_storage_data_exists="replace",
                                if_table_exists="replace",
                                biglake_table=biglake_table,
                                dataset_is_public=dataset_is_public,
                            )

                            log_mod(
                                msg=(
                                    "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                                    + f"{table_staging}\n"
                                    + f"{storage_path_link}"
                                ),
                                index=idx,
                                mod=log_number_of_batches,
                            )  # pylint: disable=C0301

                            if not cleared_table:
                                st.delete_table(
                                    mode="staging",
                                    bucket_name=st.bucket_name,
                                    not_found_ok=True,
                                )
                                log_mod(
                                    msg=(
                                        "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"  # noqa
                                        + f"{storage_path}\n"
                                        + f"{storage_path_link}"
                                    ),
                                    index=idx,
                                    mod=log_number_of_batches,
                                )  # pylint: disable=C0301
                                cleared_table = True
                    elif dump_mode == "overwrite":
                        if tb.table_exists(mode="staging") and not cleared_table:
                            log_mod(
                                msg=(
                                    "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                                    + f"{storage_path}\n"
                                    + f"{storage_path_link}"
                                ),
                                index=idx,
                                mod=log_number_of_batches,
                            )  # pylint: disable=C0301
                            st.delete_table(
                                mode="staging",
                                bucket_name=st.bucket_name,
                                not_found_ok=True,
                            )
                            log_mod(
                                msg=(
                                    "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                                    + f"{storage_path}\n"
                                    + f"{storage_path_link}"
                                ),
                                index=idx,
                                mod=log_number_of_batches,
                            )  # pylint: disable=C0301
                            # delete only staging table and let DBT overwrite the prod table
                            tb.delete(mode="staging")
                            log_mod(
                                msg=(
                                    "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                                    + f"{table_staging}\n"
                                ),
                                index=idx,
                                mod=log_number_of_batches,
                            )  # pylint: disable=C0301

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
                                    "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                                    + f"{storage_path}\n"
                                    + f"{storage_path_link}"
                                ),
                                index=idx,
                                mod=log_number_of_batches,
                            )  # pylint: disable=C0301

                            log_mod(
                                msg="MODE OVERWRITE: Table DOSEN'T EXISTS\nStart to CREATE HEADER file",  # noqa
                                index=idx,
                                mod=log_number_of_batches,
                            )
                            header_path = dump_header_to_file(data_path=saved_files[0])
                            log_mod(
                                "MODE OVERWRITE: Created HEADER file:\n"
                                f"{header_path}",
                                index=idx,
                                mod=log_number_of_batches,
                            )

                            tb.create(
                                path=header_path,
                                if_storage_data_exists="replace",
                                if_table_exists="replace",
                                biglake_table=biglake_table,
                                dataset_is_public=dataset_is_public,
                            )

                            log_mod(
                                msg=(
                                    "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
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
                                    "MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"  # noqa
                                    + f"{storage_path}\n"
                                    + f"{storage_path_link}"
                                ),
                                index=idx,
                                mod=log_number_of_batches,
                            )  # pylint: disable=C0301
                            cleared_table = True

                    log_mod(
                        msg="STARTING UPLOAD TO GCS",
                        index=idx,
                        mod=log_number_of_batches,
                    )
                    if tb.table_exists(mode="staging"):
                        # Upload them all at once
                        tb.append(filepath=prepath, if_exists="replace")
                        log_mod(
                            msg=f"STEP UPLOAD: Sucessfully uploaded batch {idx +1} file with size {len(batch)} to Storage",
                            index=idx,
                            mod=log_number_of_batches,
                        )
                        for saved_file in saved_files:
                            # Delete the files
                            saved_file.unlink()
                    else:
                        # pylint: disable=C0301
                        log_mod(
                            msg="STEP UPLOAD: Table does not exist in STAGING, need to create first",  # noqa
                            index=idx,
                            mod=log_number_of_batches,
                        )
                    # Get next batch.
                    batch = db_object.fetch_batch(batch_size)
                    idx += 1

                    # delete batch data from prepath
                    shutil.rmtree(prepath)

                # end try
                attempts = -1

            except Exception as e:
                if attempts == 0:
                    log(f"last executed query: {query}")
                    raise e
                else:
                    log(
                        f"Remaning Attempts: {attempts}. Retry in {wait_seconds}s",
                        level="error",
                    )
                    log(f"executed query: {query}", level="error")
                    log(e, level="error")
                    # delete batch data from prepath
                    shutil.rmtree(prepath)

                    attempts -= 1
                    time.sleep(wait_seconds)  # wait 30 secondds

            # end back while

        log(
            msg=f"Successfully dumped {idx-1} batches, total of  {batchs_len} rows",  # noqa
        )
        # end of for queries
        total_idx += idx
        total_batchs_len += batchs_len

    log(
        msg=f"Successfully dumped {len(queries_strings)} queries, {total_idx} batches, total of {total_batchs_len} rows"  # noqa
    )
