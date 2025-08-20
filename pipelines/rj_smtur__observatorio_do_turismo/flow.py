# -*- coding: utf-8 -*-
from typing import Optional

from iplanrio.pipelines_templates.dump_url.flows import (
    parse_comma_separated_string_to_list_task,
)
from iplanrio.pipelines_templates.dump_url.tasks import download_url, dump_files
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow


@flow(log_prints=True)
def rj_smtur__observatorio_do_turismo(
    # URL parameters
    url: str,
    url_type: str = "direct",
    gsheets_sheet_order: int = 0,
    gsheets_sheet_name: Optional[str] = None,
    gsheets_sheet_range: Optional[str] = None,
    # Table parameters
    partition_columns: str = "",
    encoding: str = "utf-8",
    on_bad_lines: str = "error",
    separator: str = ",",
    # BigQuery parameters
    dataset_id: Optional[str] = None,
    table_id: Optional[str] = None,
    dump_mode: str = "overwrite",  # overwrite or append
    # JSON dataframe parameters
    dataframe_key_column: Optional[str] = None,
    build_json_dataframe: bool = False,
    biglake_table: bool = False,
):
    """
    Main flow for dumping data from URLs to BigQuery.
    """
    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_task(new_name=f"Dump: {dataset_id}.{table_id}")

    #####################################
    #
    # Tasks section #1 - Get data
    #
    #####################################
    DATA_PATH = "/tmp/dump_url/"
    DUMP_DATA_PATH = "/tmp/dump_url_chunks/"
    DATA_FNAME = DATA_PATH + "data.csv"

    DOWNLOAD_URL_TASK = download_url(
        url=url,
        fname=DATA_FNAME,
        url_type=url_type,
        gsheets_sheet_order=gsheets_sheet_order,
        gsheets_sheet_name=gsheets_sheet_name,
        gsheets_sheet_range=gsheets_sheet_range,
    )

    partition_columns = parse_comma_separated_string_to_list_task(text=partition_columns)

    DUMP_CHUNKS_TASK = dump_files(
        file_path=DATA_FNAME,
        partition_columns=partition_columns,
        save_path=DUMP_DATA_PATH,
        build_json_dataframe=build_json_dataframe,
        dataframe_key_column=dataframe_key_column,
        encoding=encoding,
        on_bad_lines=on_bad_lines,
        separator=separator,
    )

    #####################################
    #
    # Tasks section #2 - Create table
    #
    #####################################
    CREATE_TABLE_AND_UPLOAD_TO_GCS_TASK = create_table_and_upload_to_gcs_task(
        data_path=DUMP_DATA_PATH,
        dataset_id=dataset_id,
        table_id=table_id,
        biglake_table=biglake_table,
        dump_mode=dump_mode,
    )
