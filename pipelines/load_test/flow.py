# -*- coding: utf-8 -*-
import base64
import random
from datetime import datetime
from os import environ
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import parse_date_columns, to_partitions
from prefect import flow, task


@task
def inject_bd_credentials(environment: str = "prod"):
    service_account_name = f"BASEDOSDADOS_CREDENTIALS_{environment.upper()}"
    service_account = base64.b64decode(environ[service_account_name])
    with open("/tmp/credentials.json", "wb") as credentials_file:
        credentials_file.write(service_account)
    environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"
    log(f"INJECTED: {service_account_name}")


def get_response_from_api(N: int = 3):
    r = requests.get(url=f"https://www.randomnumberapi.com/api/v1.0/random?count={N}")
    return r.json()


def get_dataframe(number_list, updated_at, run_number, seed, flow_id):
    params = {}
    params["flow_id"] = flow_id
    params["run"] = run_number
    params["updated_at"] = updated_at
    params["seed"] = seed
    s, sub, m, div = seed, seed, seed, seed
    for count, num in enumerate(number_list):
        params[f"num_{count+1}"] = num
        s += num
        sub -= num
        m *= num
        if num != 0:
            div /= num
        else:
            div = np.nan

    np_numbers = np.array(number_list)
    params["pure_sum"] = np.sum(np_numbers)
    params["pure_product"] = np.prod(np_numbers)  # Produto de todos os elementos

    params["min_val"] = np.min(np_numbers)
    params["max_val"] = np.max(np_numbers)
    params["range_val"] = params["max_val"] - params["min_val"]
    params["mean"] = np.mean(np_numbers)
    params["median"] = np.median(np_numbers)

    params["std_dev"] = np.std(np_numbers)
    params["variance"] = np.var(np_numbers)

    params["q1"] = np.percentile(np_numbers, 25)
    params["q3"] = np.percentile(np_numbers, 75)

    return pd.DataFrame([params])


@task
def get_metrics(number_rows=10, flow_id=1):
    final_df = pd.DataFrame()
    seed = random.randint(10, 100)
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for i in range(number_rows):
        log(f"{i+1} - {100*(i+1)/number_rows}%")

        number_list = get_response_from_api()
        dataframe = get_dataframe(
            number_list=number_list,
            updated_at=updated_at,
            run_number=i,
            seed=seed,
            flow_id=flow_id,
        )
        final_df = pd.concat([final_df, dataframe])

    return final_df


@task
def to_partition_task(data, partition_date_column, savepath):
    savepath = Path(savepath)
    savepath.mkdir(parents=True, exist_ok=True)
    dataframe_to_partition, partition_columns = parse_date_columns(
        dataframe=data, partition_date_column=partition_date_column
    )
    suffix = (
        dataframe_to_partition[partition_date_column]
        .unique()[0]
        .replace("-", "_")
        .replace(" ", "__")
        .replace(":", "_")
    )

    to_partitions(
        data=dataframe_to_partition,
        partition_columns=partition_columns,
        savepath=savepath,
        suffix=suffix,
    )
    return savepath


@flow
def load_test(
    flow_id=1,
    number_rows=100,
    dataset_id="aa_load_test",
    table_id="metrics",
):
    crd = inject_bd_credentials()
    data = get_metrics(number_rows=number_rows, flow_id=flow_id)
    savepath = to_partition_task(
        data=data, partition_date_column="updated_at", savepath="/tmp/data"
    )
    data_path = create_table_and_upload_to_gcs(
        data_path=savepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
    )
