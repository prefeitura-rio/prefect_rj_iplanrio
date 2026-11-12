# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""

import time

from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow


@flow(log_prints=True)
def rj_iplanrio__test(table_id):
    rename_current_flow_run_task(new_name=table_id)
    for x in range(10):
        time.sleep(1)
        print(f"{x} - Hello World")

