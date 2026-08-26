# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""

import logging
import time

from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow
from prefect_rj_iplanrio.log import get_logger

logger = get_logger(__name__)



@flow(log_prints=True)
def rj_iplanrio__test(table_id):
    rename_current_flow_run_task(new_name=table_id)
    for x in range(5):
        time.sleep(2)
        logger.debug(f"Total, %i %s", x, "DEBUG")
        logger.info("Total, %i %s", x, "INFO")
        logger.warning("Total, %i %s", x, "WARNINGS")
        logger.critical("Total, %i %s", x, "CRITICAL")



rj_iplanrio__test("Testando...")