# -*- coding: utf-8 -*-
"""
Constants for the rj_crm__whitelist_whatsapp pipeline.
"""
from enum import Enum


class WhitelistConstants(Enum):
    """
    Constants used in the WhatsApp Whitelist pipeline.
    """

    # Infisical Paths
    INFISICAL_PATH_AUTH = "/api-rmi-identidade-carioca"
    INFISICAL_PATH_DATA = "/api-rmi-whitelist"

    # Dataset and Tables
    DATASET_ID = "brutos_rmi_whitelist"
    BILLING_PROJECT_ID = "rj-iplanrio"
    TABLE_ID = "daily_rmi_whitelist"

    # Execution Settings
    DUMP_MODE = "append" # Full scrape daily, but we can treat as incremental if we use week_start for idempotency
    MATERIALIZE_AFTER_DUMP = True

    # Partitioning
    PARTITION_COLUMN = "data_particao"
    FILE_FORMAT = "parquet"
    ROOT_FOLDER = "/tmp/whitelist_whatsapp"
    BIGLAKE_TABLE = True

    # API Settings
    PER_PAGE = 100
    TIMEOUT = (10, 60) # Connect, Read
