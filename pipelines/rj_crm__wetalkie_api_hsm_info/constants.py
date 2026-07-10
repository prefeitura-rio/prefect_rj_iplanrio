# -*- coding: utf-8 -*-
"""
Constants for request rj_crm__wetalkie_api_hsm_info pipeline
"""

from enum import Enum


class Constants(Enum):
    """
    Constants for the Wetalkie HSM Info pipeline
    """

    # Dataset and table BigQuery
    DATASET_ID = "brutos_wetalkie"
    TABLE_ID = "hsm_templates_info"
    DUMP_MODE = "overwrite"

    # Partition configuration
    PARTITION_COLUMN = "data_particao"
    FILE_FORMAT = "csv"
    ROOT_FOLDER = "./data_hsm_info/"

    # BigQuery configuration
    BIGLAKE_TABLE = True
    BILLING_PROJECT_ID = "rj-crm-registry"

    # Wetalkie API Configuration
    INFISICAL_SECRET_PATH = "/wetalkie" # reusing from rj_crm__api_wetalkie
    BASE_URL = "https://api.whatsapp.dados.rio/v1"
    LOGIN_ROUTE = "users/login"
    HSM_ROUTE = "callcenter/hsms"
    TIMEOUT = 30
