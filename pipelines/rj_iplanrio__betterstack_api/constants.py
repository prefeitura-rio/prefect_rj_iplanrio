# -*- coding: utf-8 -*-
from enum import Enum


class BetterStackConstants(Enum):
    """
    Constantes utilizadas na pipeline do BetterStack.
    """

    BASE_URL_V3 = "https://uptime.betterstack.com/api/v3"



    # Dataset e Tabelas
    DATASET_ID = "brutos_betterstack"
    BILLING_PROJECT_ID = "rj-iplanrio"
    TABLE_ID_INCIDENTS = "eai_gateway_incidents"

    # Configurações de execução
    DUMP_MODE = "append"
    MATERIALIZE_AFTER_DUMP = True

    # Partitioning
    PARTITION_COLUMN = "data_particao"
    FILE_FORMAT = "parquet"
    ROOT_FOLDER = "/tmp/betterstack"
    BIGLAKE_TABLE = True

    # Timeout
    TIMEOUT = (5, 30)

