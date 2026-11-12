# -*- coding: utf-8 -*-
"""
Constantes da pipeline Salesforce Data Cloud → BigQuery.
"""

from enum import Enum


class DataCloudConstants(Enum):
    """Defaults de projeto/dataset — sobrescritos por parâmetro quando precisa."""

    # Dataset padrão no BigQuery
    DATASET_ID = "brutos_salesforce"

    # Projeto GCP de destino
    BQ_PROJECT_ID = "rj-crm-registry"
