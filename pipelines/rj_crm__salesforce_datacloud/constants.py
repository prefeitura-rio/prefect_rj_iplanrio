# -*- coding: utf-8 -*-
"""
Constantes da pipeline Salesforce Data Cloud → BigQuery.
"""

from enum import Enum

# Âncora do load_query: os .sql ficam em queries/, ao lado deste arquivo (STYLEGUIDE §7.3).
QUERIES_ANCHOR = __file__


class DataCloudConstants(Enum):
    """Defaults de projeto/dataset — sobrescritos por parâmetro quando precisa."""

    # Dataset padrão no BigQuery
    DATASET_ID = "brutos_salesforce"

    # Projeto GCP de destino
    BQ_PROJECT_ID = "rj-crm-registry"
