# -*- coding: utf-8 -*-
"""
Constantes para a pipeline TaxiRio - Migrada para Prefect 3.0.

Este módulo define todas as constantes necessárias para o processamento
das collections do MongoDB do TaxiRio, incluindo configurações de cada tabela.
"""

from enum import Enum
from typing import Optional


class Constants(Enum):
    """Constantes gerais do projeto TaxiRio."""

    DATASET_ID = "brutos_taxirio"
    MONGODB_CONNECTION_STRING = "DB_CONNECTION_STRING"
    MONGODB_DATABASE_NAME = "taxirio"
    INFISICAL_SECRET_PATH = "/taxirio"


class TableConfig:
    """
    Configuração para processamento de uma tabela do TaxiRio.

    Attributes:
        table_id: Nome da tabela/collection no MongoDB
        dump_mode: Modo de dump ('overwrite' ou 'append')
        partition_cols: Lista de colunas de particionamento
        use_period: Se deve processar por período (True) ou dump completo (False)
        frequency: Frequência para quebra por período ('D' para diário, None se não usa período)
    """

    def __init__(
        self,
        table_id: str,
        dump_mode: str = "overwrite",
        partition_cols: Optional[list[str]] = None,
        use_period: bool = False,
        frequency: Optional[str] = None,
    ):
        self.table_id = table_id
        self.dump_mode = dump_mode
        self.partition_cols = partition_cols or []
        self.use_period = use_period
        self.frequency = frequency


# Configurações de cada tabela do TaxiRio
TABLE_CONFIGS = {
    "cities": TableConfig(
        table_id="cities",
        dump_mode="overwrite",
        partition_cols=[],
        use_period=False,
    ),
    "drivers": TableConfig(
        table_id="drivers",
        dump_mode="overwrite",
        partition_cols=["ano_particao", "mes_particao"],
        use_period=False,
    ),
    "races": TableConfig(
        table_id="races",
        dump_mode="append",
        partition_cols=["ano_particao", "mes_particao", "dia_particao"],
        use_period=True,
        frequency="D",
    ),
    "passengers": TableConfig(
        table_id="passengers",
        dump_mode="append",
        partition_cols=["ano_particao", "mes_particao", "dia_particao"],
        use_period=True,
        frequency="D",
    ),
    "users": TableConfig(
        table_id="users",
        dump_mode="overwrite",
        partition_cols=["ano_particao", "mes_particao"],
        use_period=False,
    ),
    "discounts": TableConfig(
        table_id="discounts",
        dump_mode="overwrite",
        partition_cols=[],
        use_period=False,
    ),
    "paymentmethods": TableConfig(
        table_id="paymentmethods",
        dump_mode="overwrite",
        partition_cols=[],
        use_period=False,
    ),
    "rankingraces": TableConfig(
        table_id="rankingraces",
        dump_mode="overwrite",
        partition_cols=[],
        use_period=False,
    ),
    "metricsdriverunoccupieds": TableConfig(
        table_id="metricsdriverunoccupieds",
        dump_mode="overwrite",
        partition_cols=[],
        use_period=False,
    ),
}
