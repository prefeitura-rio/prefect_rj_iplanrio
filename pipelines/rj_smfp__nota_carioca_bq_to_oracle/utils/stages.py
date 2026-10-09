"""Passos da carga BigQuery → Oracle, etapas de cada tabela e os pesos da barra de progresso."""

from enum import IntEnum


class Step(IntEnum):
    """Passos da execução, em ordem; o valor é a posição no checklist."""

    DBT = 0
    EXPORT = 1
    TABLES = 2
    INMEMORY_WAIT = 3
    SWAP = 4


class TableStep(IntEnum):
    """Etapas de cada tabela, em ordem; ``DONE`` é a tabela concluída."""

    PREPARATION = 0
    LOAD = 1
    VALIDATION = 2
    INDEXES = 3
    STATS = 4
    ACCESS = 5
    INMEMORY = 6
    DONE = 7


STEP_WEIGHTS = {Step.DBT: 20.0, Step.EXPORT: 5.0, Step.TABLES: 70.0, Step.INMEMORY_WAIT: 4.0, Step.SWAP: 1.0}
STEP_LABELS = {
    Step.DBT: "dbt",
    Step.EXPORT: "Exportação do BigQuery",
    Step.TABLES: "Carga das tabelas",
    Step.INMEMORY_WAIT: "Espera do In-Memory",
    Step.SWAP: "Troca dos sinônimos",
}
TABLE_STEP_LABELS = {
    TableStep.PREPARATION: "Preparação",
    TableStep.LOAD: "Carga SQL*Loader",
    TableStep.VALIDATION: "Validação",
    TableStep.INDEXES: "Índices",
    TableStep.STATS: "Estatísticas",
    TableStep.ACCESS: "Acesso/registro",
    TableStep.INMEMORY: "In-Memory",
    TableStep.DONE: "Concluída",
}
TABLE_WEIGHTS = {
    TableStep.PREPARATION: 5.0,
    TableStep.LOAD: 60.0,
    TableStep.VALIDATION: 3.0,
    TableStep.INDEXES: 20.0,
    TableStep.STATS: 7.0,
    TableStep.ACCESS: 2.0,
    TableStep.INMEMORY: 3.0,
}
TABLE_WEIGHT_TOTAL = sum(TABLE_WEIGHTS.values())
