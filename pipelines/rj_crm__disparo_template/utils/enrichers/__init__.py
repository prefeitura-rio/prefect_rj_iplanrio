# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Registry of DataFrame enrichers para disparo template.
Cada módulo neste pacote representa uma API externa: contém a chamada HTTP e
a função de enriquecimento correspondente. Selecionado no flow via
`enrich_with_api_name`/`enrich_with_api_params`, sem precisar de branch por
campanha. Espelha o padrão de utils/processors.py (QUERY_PROCESSORS), mas
operando sobre o DataFrame pós-query em vez do texto da query.

Pra registrar um novo enricher: crie um módulo `utils/enrichers/<nome>.py`
com uma função `(df: pd.DataFrame, params: dict) -> pd.DataFrame` e adicione
uma entrada em DF_ENRICHERS abaixo.
"""

from pipelines.rj_crm__disparo_template.utils.enrichers.consulta_debitos import (  # pylint: disable=E0611, E0401
    enrich_with_debitos_api,
)

DF_ENRICHERS = {
    "consulta_debitos": enrich_with_debitos_api,
    # Futuros enrichers entram aqui.
}


def get_df_enricher(name: str):
    """Get dataframe enricher function by name"""
    return DF_ENRICHERS.get(name)
