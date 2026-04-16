# -*- coding: utf-8 -*-
import json
import os

import pandas as pd
import requests
from prefect import task


def requests_get_compstat(
                        result: str,
                        endpoint : str = "blitz-leiseca",
                        url_base : str = "https://compstat.bugarintec.com.br/compstat/api/",
                        token_estatic : str = "compstat-rio-static-token-2026-fm-qmd") -> json:

    url = f"{url_base}{endpoint}"

    params = {
        "token" : token_estatic
    }

    response = requests.get(url, params=params)

    data = response.json()

    results = data.get(result)
    return results


@task
def json_compstat_to_dataframe(
    table_id: str,
    result: str,
    endpoint,
) -> pd.DataFrame:

    requests = requests_get_compstat(
        result=result,
        endpoint=endpoint
    )
    path = os.path.join(os.getcwd(), f"pipelines/rj_ssm__compstat/{table_id}/data.csv")
    os.makedirs(path, exist_ok=True)

    # ! Tabela blitz/lei_seca
    if table_id == "blitz_lei_seca":

        df = pd.json_normalize(requests)
        df.to_csv(path, index=False)


    # ! Tabela subareas
    elif table_id == "subareas":
        df = pd.json_normalize(
            requests[0]['qmds']
            , sep='_'
        )

        df = df.explode('elementos')

        df_elementos = pd.json_normalize(df['elementos'], sep='_')

        df_final = pd.concat(
            [
                df.drop(columns=['elementos']).reset_index(drop=True),
                df_elementos.reset_index(drop=True)
            ],
            axis=1
        )
        df_final.to_csv(path, index=False)

    return path
