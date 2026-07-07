# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""

import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import flow


@flow(log_prints=True)
def rj_ssm__celular_seguro():
    url = getenv_or_action("API_SINESP__ACESS_TOKEN_URL")
    # api_key = getenv_or_action("API_SINESP__CLIENT_ID")
    client_id = getenv_or_action("API_SINESP__CLIENT_ID")
    client_secret = getenv_or_action("API_SINESP__CLIENT_SECRET")



    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    response = requests.post(url, json=payload, verify=False)

    print("Status:", response.status_code)
    print("Resultado:", response.json())