# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""

import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import flow
import os

@flow(log_prints=True)
def rj_ssm__celular_seguro():
    url = getenv_or_action("API_SINESP__ACESS_TOKEN_URL")
    # api_key = getenv_or_action("API_SINESP__CLIENT_ID")
    client_id = getenv_or_action("API_SINESP__CLIENT_ID")
    client_secret = getenv_or_action("API_SINESP__CLIENT_SECRET")
    # headers = getenv_or_action("API_SINESP__HEADERS")
    cert_key = getenv_or_action("API_SINESP__CERT_KEY")
    cert_crt = getenv_or_action("API_SINESP__CERT_CRT")

    print("Testando diretamente pelo requests")
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    headers = {
        'Content-Type: application/json'
    }
    cert = (
        cert_crt,
        cert_key
    )

    response = requests.post(url,
                            json=payload,
                            verify=False,
                            cert=cert,
                            headers=headers
                            )

    print("Status:", response.status_code)
    print("Resultado:", response.json())


    requests.post()
