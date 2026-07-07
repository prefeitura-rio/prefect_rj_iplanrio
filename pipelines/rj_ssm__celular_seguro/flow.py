# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""
import os

from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import flow


@flow(log_prints=True)
def rj_ssm__celular_seguro():
    url = getenv_or_action("API_SINESP__ACESS_TOKEN_URL")
    # api_key = getenv_or_action("API_SINESP__CLIENT_ID")
    client_id = getenv_or_action("API_SINESP__CLIENT_ID")
    client_secret = getenv_or_action("API_SINESP__CLIENT_SECRET")

    result = os.system(
        f"""
        curl --request POST "{url}"
        --header 'Content-Type: application/json'
        --data-raw '{
        "grant_type": "client_credentials",
        "client_id": "{client_id}",
        "client_secret": "{client_secret}"
        }' -kv
        """
    )

    print(f"Result: {result}")