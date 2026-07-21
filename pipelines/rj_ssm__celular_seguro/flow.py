import requests
from prefect import flow
from iplanrio.pipelines_utils.env import getenv_or_action
import tempfile

@flow(log_prints=True)
def rj_ssm__celular_seguro():
    url = getenv_or_action("API_SINESP__ACESS_TOKEN_URL")
    client_id = getenv_or_action("API_SINESP__CLIENT_ID")
    client_secret = getenv_or_action("API_SINESP__CLIENT_SECRET")
    cert_key = getenv_or_action("API_SINESP__CERT_KEY")
    cert_crt = getenv_or_action("API_SINESP__CERT_CRT")


    with tempfile.NamedTemporaryFile(mode="w", suffix=".crt", delete=False) as crt_file:
        crt_file.write(cert_crt)
        crt_path = crt_file.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False) as key_file:
        key_file.write(cert_key)
        key_path = key_file.name

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    headers = {
        "Content-Type": "application/json"
    }


    response = requests.post(
        url,
        json=payload,
        headers=headers,
        cert=(crt_path, key_path),
        verify=False,
        timeout=30,
    )

    print("Status:", response.status_code)
    print("Resposta:", response.text)

    response.raise_for_status()

    try:
        print(response.json())
    except ValueError:
        print("A resposta não é um JSON válido.")