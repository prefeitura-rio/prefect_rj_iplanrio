# -*- coding: utf-8 -*-
"""Configurações do sistema de envio de e-mails."""
import os
from os import getenv
from dotenv import load_dotenv

# this folder path
env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
# if file .env exists, load it
if os.path.exists(env_file):
    import dotenv

    dotenv.load_dotenv(dotenv_path=env_file, override=True)
else:
    load_dotenv()


# BigQuery
BIGQUERY_PROJECT_ID = getenv("PIC_DISPAROS_EMAIL_BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET_ID = getenv("PIC_DISPAROS_EMAIL_BIGQUERY_DATASET_ID")
BIGQUERY_TABLE_ID = getenv("PIC_DISPAROS_EMAIL_BIGQUERY_TABLE_ID")

# Data Relay API
DATA_RELAY_URL = getenv(
    "PIC_DISPAROS_EMAIL_DATA_RELAY_URL",
    "https://staging.data-relay.dados.rio/data/mailman",
)
DATA_RELAY_API_KEY = getenv("PIC_DISPAROS_EMAIL_DATA_RELAY_API_KEY")
DATA_RELAY_FROM_ADDRESS = getenv("PIC_DISPAROS_EMAIL_DATA_RELAY_FROM_ADDRESS")
DATA_RELAY_USE_GMAIL_API = (
    getenv("PIC_DISPAROS_EMAIL_DATA_RELAY_USE_GMAIL_API", "true").lower() == "true"
)
DATA_RELAY_SUBJECT = (
    getenv("PIC_DISPAROS_EMAIL_DATA_RELAY_SUBJECT", "true").lower() == "true"
)
DATA_RELAY_IS_HTML_BODY = (
    getenv("PIC_DISPAROS_EMAIL_DATA_RELAY_IS_HTML_BODY", "true").lower() == "true"
)

# Retry and throttle settings
MAX_RETRIES = int(getenv("PIC_DISPAROS_EMAIL_MAX_RETRIES", "3"))
RETRY_DELAY = int(getenv("PIC_DISPAROS_EMAIL_RETRY_DELAY", "5"))  # segundos
THROTTLE_DELAY = float(getenv("PIC_DISPAROS_EMAIL_THROTTLE_DELAY", "1"))  # segundos


FILTER_EMAILS = getenv("PIC_DISPAROS_EMAIL_FILTER_EMAILS", "")
