"""Configurações do sistema de envio de e-mails."""
import os
from dotenv import load_dotenv

load_dotenv()



# Configurações SMTP Gmail
SMTP_SERVER = os.getenv("PIC_DISPAROS_EMAIL_SMTP_SERVER")
SMTP_PORT = int(os.getenv("PIC_DISPAROS_EMAIL_SMTP_PORT"))
SMTP_USERNAME = os.getenv("PIC_DISPAROS_EMAIL_SMTP_USERNAME", "") # Email do remetente
SMTP_PASSWORD = os.getenv("PIC_DISPAROS_EMAIL_SMTP_PASSWORD", "")  # App Password do Gmail
EMAIL_FROM = os.getenv("PIC_DISPAROS_EMAIL_EMAIL_FROM", SMTP_USERNAME) # Email do remetente
BIGQUERY_PROJECT_ID = os.getenv("PIC_DISPAROS_EMAIL_BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET_ID = os.getenv("PIC_DISPAROS_EMAIL_BIGQUERY_DATASET_ID")
BIGQUERY_TABLE_ID = os.getenv("PIC_DISPAROS_EMAIL_BIGQUERY_TABLE_ID")
