# -*- coding: utf-8 -*-
"""
Tasks para a pipeline rj_iplanrio__1746_seconverva_salesforce_poc
"""

import os
import base64
import json
import pandas as pd
import gspread
from google.oauth2 import service_account
from prefect import task, get_run_logger
from iplanrio.pipelines_utils.logging import log

@task
def get_1746_credentials():
    """
    Busca credenciais do segredo 1746 injetado no ambiente pelo Kubernetes.
    """
    log("Recuperando credenciais do banco 1746")

    user = os.getenv("RJ_IPLANRIO__1746_TICKET_CAPTURE__DB_USERNAME")
    password = os.getenv("RJ_IPLANRIO__1746_TICKET_CAPTURE__DB_PASSWORD")

    if not user or not password:
        # Fallback para nomes genéricos caso a injeção plana não use o prefixo do segredo
        user = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")

    if not user or not password:
        raise ValueError("Credenciais DB_USERNAME ou DB_PASSWORD não encontradas no ambiente.")

    return {
        "user": user,
        "password": password
    }

@task
def write_to_google_sheets_task(
    dataframe: pd.DataFrame,
    spreadsheet_id: str,
    sheet_name: str = "Dados"
):
    """
    Escreve o DataFrame no Google Sheets (Full Dump).
    Utiliza as credenciais BASEDOSDADOS_CREDENTIALS_PROD injetadas.
    """
    logger = get_run_logger()
    
    if not spreadsheet_id:
        logger.warning("spreadsheet_id não fornecido. Pulando escrita no Sheets.")
        return

    # 1. Carregar credenciais da conta de serviço a partir do ambiente
    env_creds = os.getenv("BASEDOSDADOS_CREDENTIALS_PROD")
    if not env_creds:
        logger.error("BASEDOSDADOS_CREDENTIALS_PROD não encontrada no ambiente.")
        return

    try:
        creds_info = json.loads(base64.b64decode(env_creds))
        logger.info(f"Utilizando conta de serviço: {creds_info.get('client_email')}")
        logger.info("### IMPORTANTE: Compartilhe sua planilha com o e-mail acima como 'Editor'!")

        # 2. Autenticar
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = service_account.Credentials.from_service_account_info(creds_info).with_scopes(scopes)
        gc = gspread.authorize(creds)
        
        # 3. Abrir planilha e aba
        sh = gc.open_by_key(spreadsheet_id)
        
        try:
            worksheet = sh.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=sheet_name, rows="100", cols="20")
            logger.info(f"Aba '{sheet_name}' não encontrada. Criada nova aba.")

        # 4. FULL DUMP (Limpar e Escrever)
        worksheet.clear()
        
        # Preparar dados (Header + Valores)
        # Limpeza robusta: converte tudo para string e remove NaNs/Nones manualmente
        raw_data = [dataframe.columns.tolist()] + dataframe.values.tolist()
        cleaned_data = []
        for row in raw_data:
            new_row = [
                "" if pd.isna(val) or val is None or str(val).lower() in ["nan", "none", "nat"]
                else str(val)
                for val in row
            ]
            cleaned_data.append(new_row)
        
        worksheet.update('A1', cleaned_data)
        logger.info(f"Dados exportados com sucesso para a planilha {spreadsheet_id} (Aba: {sheet_name}).")
        
    except Exception as e:
        logger.error(f"Erro ao acessar Google Sheets: {e}")
        raise e
