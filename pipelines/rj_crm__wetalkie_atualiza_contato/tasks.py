"""
Tasks migradas do Prefect 1.4 para 3.0 - CRM Wetalkie Atualiza Contato

⚠️ ATENÇÃO: Algumas funções da biblioteca prefeitura_rio NÃO têm equivalente no toolkit:
- handler_initialize_sentry: SEM EQUIVALENTE
- ENDRUN/Skipped: Substituído por return/exception padrão
- LocalDaskExecutor: Removido no Prefect 3.0
- KubernetesRun: Removido no Prefect 3.0 (configurado no YAML)
- GCS storage: Removido no Prefect 3.0 (configurado no YAML)
"""

import json

import pandas as pd
from prefect import task
from prefect.logging import get_logger

logger = get_logger("toolkit")


@task
def get_contacts(api: object, dfr: pd.DataFrame) -> pd.DataFrame:
    """
    Get all missing contacts from the Wetalkie API

    Args:
        api: Authenticated API handler instance
        dfr: DataFrame with contact IDs that need phone data

    Returns:
        DataFrame with updated contact phone and name data
    """
    if dfr.empty:
        logger.info("No contacts missing phone - returning empty DataFrame")
        return pd.DataFrame()  # Return empty DataFrame instead of raising ENDRUN

    logger.info(f"Getting {dfr.shape[0]} missing contacts from the Wetalkie API")

    # Create a copy to avoid modifying the original DataFrame
    result_dfr = dfr.copy()
    result_dfr["json_data"] = None

    updated_count = 0
    failed_count = 0

    for contact_id in result_dfr["id_contato"].unique():
        try:
            logger.info(f"Getting contact {contact_id} from the Wetalkie API")
            response = api.get(path=f"/callcenter/contacts/{contact_id!s}")

            # Handle API response structure
            if hasattr(response, "json"):
                response_data = response.json()
            else:
                response_data = response

            if not response_data.get("data"):
                logger.info(f"No data found for contact {contact_id}")
                failed_count += 1
                continue

            data = response_data["data"]

            # Check if the expected structure exists
            if "item" not in data or not data["item"]:
                logger.info(f"No item data found for contact {contact_id}")
                failed_count += 1
                continue

            item = data["item"]

            # Update contact information
            result_dfr.loc[result_dfr["id_contato"] == contact_id, "json_data"] = json.dumps(item)
            exemple = result_dfr.loc[result_dfr["id_contato"] == contact_id].copy()
            updated_count += 1

        except Exception as error:
            logger.error(f"Error processing contact {contact_id}: {error}")
            failed_count += 1
            continue

    logger.info(f"Contact processing completed. Updated: {updated_count}, Failed: {failed_count}")

    # Filter out contacts that weren't updated successfully
    successful_contacts = result_dfr[result_dfr["json_data"].notna()]
    successful_contacts["id_contato"] = successful_contacts["id_contato"].astype(str)

    logger.info(f"Returning {len(successful_contacts)} successfully updated contacts")
    return successful_contacts


@task
def download_missing_contacts(query: str, billing_project_id: str, bucket_name: str) -> pd.DataFrame:
    """
    Download contacts with missing phone data from BigQuery

    Args:
        query: SQL query to get missing contacts
        billing_project_id: GCP project ID for billing
        bucket_name: GCS bucket name for credentials

    Returns:
        DataFrame with contact IDs that need phone data
    """
    from pipelines.rj_crm__wetalkie_atualiza_contato.helpers.tasks import (
        download_data_from_bigquery,
    )

    logger.info("Downloading missing contacts from BigQuery")
    dfr = download_data_from_bigquery(query, billing_project_id, bucket_name)
    logger.info(f"Found {len(dfr)} contacts with missing phone data")
    return dfr


@task
def safe_export_df_to_parquet(dfr: pd.DataFrame, output_path: str = "./data_contacts/") -> str:
    """
    Safely exports a DataFrame to a Parquet file in the specified directory

    Args:
        dfr: DataFrame to export
        output_path: Directory path for the output file

    Returns:
        str: The path to the exported parquet file
    """
    import os
    import uuid

    # Create directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)

    # Generate a unique filename
    filename = f"contacts_{uuid.uuid4()}.parquet"
    parquet_path = os.path.join(output_path, filename)

    # Export to parquet
    dfr.to_parquet(parquet_path, index=False)
    logger.info(f"DataFrame exported to parquet: {parquet_path}")

    return output_path  # Return the directory path as expected by BigQuery upload
