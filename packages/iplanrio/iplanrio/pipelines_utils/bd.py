# -*- coding: utf-8 -*-
import base64
from pathlib import Path
from typing import Dict, List, Union

import basedosdados as bd
from basedosdados import Base
from google.cloud import storage
from google.cloud.storage.blob import Blob
from prefect import task

from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import dump_header_to_file


@task
def create_table_and_upload_to_gcs_task(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    biglake_table: bool = True,
    source_format: str = "csv",
    only_staging_dataset: bool = False,
) -> Union[str, Path]:
    return create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
        source_format=source_format,
        only_staging_dataset=only_staging_dataset,
    )


def _delete_prod_dataset(only_staging_dataset: bool, dataset_id: str):
    ds = bd.Dataset(dataset_id=dataset_id)
    if only_staging_dataset and ds.exists(mode="prod"):
        try:
            ds.delete(mode="prod")
            log("Successfully deleted prod dataset")
        except Exception as e:
            log(f"Error while deleting prod dataset: {e}")


def create_table_and_upload_to_gcs(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    biglake_table: bool = True,
    source_format: str = "csv",
    only_staging_dataset: bool = False,
) -> Union[str, Path]:
    """
    Create table using BD+ and upload to GCS.
    """
    bd_version = bd.__version__
    log(f"USING BASEDOSDADOS {bd_version}")
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    log(f"Dataset:{dataset_id} Table:{table_id} ")
    table_staging = f"{tb.table_full_name['staging']}"
    log(f"table_staging: {table_staging}")

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    log(f"storage_path: {storage_path}")
    storage_path_link = (
        f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
        f"/staging/{dataset_id}/{table_id}"
    )
    log(f"storage_path_link: {storage_path_link}")

    # prod datasets is public if the project is datario. staging are private im both projects
    dataset_is_public = tb.client["bigquery_prod"].project == "datario"
    log(f"dataset_is_public: {dataset_is_public}")

    #####################################
    #
    # MANAGEMENT OF TABLE CREATION
    #
    #####################################
    log("STARTING TABLE CREATION MANAGEMENT")
    if dump_mode == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"MODE APPEND: Table ALREADY EXISTS:"
                f"\n{table_staging}"
                f"\n{storage_path_link}"
            )
        else:
            # the header is needed to create a table when doesn't exist
            log("MODE APPEND: Table DOESN'T EXISTS\nStart to CREATE HEADER file")
            header_path = dump_header_to_file(
                data_path=data_path, data_type=source_format
            )
            log("MODE APPEND: Created HEADER file:\n" f"{header_path}")

            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_exists="replace",
                biglake_table=biglake_table,
                dataset_is_public=dataset_is_public,
                source_format=source_format,
                set_biglake_connection_permissions=False,
            )

            log(
                "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                f"{table_staging}\n"
                f"{storage_path_link}"
            )

            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )
    elif dump_mode == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )
            tb.delete(mode="staging")
            log(
                "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                f"{table_staging}\n"
                # f"{tb.table_full_name['prod']}"
            )

        # the header is needed to create a table when doesn't exist
        # in overwrite mode the header is always created
        log("MODE OVERWRITE: Table DOESN'T EXISTS\nStart to CREATE HEADER file")
        header_path = dump_header_to_file(data_path=data_path, data_type=source_format)
        log("MODE OVERWRITE: Created HEADER file:\n" f"{header_path}")

        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
            biglake_table=biglake_table,
            dataset_is_public=dataset_is_public,
            source_format=source_format,
            set_biglake_connection_permissions=False,
        )

        log(
            "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
            f"{table_staging}\n"
            f"{storage_path_link}"
        )

        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        log(
            f"MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )

    if only_staging_dataset:
        _delete_prod_dataset(
            only_staging_dataset=only_staging_dataset, dataset_id=dataset_id
        )
    #####################################
    #
    # Uploads a bunch of files using BD+
    #
    #####################################

    log("STARTING UPLOAD TO GCS")
    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(filepath=data_path, if_exists="replace")

        log(
            f"STEP UPLOAD: Successfully uploaded {data_path} to Storage:\n"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )
    else:
        log("STEP UPLOAD: Table does not exist in STAGING, need to create first")

    return data_path


def get_storage_blobs(dataset_id: str, table_id: str, mode: str = "staging") -> list:
    """
    Get all blobs from a table in a dataset.

    Args:
        dataset_id (str): dataset id
        table_id (str): table id
        mode (str, optional): mode to use. Defaults to "staging".

    Returns:
        list: list of blobs
    """

    bd_storage = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    return list(
        bd_storage.client["storage_staging"]
        .bucket(bd_storage.bucket_name)
        .list_blobs(prefix=f"{mode}/{bd_storage.dataset_id}/{bd_storage.table_id}/")
    )


def get_project_id(mode: str = None) -> str:
    """
    Get the project ID from the environment.

    Args:
        mode (str): The mode to filter by (prod or staging).
    """

    if mode not in ["prod", "staging"]:
        raise ValueError("Mode must be 'prod' or 'staging'")
    base = Base()
    return base.config["gcloud-projects"][mode]["name"]


def list_blobs_with_prefix(
    bucket_name: str, prefix: str, mode: str = "prod"
) -> List[Blob]:
    """
    Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    Mode needs to be "prod" or "staging"
    """

    credentials = get_bd_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    return list(blobs)


def secret_to_base64(secret_dict: Dict) -> str:
    """
    Converts a dictionary to a JSON-formatted string, encodes it in Base64,
    and returns the Base64-encoded string.

    Args:
    secret_dict (Dict): A dictionary to be encoded.

    Returns:
    str: A Base64-encoded string.
    """
    input_string = str(secret_dict).replace("'", '"')
    bytes_data = input_string.encode("utf-8")
    base64_data = base64.b64encode(bytes_data)
    base64_string = base64_data.decode("utf-8")
    return base64_string


def get_base64_bd_config(projec_id: str) -> str:
    """
    Generates a Base64-encoded configuration string for a project ID,
    formatted for use with a data bucket and Google Cloud configurations.

    Args:
    projec_id (str): The project ID used in the configuration.

    Returns:
    str: A Base64-encoded configuration string.
    """

    string = f"""# What is the bucket that you are saving all the data? It should be
                # an unique name.
                bucket_name = "{projec_id}"

                [gcloud-projects]

                [gcloud-projects.staging]
                    credentials_path = "~/.basedosdados/credentials/staging.json"
                    name = "{projec_id}"

                [gcloud-projects.prod]
                    credentials_path = "~/.basedosdados/credentials/prod.json"
                    name = "{projec_id}"

                [api]
                url = "https://api.dados.rio/api/v1/graphql"
            """.replace(
        "                ", ""
    )

    string_bytes = string.encode("utf-8")

    encoded_string = base64.b64encode(string_bytes).decode("utf-8")

    return encoded_string


def base64_to_string(base64_string: str) -> str:
    """
    Decodes a Base64-encoded string back to a regular string.

    Args:
    base_64 (str): The Base64 string to be decoded.

    Returns:
    str: The decoded string.
    """
    base64_bytes = base64_string.encode("utf-8")
    message_bytes = base64.b64decode(base64_bytes)
    message = message_bytes.decode("utf-8")
    return message
