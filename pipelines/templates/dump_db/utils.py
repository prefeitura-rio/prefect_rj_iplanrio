# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from iplanrio.pipelines_utils.io import query_to_line


def create_dump_db_deployment(
    deployment_name: str,
    version: str,
    entrypoint: str,
    table_parameters_list: list,
    base_interval_seconds: int,
    base_anchor_date_str: str,
    runs_interval_minutes: int,
    timezone: str,
    db_type: str,
    db_database: str,
    db_host: str,
    db_port: int,
    dataset_id: str,
    infisical_secret_path: str,
    work_pool_name: str,
    work_queue_name: str,
    job_image: str,
    job_command: str,
    default_biglake_table: bool = True,
    default_batch_size: int = 50000,
):
    """
    Generates a full Prefect deployment YAML for database dump tasks.

    Args:
        deployment_name (str): The name for the Prefect deployment.
        entrypoint (str): The entrypoint for the flow (e.g., 'path/to/flow.py:flow_name').
        table_parameters_list (list): A list of dictionaries, each defining a table to dump.
        base_interval_seconds (int): The base interval for schedules.
        base_anchor_date_str (str): The anchor date for the first schedule.
        runs_interval_minutes (int): The number of minutes to wait between starting each schedule.
        timezone (str): The IANA timezone for all schedules.
        db_type (str): The database type (e.g., 'oracle').
        db_database (str): The name of the database.
        db_host (str): The database host.
        db_port (int): The database port.
        dataset_id (str): The default dataset ID for the dumps.
        infisical_secret_path (str): The path to secrets in Infisical.
        work_pool_name (str): The name of the work pool.
        work_queue_name (str): The name of the work queue.
        job_image (str): The Docker image for the job.
        job_command (str): The command to execute the flow run.
        default_biglake_table (bool): The default value for 'biglake_table'.
        default_batch_size (int): The default value for 'batch_size'.

    Returns:
        dict: A dictionary representing the complete Prefect deployment YAML.
    """
    base_anchor_date = datetime.fromisoformat(base_anchor_date_str)
    schedules = []

    for i, table_params in enumerate(table_parameters_list):
        # Calculate the staggered anchor date for this schedule
        anchor_date = base_anchor_date + timedelta(minutes=runs_interval_minutes * i)

        # Start with a base set of parameters for the flow run
        flow_run_parameters = {
            "db_type": db_type,
            "db_database": db_database,
            "db_host": db_host,
            "db_port": str(db_port),
            "dataset_id": table_params.get("dataset_id", dataset_id),
            "infisical_secret_path": infisical_secret_path,
            "biglake_table": default_biglake_table,
            "batch_size": default_batch_size,
        }

        # Merge the specific parameters for this table
        # This includes table_id, execute_query, dump_mode, etc.
        for key, value in table_params.items():
            if key == "execute_query":
                flow_run_parameters[key] = query_to_line(value).strip()
            else:
                flow_run_parameters[key] = value

        # Ensure required parameters from the list are set
        if "table_id" not in flow_run_parameters:
            raise ValueError(f"Missing 'table_id' in table parameters at index {i}")

        # Create the final schedule object for the YAML
        schedule_config = {
            "interval": base_interval_seconds,
            "anchor_date": anchor_date.isoformat(),
            "timezone": timezone,
            "slug": flow_run_parameters["table_id"],
            "parameters": flow_run_parameters,
        }
        schedules.append(schedule_config)

    # Assemble the final deployment structure
    deployment_config = {
        "deployments": [
            {
                "name": deployment_name,
                "version": version,
                "entrypoint": entrypoint,
                "work_pool": {
                    "name": work_pool_name,
                    "work_queue_name": work_queue_name,
                    "job_variables": {
                        "image": job_image,
                        "command": job_command,
                    },
                },
                "schedules": schedules,
            }
        ]
    }
    return deployment_config
