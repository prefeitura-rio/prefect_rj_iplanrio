# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Migrated DBT Transform Flow from Prefect 1.4 to 3.0
"""

import datetime
import json
import os
import re
import shutil
from typing import TypedDict

import git
import requests
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from iplanrio.pipelines_utils.logging import log
from utils import send_message, log_to_file, process_dbt_logs, Summarizer, download_from_cloud_storage, upload_to_cloud_storage
from pathlib import Path


# Import statements for external modules (to be handled later)..
# from prefeitura_rio.pipelines_utils.credential_injector import authenticated_task
# from prefeitura_rio.pipelines_utils.dbt import Summarizer, log_to_file, process_dbt_logs
# from prefeitura_rio.pipelines_utils.googleutils import download_from_cloud_storage, upload_to_cloud_storage
# from prefeitura_rio.pipelines_utils.logging import log
# from prefeitura_rio.pipelines_utils.monitor import send_message


class GcsBucket(TypedDict):
    prod: str
    dev: str


@task
def download_repository(git_repository_path: str):
    """
    Downloads the repository specified by the REPOSITORY_URL.
    """
    # Create repository folder
    try:
        repository_path = os.path.join(os.getcwd(), "dbt_repository")

        if os.path.exists(repository_path):
            shutil.rmtree(repository_path, ignore_errors=False)
        os.makedirs(repository_path)

        log(f"Repository folder created: {repository_path}")

    except Exception as e:
        raise Exception(f"Error when creating repository folder: {e}")

    # Download repository
    try:
        git.Repo.clone_from(git_repository_path, repository_path)
        log(f"Repository downloaded: {git_repository_path}")
    except git.GitCommandError as e:
        raise Exception(f"Error when downloading repository: {e}")

    # check for 'queries' folder
    queries_path = os.path.join(repository_path, "queries")
    if os.path.isdir(queries_path):
        log(f"'queries' folder found at: {queries_path}")
        return queries_path

    return repository_path


@task
def execute_dbt(
    repository_path: str,
    command: str = "run",
    target: str = "dev",
    select="",
    exclude="",
    state="",
    flag="",
    prefect_environment="",
):
    """
    Executes a dbt command using PrefectDbtRunner from prefect-dbt.
    """
    
    # Build the command arguments
    if command == "source freshness":
        command_args = ["source", "freshness"]
    else:
        command_args = [command]
    
    if command in ("build", "data_test", "run", "test", "source freshness"):
        command_args.extend(["--target", target])
        
        if select:
            command_args.extend(["--select", select])
        if exclude:
            command_args.extend(["--exclude", exclude])
        if state:
            command_args.extend(["--state", state])
        if flag:
            command_args.extend([flag])
    
    log(f"Executing dbt command: {' '.join(command_args)}", level="info")
    
    settings_dbt = PrefectDbtSettings(
            profiles_dir=Path(repository_path),
            project_dir=Path(repository_path)
    )

    # Initialize PrefectDbtRunner with project directory
    runner = PrefectDbtRunner(
        settings=settings_dbt,
        raise_on_failure=False  # Allow the flow to handle failures gracefully
    )
        
    # Execute the dbt command
    running_result = runner.invoke(command_args)
    
    log("RESULTADOS:")
    log(str(running_result))
    
    # Check if the command was successful
    if not running_result.success:
        send_message(
            title="❌ Erro ao executar DBT",
            message=f"DBT command '{command}' failed. Check logs for details.",
            monitor_slug="dbt-runs",
            prefect_environment=prefect_environment,
        )
        raise Exception(f"DBT command '{command}' failed. Success: {running_result.success}")
    
    return running_result


@task
def create_dbt_report(
    running_results,
    repository_path: str,
    bigquery_project: str,
    prefect_environment: str,
    github_issue_repository: str,
) -> None:
    """
    Creates a report based on the results of running dbt commands.
    """
    logs = process_dbt_logs(log_path=os.path.join(repository_path, "logs", "dbt.log"))

    log(f"Processed logs: {logs}", level="info")
    log_path = log_to_file(logs)
    summarizer = Summarizer()

    is_successful, has_warnings = True, False

    general_report = []
    failed_models = []
    
    # PrefectDbtRunner provides results in a different format
    # The result object contains information about the execution
    if hasattr(running_results, 'result') and running_results.result:
        for command_result in running_results.result:
            if hasattr(command_result, 'status'):
                if command_result.status == "fail":
                    is_successful = False
                    general_report.append(f"- 🛑 FAIL: {summarizer(command_result)}")
                    if hasattr(command_result, 'node') and hasattr(command_result.node, 'name'):
                        failed_models.append(command_result.node.name)
                elif command_result.status == "error":
                    is_successful = False
                    general_report.append(f"- ❌ ERROR: {summarizer(command_result)}")
                    if hasattr(command_result, 'node') and hasattr(command_result.node, 'name'):
                        failed_models.append(command_result.node.name)
                elif command_result.status == "warn":
                    has_warnings = True
                    general_report.append(f"- ⚠️ WARN: {summarizer(command_result)}")
                    if hasattr(command_result, 'node') and hasattr(command_result.node, 'name'):
                        failed_models.append(command_result.node.name)
                elif command_result.status == "runtime error":  # Table which source freshness failed
                    is_successful = False
                    general_report.append(f"- ⏱️ STALE TABLE: {summarizer(command_result)}")
                    if hasattr(command_result, 'node') and hasattr(command_result.node, 'name'):
                        failed_models.append(command_result.node.name)
    
    # If no detailed results, check overall success
    if not general_report:
        if not running_results.success:
            is_successful = False
            general_report.append(f"- ❌ DBT command failed with success status: {running_results.success}")
        else:
            general_report.append("- ✅ DBT command completed successfully")

    # Sort and log the general report
    general_report = sorted(general_report, reverse=True)
    general_report = "**Resumo**:\n" + "\n".join(general_report)
    log(general_report)

    # Get Parameters
    param_report = ["**Parametros**:"]

    context = get_run_context()
    parameters = context.parameters

    if parameters.get("environment") == "dev":
        bigquery_project = "rj-" + bigquery_project + "-dev"
    elif parameters.get("environment") == "prod":
        bigquery_project = "rj-" + bigquery_project

    param_report.append(f"- Projeto BigQuery: `{bigquery_project}`")
    param_report.append(f"- Target dbt: `{parameters.get('environment')}`")
    param_report.append(f"- Comando: `{parameters.get('command')}`")

    if parameters.get("select"):
        param_report.append(f"- Select: `{parameters.get('select')}`")
    if parameters.get("exclude"):
        param_report.append(f"- Exclude: `{parameters.get('exclude')}`")
    if parameters.get("flag"):
        param_report.append(f"- Flag: `{parameters.get('flag')}`")

    param_report.append(
        f"- GitHub Repo: `{parameters.get('github_repo').rsplit('/', 1)[-1].removesuffix('.git')}`"
    )

    param_report = "\n".join(param_report)
    param_report += " \n"

    # PrefectDbtRunner provides success status directly
    fully_successful = is_successful and getattr(running_results, 'success', True)
    include_report = has_warnings or (not fully_successful)

    # DBT - Sending Logs to Discord
    command = parameters.get("command")
    emoji = "❌" if not fully_successful else "✅"
    complement = "com Erros" if not fully_successful else "sem Erros"
    message = f"{param_report}\n{general_report}" if include_report else param_report

    send_message(
        title=f"{emoji} [{bigquery_project}] - Execução `dbt {command}` finalizada {complement}",
        message=message,
        file_path=log_path,
        monitor_slug="dbt-runs",
        prefect_environment=prefect_environment,
    )

#    if not fully_successful and parameters.get("environment") == "prod":
#        log(f"Warning the X9 Agent about failed models: {failed_models}")
#
#        br_timezone = datetime.timezone(datetime.timedelta(hours=-3))
#
#        github_issue_repo = github_issue_repository.split("/")[-1].replace(".git", "")
#
#        log(f"Github issue repo: {github_issue_repo}")
#
#        # Raw content with failed models list
#        # Clean and format logs for AI processing
#        cleaned_logs = []
#        for _, row in logs.iterrows():
#            # Clean the text by removing special characters and normalizing
#            clean_text = row["text"]
#
#            # Remove ANSI color codes and escape sequences
#            clean_text = re.sub(r"\x1b\[[0-9;]*m", "", clean_text)
#            clean_text = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", clean_text)
#
#            # Remove or replace problematic characters for JSON
#            clean_text = clean_text.replace("`", "").replace('"', "'")
#            clean_text = clean_text.replace("\\", "/")  # Replace backslashes
#            clean_text = re.sub(
#                r"[\x00-\x1f\x7f-\x9f]", "", clean_text
#            )  # Remove control characters
#
#            # Normalize whitespace and remove excessive spaces
#            clean_text = " ".join(clean_text.split())
#
#            # Skip empty messages after cleaning
#            if clean_text.strip():
#                cleaned_logs.append(
#                    {"timestamp": row["time"], "level": row["level"], "message": clean_text}
#                )
#
#        data = {
#            "source_system": "dbt",
#            "timestamp": datetime.datetime.now(br_timezone).isoformat(),
#            "metadata": {
#                "failed_models_dbt": failed_models,
#                "log_message_original": cleaned_logs,
#                "github_issue_repo": github_issue_repo,
#            },
#        }
#
#        # Get the proxy url from Infisical
#        headers = {
#            "Content-Type": "application/json",
#            "X-Proxy-Api-Token": get_secret(secret_name="PROXY_TOKEN")["PROXY_TOKEN"],
#        }
#
#        api_url = get_secret(secret_name="PROXY_CLICKUP_JOURNALIST")["PROXY_CLICKUP_JOURNALIST"]
#
#        # Validate JSON before sending
#        try:
#            json.dumps(data, ensure_ascii=False, default=str)
#            log(f"✅ JSON validation successful - {len(cleaned_logs)} logs processed")
#        except Exception as json_error:
#            log(f"❌ JSON validation failed: {json_error}")
#            # Fallback: send only essential data without logs
#            data = {
#                "source_system": "dbt",
#                "timestamp": datetime.datetime.now(br_timezone).isoformat(),
#                "metadata": {
#                    "failed_models_dbt": failed_models,
#                    "log_summary": logs.to_dict(),
#                    "github_issue_repo": github_issue_repo,
#                    "log_error": "Logs could not be serialized due to encoding issues",
#                },
#            }
#
#        # Send the data to the x9 agent
#        try:
#            response = requests.post(api_url, json=data, headers=headers, timeout=300)
#        except requests.exceptions.RequestException as e:
#            log(f"❌ Failed to send DBT log to X9 Agent: {e}")
#            return
#
#        log(f"✅ DBT log sent successfully")
#        log(f"Response status: {response.status_code}")
#        log(f"Response content: {response.text}")
#
#        # Parse the response to extract the message
#        try:
#            response_text = json.loads(response.text)
#        except json.JSONDecodeError:
#            log(f"❌ Failed to decode JSON response: {response.text}")
#            return
#
#        # Extract task details from response
#        task_details = response_text.get("task_details", {})
#        details = task_details.get("name", "Detalhes não disponíveis")
#        ticket_link = task_details.get("url", "Link não disponível")
#
#        # Get the Discord webhook URL for Incidentes from Infisical
#        incidentes_webhook_discord = get_secret(secret_name="DISCORD_WEBHOOK_URL_INCIDENTES")[
#            "DISCORD_WEBHOOK_URL_INCIDENTES"
#        ]
#
#        discord_message = None
#
#        # If the response is successful, prepare the Discord message
#        if response.status_code == 200:
#            log(f"Sending message to Incidentes Discord webhook about the ticket creation")
#            discord_message = {
#                "content": "🚨 **Novo Incidente** 🚨",
#                "embeds": [
#                    {
#                        "title": "Novo Incidente",
#                        "description": "Incidente detectado no fluxo do DBT",
#                        "color": 15158332,  # Red color for incident
#                        "fields": [
#                            {"name": "📊 FLUXO", "value": "DBT", "inline": True},
#                            {"name": "📁 Projeto", "value": bigquery_project, "inline": True},
#                            {"name": "📝 DETALHES", "value": details, "inline": False},
#                            {"name": "🔗 LINK DO TICKET", "value": ticket_link, "inline": False},
#                        ],
#                        "footer": {
#                            "text": "Agente X9 🤫",
#                        },
#                        "timestamp": datetime.datetime.now(br_timezone).isoformat(),
#                    }
#                ],
#            }
#
#        elif response.status_code == 409:  # Card already exists
#            log(f"⚠️ Card already exists: {response_text.get('details', 'No message provided')}")
#
#        else:
#            log(f"❌ API response was not successful, status code: {response.status_code}")
#
#        # Send Discord webhook if message was created
#        if discord_message:
#            try:
#                discord_response = requests.post(
#                    incidentes_webhook_discord,
#                    json=discord_message,
#                    headers={"Content-Type": "application/json"},
#                    timeout=300,
#                )
#                discord_response.raise_for_status()
#                log(f"✅ Discord webhook sent successfully")
#                log(f"Discord response status: {discord_response.status_code}")
#
#            except requests.exceptions.RequestException as e:
#                log(f"❌ Failed to send Discord webhook: {e}")
#
    raise Exception(general_report)


@task
def rename_current_flow_run_dbt(
    command: str,
    target: str,
    select: str,
    exclude: str,
) -> None:
    """
    Rename the current flow run.
    """
    # In Prefect 3.0, flow run naming is handled differently
    # This is a placeholder for the functionality
    flow_run_name = f"dbt {command}"

    if select:
        flow_run_name += f" --select {select}"
    if exclude:
        flow_run_name += f" --exclude {exclude}"

    flow_run_name += f" --target {target}"

    log(f"Flow run would be renamed to: {flow_run_name}", level="info")


@task
def get_target_from_environment(environment: str):
    """
    Retrieves the target environment based on the given environment parameter.
    """
    converter = {
        "prod": "prod",
        "local-prod": "prod",
        "staging": "dev",
        "local-staging": "dev",
        "dev": "dev",
    }
    return converter.get(environment, "dev")


@task
def download_dbt_artifacts_from_gcs(dbt_path: str, environment: str, gcs_buckets: GcsBucket):
    """
    Retrieves the dbt artifacts from Google Cloud Storage.
    """
    gcs_bucket = gcs_buckets[environment]

    target_base_path = os.path.join(dbt_path, "target_base")

    if os.path.exists(target_base_path):
        shutil.rmtree(target_base_path, ignore_errors=False)
        os.makedirs(target_base_path)

    try:
        download_from_cloud_storage(target_base_path, gcs_bucket)
        log(f"DBT artifacts downloaded from GCS bucket: {gcs_bucket}", level="info")
        return target_base_path

    except Exception as e:
        log(f"Error when downloading DBT artifacts from GCS: {e}", level="error")
        return None


@task
def upload_dbt_artifacts_to_gcs(dbt_path: str, environment: str, gcs_buckets: GcsBucket):
    """
    Sends the dbt artifacts to Google Cloud Storage.
    """
    dbt_artifacts_path = os.path.join(dbt_path, "target_base")

    gcs_bucket = gcs_buckets[environment]

    upload_to_cloud_storage(dbt_artifacts_path, gcs_bucket)
    log(f"DBT artifacts sent to GCS bucket: {gcs_bucket}", level="info")


@task
def check_if_dbt_artifacts_upload_is_needed(command: str):
    """
    Checks if the upload of dbt artifacts is needed.
    """
    if command in ["build", "source freshness"]:
        return True
    return False


@task
def get_current_flow_project_name():
    """
    Placeholder for get_current_flow_project_name function.
    """
    return "dbt-transform-project"


@flow(log_prints=True)
def rj_iplanrio__run_dbt(
    # Flow parameters
    rename_flow: bool = False,
    send_discord_report: bool = True,
    
    # DBT parameters
    command: str = "test",
    select: str = None,
    exclude: str = None,
    flag: str = None,
    github_repo: str = None,
    bigquery_project: str = None,
    dbt_secrets: list[str] = None,
    
    # GCP parameters
    environment: str = "dev",
    gcs_buckets: GcsBucket = None,
) -> None:
    """
    Main DBT Transform Flow migrated from Prefect 1.4 to 3.0
    """
    
    #####################################
    # Set environment
    ####################################
    target = get_target_from_environment(environment=environment)
    
    current_flow_project_name = get_current_flow_project_name()

    if rename_flow:
        rename_current_flow_run_dbt(
            command=command, 
            select=select, 
            exclude=exclude, 
            target=target
        )
    
    download_repository_task = download_repository(git_repository_path=github_repo)
    
    install_dbt_packages = execute_dbt(
        repository_path=download_repository_task,
        target=target,
        command="deps",
    )
    
    download_dbt_artifacts_task = download_dbt_artifacts_from_gcs(
        dbt_path=download_repository_task, 
        environment=environment, 
        gcs_buckets=gcs_buckets
    )
    
    ####################################
    # Tasks section #1 - Execute commands in DBT
    #####################################
    
    running_results = execute_dbt(
        repository_path=download_repository_task,
        state=download_dbt_artifacts_task,
        target=target,
        command=command,
        select=select,
        exclude=exclude,
        flag=flag,
        prefect_environment=current_flow_project_name,
    )
    
    if send_discord_report:
        create_dbt_report(
            running_results=running_results,
            repository_path=download_repository_task,
            bigquery_project=bigquery_project,
            prefect_environment=current_flow_project_name,
            github_issue_repository=github_repo,
        )
    
    ####################################
    # Tasks section #3 - Upload new artifacts to GCS
    #####################################
    
    check_if_upload_dbt_artifacts = check_if_dbt_artifacts_upload_is_needed(command=command)
    
    if check_if_upload_dbt_artifacts:
        upload_dbt_artifacts_to_gcs(
            dbt_path=download_repository_task, 
            environment=environment, 
            gcs_buckets=gcs_buckets
        )

# PARAMETERS 

rj_iplanrio__run_dbt(
    flag="",
    select="",
    command="source freshness",
    exclude="",
    dbt_secrets=[],
    environment="prod",
    gcs_buckets={
        "dev": "rj-iplanrio-dev_dbt",
        "prod": "rj-iplanrio_dbt"
    },
    github_repo="https://github.com/prefeitura-rio/queries-rj-iplanrio.git",
    rename_flow=True,
    bigquery_project="rj-iplanrio",
    send_discord_report=False
)