# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Migrated DBT Transform Flow from Prefect 1.4 to 3.0
"""

import os
import re
import shutil
from typing import TypedDict, Optional

import git
from prefect import flow, task
from prefect import runtime
from prefect_dbt import PrefectDbtRunner
from iplanrio.pipelines_utils.logging import log
from utils import send_message, log_to_file, process_dbt_logs, Summarizer, download_from_cloud_storage, upload_to_cloud_storage
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from prefect.states import Failed


class GcsBucket(TypedDict):
    prod: str
    dev: str


@task
def get_current_flow_info():
    """
    Retrieves the current flow project, flow run id and environment.
    """
    try:
        flow_name = runtime.flow_run.name
        flow_run_id = runtime.flow_run.id
        flow_environment = runtime.deployment.name.split("--")[-1]
    except Exception:
        flow_name = None
        flow_run_id = None
        flow_environment = None

    return {
        "flow_name": flow_name,
        "flow_run_id": flow_run_id,
        "flow_environment": flow_environment,
    }


@task
def download_repository(git_repository_path: str):
    """
    Downloads the repository specified by the REPOSITORY_URL.
    """
    if not git_repository_path:
        raise ValueError("git_repository_path is required")
    
    # Create repository folder
    try:
        repository_path = os.path.join(os.getcwd(), "dbt_repository")

        if os.path.exists(repository_path):
            shutil.rmtree(repository_path, ignore_errors=False)
        os.makedirs(repository_path)

        log(f"Repository folder created: {repository_path}", level="info")

    except Exception as e:
        raise Exception(f"Error when creating repository folder: {e}")

    # Download repository
    try:
        git.Repo.clone_from(git_repository_path, repository_path)
        log(f"Repository downloaded: {git_repository_path}", level="info")
    except git.GitCommandError as e:
        raise Exception(f"Error when downloading repository: {e}")

    # check for 'queries' folder
    queries_path = os.path.join(repository_path, "queries")
    if os.path.isdir(queries_path):
        log(f"'queries' folder found at: {queries_path}", level="info")
        return queries_path

    return repository_path


@task
def execute_dbt(
    command: str = "run",
    target: str = "dev",
    select: str = "",
    exclude: str = "",
    state: str = "",
    flag: str = "",
):
    """
    Executes a dbt command using PrefectDbtRunner from prefect-dbt.
    
    Args:
        command (str): DBT command to execute (run, test, build, source freshness, deps, etc.)
        target (str): DBT target environment (dev, prod, etc.)
        select (str): DBT select argument for filtering models
        exclude (str): DBT exclude argument for filtering models
        state (str): DBT state argument for incremental processing
        flag (str): Additional DBT flags
        
    Returns:
        PrefectDbtResult: Result of the DBT command execution
    """
    
    # Build the command arguments
    if command == "source freshness":
        command_args = ["source", "freshness"]
    else:
        command_args = [command]
    
    # Add common arguments for most DBT commands
    if command in ("build", "run", "test", "source freshness", "seed", "snapshot"):
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
    
    # Initialize PrefectDbtRunner
    runner = PrefectDbtRunner(
        raise_on_failure=False  # Allow the flow to handle failures gracefully
    )
        
    # Execute the dbt command with the constructed arguments
    try:
        running_result = runner.invoke(command_args)
        log(f"DBT command completed with success: {running_result.success}", level="info")
    except Exception as e:
        log(f"Error executing DBT command: {e}", level="error")
        raise
    
    
    return running_result


@task
def install_dbt_dependencies():
    """
    Installs DBT dependencies using the 'deps' command.
    This task is specifically designed to install packages defined in packages.yml.
    """
    
    log("Installing DBT dependencies...", level="info")
    
    # Initialize PrefectDbtRunner
    runner = PrefectDbtRunner(
        raise_on_failure=False  # Allow the flow to handle failures gracefully
    )
    
    # Execute the dbt deps command
    try:
        deps_result = runner.invoke(["deps"])
        log("✅ DBT dependencies installed successfully", level="info")
        return deps_result
    except Exception as e:
        log(f"❌ Error installing DBT dependencies: {e}", level="error")
        raise


@task
def create_dbt_report(
    running_results,
    repository_path: str,
    bigquery_project: str,
    flow_info: dict,
    github_issue_repository: str,
    send_discord_report: bool,
) -> None:
    """
    Creates a report based on the results of running dbt commands.
    """
    try:
        logs = process_dbt_logs(log_path=os.path.join(repository_path, "logs", "dbt.log"))
    except Exception as e:
        log(f"Warning: Could not process DBT logs: {e}", level="warning")
        logs = None

    log_path = None
    if logs is not None:
        log_path = log_to_file(logs)
    
    summarizer = Summarizer()
    parameters = runtime.flow_run.parameters
    is_successful, has_warnings = True, False
    general_report = []
    failed_models = []
    
    # Check if the are errors in the running results
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


    # Sort and log the general report
    general_report = sorted(general_report, reverse=True)
    general_report = "**Resumo**:\n" + "\n".join(general_report)
    log(general_report)
    
    if send_discord_report:
        # Get Parameters
        param_report = ["**Parametros**:"]

        if parameters.get("target") == "dev":
            bigquery_project = bigquery_project + "-dev"
        elif parameters.get("target") == "prod":
            bigquery_project = bigquery_project

        param_report.append(f"- Projeto BigQuery: `{bigquery_project}`")
        param_report.append(f"- Target dbt: `{parameters.get('target')}`")
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
            flow_info=flow_info,
            destination="notifications",  # Default destination for DBT reports
        )

#    if not fully_successful and parameters.get("target") == "prod":
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
#
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
    if not is_successful:
        return Failed(message=f"dbt {parameters.get('command')} executed with errors")

@task
def download_dbt_artifacts_from_gcs(environment: str, gcs_buckets: GcsBucket) -> Optional[str]:
    """
    Retrieves the dbt artifacts from Google Cloud Storage.
    
    Args:
        environment (str): Environment (dev/prod)
        gcs_buckets (GcsBucket): Dictionary with bucket names for each environment
        
    Returns:
        Optional[str]: Path to downloaded artifacts or None if failed
    """
    if not gcs_buckets or environment not in gcs_buckets:
        log(f"No GCS bucket configured for environment: {environment}", level="warning")
        return None
        
    gcs_bucket = gcs_buckets[environment]
    gcs_artifacts_path = os.path.join(os.getcwd(), "gcs_artifacts")

    if os.path.exists(gcs_artifacts_path):
        shutil.rmtree(gcs_artifacts_path, ignore_errors=False)
        os.makedirs(gcs_artifacts_path)

    try:
        download_from_cloud_storage(gcs_artifacts_path, gcs_bucket)
        log(f"DBT artifacts downloaded from GCS bucket: {gcs_bucket}", level="info")
        return gcs_artifacts_path

    except Exception as e:
        log(f"Error when downloading DBT artifacts from GCS: {e}", level="error")
        return None


@task
def upload_dbt_artifacts_to_gcs(environment: str, gcs_buckets: GcsBucket) -> bool:
    """
    Sends the dbt artifacts to Google Cloud Storage.
    
    Args:
        environment (str): Environment (dev/prod)
        gcs_buckets (GcsBucket): Dictionary with bucket names for each environment
        
    Returns:
        bool: True if upload was successful, False otherwise
    """
    if not gcs_buckets or environment not in gcs_buckets:
        log(f"No GCS bucket configured for environment: {environment}", level="warning")
        return False
        
    gcs_bucket = gcs_buckets[environment]
    dbt_artifacts_path = os.path.join(os.getcwd(), "target_base")

    # Check if artifacts directory exists
    if not os.path.exists(dbt_artifacts_path):
        log(f"DBT artifacts directory not found: {dbt_artifacts_path}", level="warning")
        return False

    try:
        upload_to_cloud_storage(path=dbt_artifacts_path, bucket_name=gcs_bucket)
        log(f"DBT artifacts uploaded to GCS bucket: {gcs_bucket}", level="info")
        return True
    except Exception as e:
        log(f"Error when uploading DBT artifacts to GCS: {e}", level="error")
        return False


@flow(log_prints=True, flow_run_name="DBT {command} {target}")
def rj_iplanrio__run_dbt(
    # Flow parameters
    send_discord_report: bool = True,
    
    # DBT parameters
    command: str = "test",
    select: str = None,
    exclude: str = None,
    flag: str = None,
    github_repo: str = None,
    bigquery_project: str = None,
    target: str = "dev",
    
    # GCP parameters
    gcs_buckets: GcsBucket = None,
) -> None:
    """
    Main DBT Transform Flow migrated from Prefect 1.4 to 3.0
    
    Args:
        send_discord_report (bool): Whether to send Discord report
        command (str): DBT command to execute
        select (str): DBT select argument
        exclude (str): DBT exclude argument
        flag (str): Additional DBT flags
        github_repo (str): GitHub repository URL
        bigquery_project (str): BigQuery project name
        target (str): DBT target environment
        gcs_buckets (GcsBucket): GCS bucket configuration
    """
    
    # Validate required parameters
    if not github_repo:
        raise ValueError("github_repo is required")
    if not bigquery_project:
        raise ValueError("bigquery_project is required")
    
    # Load BQ Credentials
    crd = inject_bd_credentials_task(environment="prod")  # noqa
    
    # Get flow info
    flow_info = get_current_flow_info()

    log(f"Flow name: {flow_info['flow_name']}", level="info")
    log(f"Flow run id: {flow_info['flow_run_id']}", level="info")
    log(f"Flow environment: {flow_info['flow_environment']}", level="info")

    # Download repository
    download_repository_task = download_repository(git_repository_path=github_repo)
    
    # Download dbt artifacts
    download_dbt_artifacts_task = download_dbt_artifacts_from_gcs(
        environment=target, 
        gcs_buckets=gcs_buckets
    )
    
    # Install dbt packages
    install_dbt_packages = install_dbt_dependencies()
        
    # Execute DBT command
    running_results = execute_dbt(
          command=command,
          target=target,
          select=select,
          exclude=exclude,
          flag=flag,
          state=download_dbt_artifacts_task,
    )
    
    # Create summary report
    dbt_report = create_dbt_report(
        running_results=running_results,
        repository_path=download_repository_task,
        bigquery_project=bigquery_project,
        flow_info=flow_info,
        github_issue_repository=github_repo,
        send_discord_report=send_discord_report,
    )
    
    # Upload dbt artifacts to GCS if needed
    if flow_info["flow_environment"] == "prod" and command in ["build", "source freshness"]:
        upload_dbt_artifacts_to_gcs(
            environment=target, 
            gcs_buckets=gcs_buckets
        )

    return dbt_report
