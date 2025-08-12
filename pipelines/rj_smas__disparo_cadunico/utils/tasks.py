# -*- coding: utf-8 -*-
"""
Utility tasks for prefect_rj_iplanrio pipelines
Migrated and adapted from pipelines_rj_crm_registry
"""

from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task

from pipelines.rj_smas__disparo_cadunico.utils.api_handler import ApiHandler


@task
def access_api(
    infisical_path: str,
    infisical_url: str,
    infisical_username: str,
    infisical_password: str,
    login_route: str = "users/login",
) -> ApiHandler:
    """
    Access API and return authenticated handler to be used in other requests.
    Uses environment variables injected from Infisical secrets.

    Args:
        infisical_path: Path in Infisical (e.g., "/wetalkie")
        infisical_url: Key name for URL in Infisical
        infisical_username: Key name for username in Infisical
        infisical_password: Key name for password in Infisical
        login_route: API login endpoint path

    Returns:
        ApiHandler: Authenticated API handler instance
    """
    # Transform path for environment variables
    # /wetalkie -> WETALKIE
    env_prefix = infisical_path.upper().replace("-", "_").replace("/", "")

    # Get credentials from environment variables
    url = getenv_or_action(f"{env_prefix}__{infisical_url.upper()}")
    username = getenv_or_action(f"{env_prefix}__{infisical_username.upper()}")
    password = getenv_or_action(f"{env_prefix}__{infisical_password.upper()}")

    # Create and return authenticated API handler
    api = ApiHandler(base_url=url, username=username, password=password, login_route=login_route)

    return api


@task
def create_date_partitions(*args, **kwargs):
    """
    Placeholder for create_date_partitions task.
    This function needs to be implemented based on the original from pipelines_rj_crm_registry.
    """
    # TODO: Implement this function based on the original requirements
    pass
