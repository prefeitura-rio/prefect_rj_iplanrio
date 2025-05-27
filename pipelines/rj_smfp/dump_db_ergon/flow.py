# -*- coding: utf-8 -*-
from iplanrio.pipeline_utils.infisical import (
    get_database_username_and_password_from_secret, inject_bd_credentials)
from iplanrio.pipeline_utils.logging import log
from prefect import flow, task


@task
def get_database_username_and_password_from_secret_tastk(secret_path: str):
    secrets = get_database_username_and_password_from_secret(secret_path=secret_path)
    return secrets


@task
def inject_bd_credentials_task(enviroment: str = "prod"):
    inject_bd_credentials(enviroment=enviroment)


@flow(log_prints=True)
def ergon(secret_path: str):
    inject_bd_credentials_task(enviroment="prod")
    secrets = get_database_username_and_password_from_secret_tastk(
        secret_path=secret_path
    )

    login = secrets[0]
    password = secrets[1]
    log(login)


if __name__ == "__main__":
    ergon(secret_path="")
