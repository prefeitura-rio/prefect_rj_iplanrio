# -*- coding: utf-8 -*-
from prefect import flow, task

from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.env import (
    get_database_username_and_password_from_secret_env,
)


@task
def get_database_username_and_password_from_secret_tastk(secret_path: str):
    return get_database_username_and_password_from_secret_env(path=secret_path)


@flow(log_prints=True)
def dump_db_ergon(secret_path: str):
    secrets = get_database_username_and_password_from_secret_tastk(
        secret_path=secret_path
    )
    log(secrets["DB_USERNAME"])
    log(secrets["DB_USERNAME"])


if __name__ == "__main__":
    dump_db_ergon(secret_path="db-ergon-prod")
