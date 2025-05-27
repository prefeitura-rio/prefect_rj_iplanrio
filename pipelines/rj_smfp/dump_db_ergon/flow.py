from prefect import flow, task
from pipelines.utils.infisical import get_database_username_and_password_from_secret
from pipelines.utils.utils import log


@task
def get_database_username_and_password_from_secret_tastk(secret_path: str):
    secrets = get_database_username_and_password_from_secret(secret_path=secret_path)
    return secrets


@flow(log_prints=True)
def ergon(secret_path: str):

    secrets = get_database_username_and_password_from_secret_tastk(
        secret_path=secret_path
    )
    login = secrets[0]
    password = secrets[1]
    log(login)


if __name__ == "__main__":
    ergon(secret_path="")
