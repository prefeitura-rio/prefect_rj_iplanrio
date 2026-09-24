"""Flow for rj_rmi__run_dbt."""

from prefect import flow

from pipelines.rj_rmi__run_dbt.tasks import clone_repository_task, run_dbt_task, setup_credentials_task


@flow(log_prints=True, flow_run_name="DBT {command} {target}")
def rj_rmi__run_dbt(command: str = "build", select: str = "", flag: str = "", target: str = "dev") -> None:
    """Roda um comando dbt do queries-rj-rmi como a service account do RMI.

    :param command: Comando dbt, como ``build``, ``run``, ``test``, ``source freshness`` ou ``debug``.
    :param select: Valor do ``--select``, como ``tag:daily``.
    :param flag: Demais argumentos, separados como no shell, como ``--exclude x`` ou ``--vars '{a: 1}'``.
    :param target: Target do ``profiles.yml``. Na pod só ``prod`` funciona: a service account não tem papel no
        ``rj-rmi-dev``.
    """
    setup_credentials_task()
    project_dir = clone_repository_task()
    run_dbt_task(project_dir=project_dir, command=command, select=select, flag=flag, target=target)
