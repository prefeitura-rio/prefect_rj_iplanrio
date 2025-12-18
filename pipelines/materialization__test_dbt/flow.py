from prefect_dbt import PrefectDbtRunner

from prefect import flow, task

from iplanrio.pipelines_utils.env import inject_bd_credentials_task


@flow(log_prints=True)
def materialization__test_dbt():
    """
    Flow to execute dbt materialization tests.
    """
    inject_bd_credentials_task()

    runner = PrefectDbtRunner()
    profiles_dir = 'opt/prefect/pipelines_v3/queries'
    project_dir = 'opt/prefect/pipelines_v3/queries'
    run_result = runner.invoke(['compile', '--select', 'models/br_rj_riodejaneiro_brt_gps','--project-dir', project_dir, '--profiles-dir', profiles_dir])
    print(run_result)  # You can log or process the result as needed