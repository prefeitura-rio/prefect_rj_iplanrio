"""Flow for rj_ssm__run_dbt."""

from utils.flows import dbt_run

rj_ssm__run_dbt = dbt_run.with_options(name="rj_ssm__run_dbt")
