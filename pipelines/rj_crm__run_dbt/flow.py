"""Flow for rj_crm__run_dbt."""

from utils.flows import dbt_run

rj_crm__run_dbt = dbt_run.with_options(name="rj_crm__run_dbt")
