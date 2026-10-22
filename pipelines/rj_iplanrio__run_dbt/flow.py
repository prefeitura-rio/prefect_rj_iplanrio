"""Flow for rj_iplanrio__run_dbt."""

from utils.flows import dbt_run

rj_iplanrio__run_dbt = dbt_run.with_options(name="rj_iplanrio__run_dbt")
