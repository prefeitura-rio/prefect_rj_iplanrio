"""Flow for rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}."""

from prefect_rj_iplanrio.flows import dump_database

rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }} = dump_database.with_options(name="rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}")
