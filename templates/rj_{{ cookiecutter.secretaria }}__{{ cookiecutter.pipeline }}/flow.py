"""Flow for rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}."""

from utils.flows import dump_database

rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }} = dump_database.with_options(name="rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}")
