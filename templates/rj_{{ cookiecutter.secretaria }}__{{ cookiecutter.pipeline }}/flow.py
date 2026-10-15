"""Flow for rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}."""

from utils.flows import {{ cookiecutter.tipo }}

rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }} = {{ cookiecutter.tipo }}.with_options(name="rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}")
