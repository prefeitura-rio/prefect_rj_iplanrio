FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pipelines/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }} ./pipelines/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}/
