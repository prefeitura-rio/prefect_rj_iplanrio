FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }} ./pipelines/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}/

RUN uv sync --all-packages
