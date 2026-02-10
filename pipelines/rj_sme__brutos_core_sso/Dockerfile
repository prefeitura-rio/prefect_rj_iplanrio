FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_sme__brutos_core_sso ./pipelines/rj_sme__brutos_core_sso/

RUN uv sync --package prefect_rj_iplanrio --package rj_sme__brutos_core_sso
