FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

# Install basic build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_crm__webhook_wetalkie_template_status_update ./pipelines/rj_crm__webhook_wetalkie_template_status_update/

RUN uv sync --package prefect_rj_iplanrio --package rj_crm__webhook_wetalkie_template_status_update