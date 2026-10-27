FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:shared

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./packages ./packages/

COPY ./pipelines/rj_crm__disparo_template ./pipelines/rj_crm__disparo_template/
COPY ./pipelines/rj_crm__salesforce_api ./pipelines/rj_crm__salesforce_api/

RUN uv sync --frozen --package rj_crm__salesforce_api --inexact --no-dev --no-cache
