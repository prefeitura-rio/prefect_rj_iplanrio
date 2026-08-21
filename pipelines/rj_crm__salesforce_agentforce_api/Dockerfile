FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

# A pipeline rj_crm__salesforce_agentforce_api importa create_date_partitions de rj_crm__disparo_template
COPY ./pipelines/rj_crm__disparo_template ./pipelines/rj_crm__disparo_template/
COPY ./pipelines/rj_crm__salesforce_agentforce_api ./pipelines/rj_crm__salesforce_agentforce_api/

RUN uv sync --package rj_crm__salesforce_agentforce_api
