FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

COPY ./pipelines/rj_crm__relatorio_cvl ./pipelines/rj_crm__relatorio_cvl/

COPY ./pipelines/rj_crm__disparo_template ./pipelines/rj_crm__disparo_template/

RUN uv sync --package rj_crm__relatorio_cvl