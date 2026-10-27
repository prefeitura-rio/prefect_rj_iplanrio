FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:shared

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./packages ./packages/

COPY ./pipelines/rj_crm__disparo_template ./pipelines/rj_crm__disparo_template/
COPY ./pipelines/rj_smas__api_datametrica_agendamentos ./pipelines/rj_smas__api_datametrica_agendamentos/

RUN uv sync --frozen --package rj_smas__api_datametrica_agendamentos --inexact --no-dev --no-cache
