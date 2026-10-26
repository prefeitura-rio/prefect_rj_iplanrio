FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./packages ./packages/

COPY ./pipelines/rj_sms__sisreg_disparo_lembretes ./pipelines/rj_sms__sisreg_disparo_lembretes/

RUN uv sync --package rj_sms__sisreg_disparo_lembretes
