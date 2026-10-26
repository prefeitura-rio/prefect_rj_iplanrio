FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./packages ./packages/

COPY ./pipelines/rj_crm__disparo_template ./pipelines/rj_crm__disparo_template/
COPY ./pipelines/rj_smas__disparo_cadunico ./pipelines/rj_smas__disparo_cadunico/

RUN uv sync --package rj_smas__disparo_cadunico
