FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_pgm__divida_ativa ./pipelines/rj_pgm__divida_ativa/

RUN uv sync --package prefect_rj_iplanrio --package rj_pgm__divida_ativa
