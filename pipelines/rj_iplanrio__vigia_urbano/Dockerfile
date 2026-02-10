FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_iplanrio__vigia_urbano ./pipelines/rj_iplanrio__vigia_urbano/

RUN uv sync --package prefect_rj_iplanrio --package rj_iplanrio__vigia_urbano
