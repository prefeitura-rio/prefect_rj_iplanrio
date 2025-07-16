FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio/base:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pipelines/rj_iplan__equipamentos_arcgis ./pipelines/rj_iplan__equipamentos_arcgis/

RUN uv sync
