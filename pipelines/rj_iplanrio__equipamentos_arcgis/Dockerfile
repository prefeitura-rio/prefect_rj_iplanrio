FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pipelines/rj_iplanrio__equipamentos_arcgis ./pipelines/rj_iplanrio__equipamentos_arcgis/

RUN uv sync
