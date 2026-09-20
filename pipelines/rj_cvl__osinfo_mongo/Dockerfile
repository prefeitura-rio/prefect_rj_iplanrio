FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pipelines/rj_cvl__osinfo_mongo/pyproject.toml ./pipelines/rj_cvl__osinfo_mongo/uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_cvl__osinfo_mongo ./pipelines/rj_cvl__osinfo_mongo/

RUN uv sync
