FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_pic__disparos_email ./pipelines/rj_pic__disparos_email/

RUN uv sync --all-packages
