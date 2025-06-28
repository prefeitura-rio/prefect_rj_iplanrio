FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio/base:latest

RUN mkdir /opt/prefect/prefect_rj_iplanrio/pipelines/rj_sec__load_test

COPY . /opt/prefect/prefect_rj_iplanrio/pipelines/rj_sec__load_test

RUN uv sync --locked
