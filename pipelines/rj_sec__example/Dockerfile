FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio/base:latest

RUN mkdir /opt/prefect/prefect_rj_iplanrio/pipelines/rj_sec__example

COPY . /opt/prefect/prefect_rj_iplanrio/pipelines/rj_sec__example

RUN uv sync
