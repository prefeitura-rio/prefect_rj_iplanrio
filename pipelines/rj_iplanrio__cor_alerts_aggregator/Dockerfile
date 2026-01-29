FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio/base:latest

COPY pipelines/rj_iplanrio__cor_alerts_aggregator /opt/prefect/prefect_rj_iplanrio/pipelines/rj_iplanrio__cor_alerts_aggregator

WORKDIR /opt/prefect/prefect_rj_iplanrio

RUN uv sync --package rj_iplanrio__cor_alerts_aggregator
