FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-por \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio/

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./pipelines/rj_smtr__triagem_recursos ./pipelines/rj_smtr__triagem_recursos/

RUN uv sync --package rj-smtr-triagem-recursos