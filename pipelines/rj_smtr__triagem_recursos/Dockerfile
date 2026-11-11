FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:shared

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-por \
    libgomp1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./packages ./packages/
COPY ./pipelines/rj_smtr__triagem_recursos ./pipelines/rj_smtr__triagem_recursos/

RUN uv sync --frozen --package rj-smtr-triagem-recursos --inexact --no-dev --no-cache

ENV HF_HOME=/opt/prefect/hf_cache \
    SENTENCE_TRANSFORMERS_HOME=/opt/prefect/hf_cache

RUN uv run --no-sync --package rj-smtr-triagem-recursos python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')" \
    && chmod -R a+rX /opt/prefect/hf_cache
