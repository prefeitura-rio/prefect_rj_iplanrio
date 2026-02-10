FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

# Install additional dependencies for audio processing
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libavcodec-extra \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

COPY ./pipelines/rj_crm__api_wetalkie ./pipelines/rj_crm__api_wetalkie/

RUN uv sync --package rj_crm__api_wetalkie
