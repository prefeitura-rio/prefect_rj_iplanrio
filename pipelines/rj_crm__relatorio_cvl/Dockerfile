FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

# Install necessary system dependencies for pandas (if any) and other libraries
RUN apt-get update && apt-get install -y 
    build-essential 
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

COPY ./pipelines/rj_crm__relatorio_cvl ./pipelines/rj_crm__relatorio_cvl/

RUN uv sync --package rj_crm__relatorio_cvl