FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_iplan__issue_postfix_certificate ./pipelines/rj_iplan__issue_postfix_certificate/

RUN uv sync --all-packages

RUN apt-get update && apt-get install -y --no-install-recommends \
    certbot \
    && rm -rf /var/lib/apt/lists/*