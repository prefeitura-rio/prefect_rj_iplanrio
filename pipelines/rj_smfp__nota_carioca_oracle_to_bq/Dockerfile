FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

COPY ./pipelines/rj_smfp__nota_carioca_oracle_to_bq ./pipelines/rj_smfp__nota_carioca_oracle_to_bq/

RUN uv sync --package rj_smfp__nota_carioca_oracle_to_bq
