FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:shared

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./packages ./packages/

COPY ./pipelines/rj_smfp__nota_carioca_bq_to_oracle_validation ./pipelines/rj_smfp__nota_carioca_bq_to_oracle_validation/

COPY ./pipelines/rj_smfp__nota_carioca_bq_to_oracle ./pipelines/rj_smfp__nota_carioca_bq_to_oracle/

RUN uv sync --frozen --package rj_smfp__nota_carioca_bq_to_oracle_validation --inexact --no-dev --no-cache
