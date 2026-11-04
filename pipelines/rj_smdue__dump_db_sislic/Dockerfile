FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:shared

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./packages ./packages/

COPY ./pipelines/rj_smdue__dump_db_sislic ./pipelines/rj_smdue__dump_db_sislic/

RUN uv sync --frozen --package rj_smdue__dump_db_sislic --inexact --no-dev --no-cache
