FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

# Roda como root, igual à imagem base (ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest)
# e às demais pipelines do monorepo — nenhuma delas define USER. Adotar usuário sem
# privilégio é mudança transversal (exige chown de /opt/prefect e ajuste do `uv run`,
# que escreve no ambiente), não de uma pipeline só.

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

COPY ./pipelines/rj_iplanrio__czrm_salesforce_datacloud ./pipelines/rj_iplanrio__czrm_salesforce_datacloud/

RUN uv sync --package rj_iplanrio__czrm_salesforce_datacloud
