FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./packages ./packages/

COPY ./pipelines/rj_cor__precipitacao_inea ./pipelines/rj_cor__precipitacao_inea/

RUN cp ./pipelines/rj_cor__precipitacao_inea/inea-ca.pem /usr/local/share/ca-certificates/inea.crt \
    && update-ca-certificates
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

RUN uv sync --package rj_cor__precipitacao_inea
