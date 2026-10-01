FROM alpine:3.22 AS get-instant-client-tools-step

RUN wget -O /tmp/instantclient-tools.zip "https://download.oracle.com/otn_software/linux/instantclient/2118000/instantclient-tools-linux.x64-21.18.0.0.0dbru.zip" \
    && unzip /tmp/instantclient-tools.zip -d /tmp \
    && rm /tmp/instantclient-tools.zip

FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

COPY --from=get-instant-client-tools-step /tmp/instantclient_21_18/sqlldr /opt/oracle/instantclient/sqlldr
ENV PATH="/opt/oracle/instantclient:${PATH}"

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

COPY ./pipelines/rj_smfp__nota_carioca_bq_to_oracle ./pipelines/rj_smfp__nota_carioca_bq_to_oracle/

RUN uv sync --package rj_smfp__nota_carioca_bq_to_oracle
