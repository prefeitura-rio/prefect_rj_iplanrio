FROM alpine:3.22 AS get-instant-client-step

RUN wget -O /tmp/instantclient.zip "https://download.oracle.com/otn_software/linux/instantclient/2118000/instantclient-basiclite-linux.x64-21.18.0.0.0dbru.zip" \
    && unzip /tmp/instantclient.zip -d /tmp \
    && rm /tmp/instantclient.zip

FROM alpine:3.22 AS manifests

COPY ./pipelines /pipelines
RUN find /pipelines -type f ! -name pyproject.toml -delete \
    && find /pipelines -type d -empty -delete

FROM python:3.14-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update \
    && apt-get install --no-install-recommends -y git ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock ./
COPY ./src ./src/
COPY ./packages ./packages/
COPY --from=manifests /pipelines ./pipelines/

ARG HEAVY_PIPELINE=rj_crm__modelo_qualidade_telefone
RUN set -e; \
    names() { uv export --frozen --package "$1" --no-dev --no-hashes --no-emit-workspace --no-header | sed -n 's/^\([A-Za-z0-9_.-]*\)==.*/\1/p' | tr 'A-Z_' 'a-z-'; }; \
    for member in $(ls pipelines); do [ "$member" = "$HEAVY_PIPELINE" ] || names "$member"; done | sort -u > /tmp/shared; \
    names "$HEAVY_PIPELINE" | sort -u | comm -23 - /tmp/shared | sed 's/^/--no-install-package /' > /tmp/skip; \
    uv sync --frozen --all-packages --no-install-workspace --no-dev --no-cache $(cat /tmp/skip) \
    && find .venv -type d \( -name tests -o -name test \) -prune -exec rm -rf {} + \
    && find .venv -name "*.pyi" -delete \
    && rm -f .venv/bin/dbt-core-experimental-parser \
    && rm -rf .venv/lib/python3.14/site-packages/google/cloud/aiplatform* \
    && rm -f .venv/lib/python3.14/site-packages/pyarrow/*flight* \
    && find .venv/lib/python3.14/site-packages/babel/locale-data -type f ! -name "root.dat" ! -name "en*.dat" ! -name "pt*.dat" -delete \
    && find .venv/lib/python3.14/site-packages/googleapiclient/discovery_cache/documents -type f ! -name "drive.v3.json" -delete

ARG DUCKDB_VERSION=1.5.6
RUN uv run --no-project --with "duckdb==${DUCKDB_VERSION}" python -c "\
import duckdb; \
connection = duckdb.connect(); \
[connection.execute(f'INSTALL {name}') for name in ('postgres_scanner', 'spatial')]; \
[connection.execute(f'INSTALL {name} FROM community') for name in ('mssql', 'bigquery', 'gsheets')]"

FROM python:3.14-slim-bookworm

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY --from=get-instant-client-step /tmp/instantclient_21_18 /opt/oracle/instantclient
COPY ./openssl.cnf /etc/ssl/openssl.cnf

RUN apt-get update \
    && apt-get install --no-install-recommends -y git libaio1 libgomp1 ca-certificates openssl \
    && sh -c "echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf" \
    && ldconfig \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY --from=builder /opt/prefect/prefect_rj_iplanrio /opt/prefect/prefect_rj_iplanrio
COPY --from=builder /root/.duckdb /root/.duckdb
