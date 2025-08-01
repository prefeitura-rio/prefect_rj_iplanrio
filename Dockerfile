FROM alpine:3.22 AS get-instant-client-step

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

RUN wget -O /tmp/instantclient.zip "https://download.oracle.com/otn_software/linux/instantclient/2118000/instantclient-basic-linux.x64-21.18.0.0.0dbru.zip" && unzip /tmp/instantclient.zip -d /tmp

FROM prefecthq/prefect:3.4.3-python3.13@sha256:684329aa8a737b2505301118d220eb4fabdb0967e7ece5eeb2deb7909e9d309f

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

COPY --from=get-instant-client-step /tmp/instantclient_21_18 /opt/oracle/instantclient

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY ./openssl.cnf /etc/ssl/openssl.cnf

RUN apt-get update && \
    apt-get install --no-install-recommends -y git curl gnupg2 libaio1 && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    echo "deb [arch=amd64,arm64,armhf] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install --no-install-recommends -y ffmpeg libsm6 libxext6 msodbcsql17 msodbcsql18 openssl unixodbc-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    sh -c "echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

RUN mkdir pipelines
