FROM alpine:3.22 AS get-instant-client-step

RUN wget -O /tmp/instantclient.zip "https://download.oracle.com/otn_software/linux/instantclient/2118000/instantclient-basic-linux.x64-21.18.0.0.0dbru.zip" && unzip /tmp/instantclient.zip -d /tmp

FROM prefecthq/prefect:3.4.3-python3.13

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

COPY --from=get-instant-client-step /tmp/instantclient_21_18 /opt/oracle/instantclient

RUN apt-get update && \
    apt-get install --no-install-recommends -y git curl gnupg2 libaio1 && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    echo "deb [arch=amd64,arm64,armhf] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install --no-install-recommends -y ffmpeg libsm6 libxext6 msodbcsql18 openssl unixodbc-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    sh -c "echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig

COPY ./openssl.cnf /etc/ssl/openssl.cnf

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /opt/prefect/prefect_rj_iplanrio/

COPY ./pyproject.toml ./uv.lock ./

RUN mkdir pipelines

RUN uv sync
