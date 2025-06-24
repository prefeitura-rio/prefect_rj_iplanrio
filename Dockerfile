# Stage #1: Get Oracle Instant Client
FROM curlimages/curl:7.81.0 as curl-step
ARG ORACLE_INSTANT_CLIENT_URL=https://download.oracle.com/otn_software/linux/instantclient/215000/instantclient-basic-linux.x64-21.5.0.0.0dbru.zip
RUN curl -sSLo /tmp/instantclient.zip $ORACLE_INSTANT_CLIENT_URL

# Stage #2: Unzip Oracle Instant Client
FROM ubuntu:18.04 as unzip-step
COPY --from=curl-step /tmp/instantclient.zip /tmp/instantclient.zip
RUN apt-get update && \
    apt-get install --no-install-recommends -y unzip && \
    rm -rf /var/lib/apt/lists/* && \
    unzip /tmp/instantclient.zip -d /tmp

# Stage #3: Final stage
FROM prefecthq/prefect:3.4.3-python3.13
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install a few dependencies and setup oracle instant client
WORKDIR /opt/oracle
COPY --from=unzip-step /tmp/instantclient_21_5 /opt/oracle/instantclient_21_5
RUN apt-get update && \
    apt-get install --no-install-recommends -y git curl gnupg2 libaio1 && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    echo "deb [arch=amd64,arm64,armhf] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install --no-install-recommends -y ffmpeg libsm6 libxext6 msodbcsql18 openssl unixodbc-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    sh -c "echo /opt/oracle/instantclient_21_5 > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig
COPY ./openssl.cnf /etc/ssl/openssl.cnf

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy the project
COPY . /opt/prefect/prefect_rj_iplanrio/
WORKDIR /opt/prefect/prefect_rj_iplanrio/

# Install dependencies
RUN uv sync
