FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

RUN apt-get update \
    && apt-get install --no-install-recommends -y git curl gnupg2 libaio1 ca-certificates build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/rj_smtr__example ./pipelines/rj_smtr__example/

RUN uv sync --all-packages
