FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

# Install GEOS library for spatial operations (required by Shapely)
RUN apt-get update && apt-get install -y \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_smas__disparo_cadunico ./pipelines/rj_smas__disparo_cadunico/

RUN uv sync --all-packages