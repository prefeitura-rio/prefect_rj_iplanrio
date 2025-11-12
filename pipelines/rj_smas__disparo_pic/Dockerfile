FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

# Install GEOS library and build dependencies for spatial operations (required by Shapely)
RUN apt-get update && apt-get install -y \
    libgeos-dev \
    libgeos-c1v5 \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_smas__disparo_pic ./pipelines/rj_smas__disparo_pic/

COPY ./pipelines/rj_crm_registry__disparo_template ./pipelines/rj_crm_registry__disparo_template/

RUN uv sync --all-packages
