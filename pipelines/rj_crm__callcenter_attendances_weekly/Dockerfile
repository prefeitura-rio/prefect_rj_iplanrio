FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

# Install additional dependencies for audio processing
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libavcodec-extra \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/

COPY ./pipelines/rj_smas__callcenter_attendances_weekly ./pipelines/rj_smas__callcenter_attendances_weekly/

# Copy also the wetalkie api_handler and audio processing utilities
COPY ./pipelines/rj_crm__api_wetalkie/utils ./pipelines/rj_crm__api_wetalkie/utils/
COPY ./pipelines/rj_crm__api_wetalkie/tasks.py ./pipelines/rj_crm__api_wetalkie/tasks.py

RUN uv sync --all-packages