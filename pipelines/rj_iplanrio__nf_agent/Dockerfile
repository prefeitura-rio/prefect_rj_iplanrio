FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

# System dependency for pdf2image (PDF → image conversion)
RUN apt-get update && apt-get install -y --no-install-recommends poppler-utils git && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

COPY ./pipelines/rj_iplanrio__nf_agent ./pipelines/rj_iplanrio__nf_agent/

RUN git clone https://github.com/prefeitura-rio/agent-nf-validator.git /opt/agent-nf-validator

RUN uv sync --package rj_iplanrio__nf_agent

# google-generativeai requires protobuf<6.0 which conflicts with the workspace's
# grpcio-status==1.78.0 (protobuf>=6.31.1). Install it isolated via pip --target
# so it doesn't interfere with uv's dependency resolution.
RUN pip install --target /opt/agent-packages google-generativeai>=0.4.0

ENV PYTHONPATH="/opt/agent-nf-validator:/opt/agent-packages:${PYTHONPATH}"
