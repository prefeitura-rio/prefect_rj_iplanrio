# Build this from the prefect_rj_iplanrio workspace root:
#   docker build -f pipelines/rj_iplanrio__nf_agent/Dockerfile .

FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

# System dependency for pdf2image (PDF → image conversion)
RUN apt-get update && apt-get install -y --no-install-recommends poppler-utils git && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio
COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./pipelines/rj_iplanrio__nf_agent ./pipelines/rj_iplanrio__nf_agent/
RUN uv sync --package rj_iplanrio__nf_agent

RUN git clone https://github.com/prefeitura-rio/agent-nf-validator.git /opt/agent-nf-validator
RUN uv pip install -r /opt/agent-nf-validator/requirements.txt

ENV PYTHONPATH="/opt/agent-nf-validator:${PYTHONPATH}"
