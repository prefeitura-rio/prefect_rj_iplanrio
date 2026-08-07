FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

# poppler-utils: system dependency for pdf2image (PDF → image conversion).
# git: required by uv to resolve the pinned agent-nf-validator git dependency below.
RUN apt-get update && apt-get install -y --no-install-recommends poppler-utils git && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

COPY ./pipelines/rj_iplanrio__nf_agent ./pipelines/rj_iplanrio__nf_agent/

# agent-nf-validator is resolved as a pinned, lockfile-audited git dependency
# (see [tool.uv.sources] in pipelines/rj_iplanrio__nf_agent/pyproject.toml),
# not an untracked clone — the exact commit and its transitive deps are
# recorded in uv.lock and reviewable in PR diffs.
RUN uv sync --package rj_iplanrio__nf_agent

# google-generativeai is excluded from agent-nf-validator's default deps
# (extra "gemini") because it requires protobuf<6.0, which conflicts with the
# workspace's grpcio-status==1.78.0 (protobuf>=6.31.1) — see TROUBLESHOOTING.md #4.
# Installed isolated via pip --target so it doesn't interfere with uv's resolution.
RUN pip install --target /opt/agent-packages google-generativeai>=0.4.0

ENV PYTHONPATH="/opt/agent-packages:${PYTHONPATH}"
