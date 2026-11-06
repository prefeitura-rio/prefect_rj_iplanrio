FROM ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_iplanrio

WORKDIR /opt/prefect/prefect_rj_iplanrio

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/

COPY ./pipelines/rj_crm__relatorio_engajamento_hsm ./pipelines/rj_crm__relatorio_engajamento_hsm/

RUN uv sync --package rj_crm__relatorio_engajamento_hsm

# Modelo de embedding (RAG) baixado no build: o flow não depende do HuggingFace estar
# acessível em runtime nem baixa o modelo a cada execução. O nome vem de config.py,
# então trocar EMBEDDING_MODEL lá já faz o build baixar o modelo novo.
ENV HF_HOME=/opt/hf_cache
RUN uv run --package rj_crm__relatorio_engajamento_hsm python -c \
    "from sentence_transformers import SentenceTransformer; from pipelines.rj_crm__relatorio_engajamento_hsm.config import EMBEDDING_MODEL; SentenceTransformer(EMBEDDING_MODEL, device='cpu')"
# Depois do download, runtime 100% offline: se o modelo não estiver no cache, falha na
# hora em vez de tentar baixar do HuggingFace em produção.
ENV HF_HUB_OFFLINE=1
