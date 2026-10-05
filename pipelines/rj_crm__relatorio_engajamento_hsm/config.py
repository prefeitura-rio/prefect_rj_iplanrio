# -*- coding: utf-8 -*-
"""Constantes da pipeline de relatório de engajamento por HSM."""

from __future__ import annotations

PROJECT_ID = "rj-crm-registry"
DISPAROS_TABLE = f"{PROJECT_ID}.brutos_salesforce_staging.disparos_ativos"
CHATBOT_DATASET = "rmi_conversas"
CHATBOT_TABLE = "chatbot"

BIFROST_BASE_URL = "https://bifrost.iplan.dados.rio"
BIFROST_MODEL = "vertex/gemini-3.6-flash"
MAX_TENTATIVAS_LLM = 3
ESPERA_INICIAL_SEGUNDOS = 2
MAX_OUTPUT_TOKENS_LOTE = 8192

# Os 9 valores abaixo são só o DEFAULT de um parâmetro do flow (ver flow.py) — quem
# rodar/deployar pode sobrescrever sem tocar em código. LOTES_SEM_NOVIDADE_LIMITE e os
# SEED_* continuam fixos de propósito (não foram promovidos): são detalhe de
# implementação (early-stop da descoberta, reprodutibilidade da amostra), não algo que
# faça sentido variar por rodada.
TAMANHO_LOTE_CLASSIFICACAO = 50
TAMANHO_LOTE_DESCOBERTA = 150
TETO_AMOSTRA_DESCOBERTA = 2000
LIMIAR_JULGA_TUDO = 20
PCT_AMOSTRA_CATEGORIA_GRANDE = 30.0
TAMANHO_LOTE_JUIZ = 50
N_EXEMPLOS_POR_CATEGORIA = 5

LOTES_SEM_NOVIDADE_LIMITE = 5
SEED_DESCOBERTA = 42
SEED_AMOSTRA_JUIZ = 42

# CUSTOMER fica de fora de propósito — é cópia crua da 1ª resposta do cidadão que
# AI_AGENT_CIDADAO já repete com timestamp de processamento (mantê-la duplicaria a fala).
FONTES_CONVERSA = ["HSM", "AI_AGENT_CIDADAO", "AI_AGENT_AGENTE"]
ROTULO_FONTE = {"HSM": "HSM", "AI_AGENT_CIDADAO": "CIDADÃO", "AI_AGENT_AGENTE": "AGENTE"}
SENTINELA_SEM_CATEGORIA = "NENHUMA_DO_CATALOGO"
VEREDITOS_VALIDOS = {"CORRETO", "INCORRETO"}

# ID fixo da pasta "Relatórios de Engajamento" no Drive (não busca por nome — evita
# pegar a pasta errada se houver outra com o mesmo nome compartilhada com a service
# account). Extraído de
# https://drive.google.com/drive/folders/1wpZzSMv6dQ5Y50qWIooqRikpkVCh5u2f — a pasta
# precisa estar compartilhada (Editor) com a service account de BASEDOSDADOS_CREDENTIALS_PROD.
DRIVE_PASTA_RAIZ_ID = "1wpZzSMv6dQ5Y50qWIooqRikpkVCh5u2f"
DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder"

CSV_SEP = ";"
