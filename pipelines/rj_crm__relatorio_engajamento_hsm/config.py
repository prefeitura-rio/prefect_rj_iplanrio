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
# Exemplos de conversa por categoria na coluna "Exemplos de conversas" do Catálogo.
N_EXEMPLOS_DESCOBERTA = 5
SEED_DESCOBERTA = 42
SEED_AMOSTRA_JUIZ = 42

# CUSTOMER fica de fora de propósito — é cópia crua da 1ª resposta do cidadão que
# AI_AGENT_CIDADAO já repete com timestamp de processamento (mantê-la duplicaria a fala).
FONTES_CONVERSA = ["HSM", "AI_AGENT_CIDADAO", "AI_AGENT_AGENTE"]
# Uma sessão (id_sessao_48h) dura até 48h: a extração busca mensagens até esse número
# de dias depois da data de fim, pra sessão do último dia vir inteira (ver
# queries/extrai_conversas.sql).
DIAS_SESSAO = 2
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
DRIVE_SHEET_MIME = "application/vnd.google-apps.spreadsheet"
DRIVE_DOC_MIME = "application/vnd.google-apps.document"
DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

CSV_SEP = ";"

# ============================================================================
# Solicitações (Google Form "Solicitação de Relatório de Engajamento")
# ============================================================================
# O Form grava no Sheet abaixo; o flow LÊ e ESCREVE (status) só nele, pela API do
# Sheets — fonte única. A service account de BASEDOSDADOS_CREDENTIALS_PROD precisa ser
# Editor do Sheet. A tabela externa SOLICITACOES_TABLE (mesmo Sheet) existe só pra
# consulta/histórico no BQ — o flow não depende dela.
SOLICITACOES_TABLE = f"{PROJECT_ID}.brutos_salesforce.solicitacoes_relatorio_engajamento"
SOLICITACOES_SHEET_ID = "15dmce6ztuj9BW2ds44OZ6kZN3VOAPB3dEZ1UEwd36XY"
SOLICITACOES_ABA = "Respostas ao formulário 1"
# Cabeçalhos das colunas no Sheet = títulos exatos das perguntas do Form (renomear uma
# pergunta no Form exige atualizar aqui). As 3 de status são criadas à mão no Sheet.
COL_SHEET_CARIMBO = "Carimbo de data/hora"
COL_SHEET_EMAIL = "Endereço de e-mail"
COL_SHEET_CAMPANHA = "Campanha"
COL_SHEET_TIPO = "Tipo de solicitação"
COL_SHEET_INICIO = "Data de início das conversas"
COL_SHEET_FIM = "Data de fim das conversas"
COL_SHEET_GERACAO = "Data de geração"
COL_SHEET_STATUS = "status"
COL_SHEET_LINK = "link_resultado"
COL_SHEET_PROCESSADO_EM = "processado_em"

TIPO_MATERIAL_VALIDACAO = "Material de validação"
TIPO_RELATORIO = "Relatório de engajamento"
# Relatório rápido: descobre as categorias do zero a cada rodada (sem Catálogo validado
# e sem RAG) — não cria/altera o Catálogo oficial e nunca alimenta o banco de exemplos.
TIPO_RELATORIO_RAPIDO = "Relatório rápido (catálogo automático)"
TIPOS_SOLICITACAO = (TIPO_MATERIAL_VALIDACAO, TIPO_RELATORIO, TIPO_RELATORIO_RAPIDO)

# Valores da coluna status. Vazio = aguardando a data de geração.
STATUS_PROCESSANDO = "processando"
STATUS_CONCLUIDO = "concluido"
PREFIXO_STATUS_PULADO = "pulado"  # "pulado: <motivo>" — não gerou de propósito
PREFIXO_STATUS_ERRO = "erro"  # "erro: <motivo>" — falhou; quem pediu envia o Form de novo
# "processando" há mais que isso = processo morto de fora (OOM, pod reiniciado) sem
# chegar no except — vira erro na execução seguinte. Menor que o intervalo entre as
# execuções (8h -> 18h = 10h) e bem maior que a duração de uma rodada.
HORAS_LIMITE_PROCESSANDO = 6

# Formatos de data do Sheet (localidade Brasil). ISO aceito como alternativa caso a
# localidade do Sheet mude.
FORMATOS_DATA_SHEET = ("%d/%m/%Y", "%Y-%m-%d")
FORMATOS_DATA_HORA_SHEET = ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S")
FORMATO_PROCESSADO_EM = "%d/%m/%Y %H:%M:%S"  # mesmo formato do carimbo do Form
TIMEZONE = "America/Sao_Paulo"

# ============================================================================
# Arquivos no Drive, por campanha:
#   <campanha>/Catálogo
#   <campanha>/exemplos_nao_classificado
#   <campanha>/<data>_<hora>h__disparos_<inicio>_a_<fim>/relatorio_engajamento
#   <campanha>/<data>_<hora>h__disparos_<inicio>_a_<fim>/conversas_classificadas[_revisada_<data>]
#   <campanha>/<data>_<hora>h__disparos_<inicio>_a_<fim>__catalogo_automatico/   (relatório rápido)
#       relatorio_engajamento, conversas_classificadas (sem colunas de revisão), catalogo_automatico
# ============================================================================
ARQUIVO_CATALOGO = "Catálogo"
ARQUIVO_EXEMPLOS = "exemplos_nao_classificado"
ARQUIVO_RELATORIO = "relatorio_engajamento"
ARQUIVO_CONVERSAS = "conversas_classificadas"
ARQUIVO_CATALOGO_AUTOMATICO = "catalogo_automatico"
MARCADOR_REVISADA = "revisada"
# Sufixo da subpasta do relatório rápido — a ingestão do RAG ignora qualquer arquivo
# abaixo de uma pasta com esse sufixo, mesmo que alguém renomeie para "_revisada".
SUFIXO_PASTA_RAPIDO = "__catalogo_automatico"

# Colunas do Catálogo — preenchidas pelo flow na Fase 1 (as 3 primeiras) e pela
# secretaria na validação (as 3 últimas). O flow só LÊ o Catálogo depois de criado.
# Só a Decisão importa pro flow; Justificativa e Data da decisão são de auditoria.
COL_CAT_CATEGORIA = "Categoria"
COL_CAT_DESCRICAO = "Descrição"
COL_CAT_EXEMPLOS = "Exemplos de conversas"
COL_CAT_DECISAO = "Decisão"
COL_CAT_JUSTIFICATIVA = "Justificativa"
COL_CAT_DATA_DECISAO = "Data da decisão"
OPCOES_DECISAO = ("Manter", "Excluir", "Nova")  # ordem da lista suspensa no Sheet
DECISOES_VALIDAS = set(OPCOES_DECISAO)
DECISOES_ATIVAS = {"Manter", "Nova"}

# Colunas de conversas_classificadas geradas pelo flow e lidas de volta na ingestão.
COL_CONV_ID_SESSAO = "id_sessao"
COL_CONV_CATEGORIA = "categoria"
COL_CONV_CONVERSA = "conversa"
# Colunas de revisão em conversas_classificadas — preenchidas pela secretaria.
COL_REVISAO = "revisao"
COL_CATEGORIAS_CORRETAS = "categorias_corretas"
COL_JUSTIFICATIVA_REVISAO = "justificativa_revisao"
REVISAO_CERTO = "Certo"
REVISAO_ERRADO = "Errado"
SEP_CATEGORIAS = ";"  # separador de várias categorias numa mesma célula

# Limite de caracteres por célula do Google Sheets (conversas longas são truncadas).
SHEETS_MAX_CHARS_CELULA = 50000

# ============================================================================
# RAG — banco de conversas revisadas (few-shot da classificação recorrente)
# ============================================================================
RAG_TABLE = f"{PROJECT_ID}.brutos_salesforce.relatorio_engajamento_exemplos_revisados"
# Serafim 100m (português, ~430MB, 768 dim), via sentence-transformers (torch só-CPU).
# Escolhido no benchmark de tmp/avaliacao_embeddings (classificações antigas de
# vacinação e CadÚnico, embedando só as falas do cidadão): 1º lugar nas 2 campanhas em
# precisao@5 — 0.788 no CadÚnico (empate com o mpnet multilíngue, 0.786) e 0.502 na
# vacinação (mpnet 0.420, TF-IDF 0.441). Baixado no build da imagem (ver Dockerfile) —
# trocar aqui exige rebuild e reembedar a tabela RAG (embeddings de modelos diferentes
# não são comparáveis entre si).
EMBEDDING_MODEL = "PORTULAN/serafim-100m-portuguese-pt-sentence-encoder"
# Exemplos revisados por conversa no few-shot. 3 e não mais: o banco começa pequeno
# (os vizinhos mais distantes viram ruído) e a classificação manda 50 conversas por
# chamada — os exemplos do lote são deduplicados, mas o prompt cresce com k.
N_EXEMPLOS_RAG = 3
