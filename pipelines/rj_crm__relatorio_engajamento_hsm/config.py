"""Constantes da pipeline de relatório de engajamento por campanha (ver flow.py)."""

from __future__ import annotations

PROJECT_ID = "rj-crm-registry"
CHATBOT_DATASET = "rmi_conversas"
CHATBOT_TABLE = "chatbot"

BIFROST_BASE_URL = "https://bifrost.iplan.dados.rio"
BIFROST_MODEL = "salesforce/gemini-3.6-flash"
MAX_TENTATIVAS_LLM = 3
ESPERA_INICIAL_SEGUNDOS = 2
MAX_OUTPUT_TOKENS_LOTE = 8192

# Os valores abaixo são só o DEFAULT de um parâmetro do flow (ver flow.py) — quem
# rodar/deployar pode sobrescrever sem tocar em código. LOTES_SEM_NOVIDADE_LIMITE e os
# SEED_* continuam fixos de propósito (não foram promovidos): são detalhe de
# implementação (early-stop da descoberta, reprodutibilidade da amostra), não algo que
# faça sentido variar por rodada.
TAMANHO_LOTE_DESCOBERTA = 150
TETO_AMOSTRA_DESCOBERTA = 2000
# Juiz: avalia 100% das conversas classificadas, em lotes deste tamanho.
TAMANHO_LOTE_JUIZ = 50
N_EXEMPLOS_POR_CATEGORIA = 5
# Classificação: 1 chamada de LLM por conversa, com até esse número de chamadas em
# paralelo (limite pra não estourar o rate limit do Bifrost).
CONCORRENCIA_LLM = 8
# Teto de conversas com resposta em texto (as que vão pra LLM) num relatório (engajamento
# ou rápido). Acima disso o relatório NÃO é gerado: a solicitação fica "aguardando
# liberação" até alguém rodar o flow com a campanha em `campanhas_liberadas` — controle
# explícito do uso de LLM.
TETO_CONVERSAS = 5000
# Teto de conversas avaliadas pelo juiz. Só faz diferença numa campanha liberada acima
# de TETO_CONVERSAS: aí o juiz avalia uma amostra estratificada por categoria (piso de
# PISO_JUIZ_POR_CATEGORIA por categoria, o resto proporcional ao tamanho).
TETO_JUIZ = 2000
PISO_JUIZ_POR_CATEGORIA = 30
SEED_AMOSTRA_JUIZ = 42

LOTES_SEM_NOVIDADE_LIMITE = 5
# Exemplos de conversa por categoria na coluna "Exemplos de conversas" do Catálogo.
N_EXEMPLOS_DESCOBERTA = 5
SEED_DESCOBERTA = 42

# De onde vêm os disparos de cada solicitação (parâmetro fonte_disparos do flow):
#   "campanha" (padrão, Form): disparos da rmi_conversas.chatbot por hsm.nome_campanha;
#   "hsm"      (só custom run): por hsm.nome_hsm.
# A conversa é a mesma nos dois: as sessões receptivas ligadas ao disparo
# (queries/extrai_conversas.sql).
FONTE_DISPAROS_CAMPANHA = "campanha"
FONTE_DISPAROS_HSM = "hsm"
FONTES_DISPAROS = (FONTE_DISPAROS_CAMPANHA, FONTE_DISPAROS_HSM)
ROTULO_FONTE = {"HSM": "HSM", "AI_AGENT_CIDADAO": "CIDADÃO", "AI_AGENT_AGENTE": "AGENTE"}

# Tipo de engajamento: msg_tipo cru do WhatsApp (coluna das queries) -> tipo do relatório.
# Nulo conta como texto. Tipo cru fora do mapa vira TIPO_ENGAJAMENTO_OUTRO.
TIPO_ENGAJAMENTO_TEXTO = "Texto"
TIPO_ENGAJAMENTO_REACAO = "Reação"
TIPO_ENGAJAMENTO_OUTRO = "Outro"
TIPO_ENGAJAMENTO_POR_MSG_TIPO = {
    "text": TIPO_ENGAJAMENTO_TEXTO,
    "button": TIPO_ENGAJAMENTO_TEXTO,  # clique em botão do HSM
    "interactive": TIPO_ENGAJAMENTO_TEXTO,  # escolha em menu/lista
    "audio": "Áudio",
    "image": "Imagem",
    "video": "Vídeo",
    "document": "Documento",
    "contacts": "Contato",
    "sticker": "Figurinha",
    "reaction": TIPO_ENGAJAMENTO_REACAO,
}
# Cada sessão engajada conta 1 vez, no 1º tipo desta ordem que o cidadão usou — assim os
# tipos somam o total de engajados. Só as sessões de texto vão pra LLM classificar.
ORDEM_TIPO_ENGAJAMENTO = [
    TIPO_ENGAJAMENTO_TEXTO,
    "Áudio",
    "Imagem",
    "Vídeo",
    "Documento",
    "Contato",
    TIPO_ENGAJAMENTO_OUTRO,
    "Figurinha",
    TIPO_ENGAJAMENTO_REACAO,
]
N_REACOES_RELATORIO = 10  # emojis listados na distribuição de reações (o resto vira "outras")
# O que a LLM responde quando nenhuma categoria do catálogo descreve a conversa...
SENTINELA_SEM_CATEGORIA = "NENHUMA_DO_CATALOGO"
# ...e como isso aparece no relatório/planilha: uma "categoria" própria, sinal pra
# secretaria criar categoria Nova na revisão.
CATEGORIA_SEM_CATALOGO = "SEM_CATEGORIA_NO_CATALOGO"
# Sentimento do CIDADÃO na conversa, devolvido pela LLM junto com as categorias (mesma
# chamada). Valor fora da lista fica vazio, sem derrubar a classificação; o juiz não
# avalia sentimento. Ordem = ordem no relatório.
SENTIMENTO_POSITIVO = "Positivo"
SENTIMENTO_NEUTRO = "Neutro"
SENTIMENTO_NEGATIVO = "Negativo"
SENTIMENTOS = (SENTIMENTO_POSITIVO, SENTIMENTO_NEUTRO, SENTIMENTO_NEGATIVO)
N_EXEMPLOS_POR_SENTIMENTO = 3  # exemplos de conversa por sentimento no relatório
SEED_EXEMPLOS_SENTIMENTO = 42
# Resposta de 1 conversa (classificação) é curta: categorias + 2 frases.
MAX_OUTPUT_TOKENS_CONVERSA = 1024
# Corte de cada exemplo revisado no prompt da classificação (o few-shot não precisa da
# conversa inteira, e conversas longas inflariam todas as chamadas).
MAX_CHARS_EXEMPLO_RAG = 1500
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
# "aguardando liberação: <n> conversas..." — passou do TETO_CONVERSAS; continua pendente,
# mas só é processada numa execução com a campanha em `campanhas_liberadas`.
PREFIXO_STATUS_AGUARDANDO = "aguardando liberação"
# "processando" há mais que isso = processo morto de fora (OOM, pod reiniciado) sem
# chegar no except — vira erro numa execução seguinte. Maior que o menor intervalo
# entre execuções (8h -> 13h -> 18h = 5h), pra uma rodada longa ainda em andamento não
# ser marcada como travada pela execução seguinte.
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

# Nomes das abas principais dos sheets gerados.
ABA_CATALOGO = "catalogo"
ABA_LEGENDA_CATALOGO = "legenda"
ABA_EXEMPLOS = "exemplos"
ABA_CONVERSAS = "conversas"
ABA_LEGENDA_CONVERSAS = "legenda"
ABA_CATALOGO_AUTOMATICO = "catalogo_automatico"
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
OPCOES_REVISAO = (REVISAO_CERTO, REVISAO_ERRADO)  # lista suspensa da coluna revisao

# Colunas de avaliação do bot — preenchidas manualmente, não usadas pelo classificador.
COL_AVALIACAO_BOT = "avaliacao_resposta_bot"
COL_JUSTIFICATIVA_AVALIACAO_BOT = "justificativa_avaliacao_bot"
AVALIACAO_BOT_BOA = "Boa"
AVALIACAO_BOT_REGULAR = "Regular"
AVALIACAO_BOT_RUIM = "Ruim"
OPCOES_AVALIACAO_BOT = (AVALIACAO_BOT_BOA, AVALIACAO_BOT_REGULAR, AVALIACAO_BOT_RUIM)
# Demais colunas de conversas_classificadas (mesmos nomes do material de exemplo).
COL_CONV_JUSTIFICATIVA = "justificativa"
COL_CONV_RESUMO = "resumo"
COL_CONV_SENTIMENTO = "sentimento"
COL_CONV_JUIZ_VEREDITO = "juiz_veredito"
COL_CONV_JUIZ_ESPERADA = "juiz_categoria_esperada"
COL_CONV_JUIZ_JUSTIFICATIVA = "juiz_justificativa"
SEP_CATEGORIAS = ";"  # separador de várias categorias numa mesma célula

# Limite de caracteres por célula do Google Sheets (conversas longas são truncadas).
SHEETS_MAX_CHARS_CELULA = 50000

# Cores dos cabeçalhos dos Sheets gerados (RGB em [0, 1]).
# Azul: colunas preenchidas pelo flow. Amarelo: colunas preenchidas pela secretaria.
COR_CABECALHO_FLOW = (0.267, 0.490, 0.792)      # azul
COR_CABECALHO_SECRETARIA = (1.0, 0.812, 0.196)  # amarelo

# Cores condicionais da coluna Decisão do Catálogo (fundo da célula por valor).
COR_DECISAO_MANTER = (0.714, 0.843, 0.659)   # verde claro
COR_DECISAO_EXCLUIR = (0.918, 0.600, 0.600)  # vermelho claro
COR_DECISAO_NOVA = (1.0, 0.949, 0.667)       # amarelo claro
COR_AVALIACAO_REGULAR = (0.988, 0.737, 0.427)  # laranja claro

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

# ============================================================================
# Relatórios executivos (segundo Google Form — ver tasks/executivo.py)
# ============================================================================
# O Form "Solicitação de Relatório Executivo de Disparo" grava no Sheet abaixo (mesmo
# padrão do Form de engajamento: o flow LÊ e ESCREVE o status só nele). Cada pedido gera
# 4 PDFs no Drive, no padrão visual de assets/style-guide-relatorios-disparo.md.
EXEC_SOLICITACOES_SHEET_ID = "1NU_W0oCJMWs2EQu4e2D1W3C_YQoRegM6PpWQWgZ45cs"
EXEC_SOLICITACOES_ABA = "Respostas ao formulário 1"
# Cabeçalhos = títulos exatos das perguntas do Form. As 3 de status são criadas à mão.
COL_EXEC_CAMPANHAS = "Campanha(s)"  # caixas de seleção: 1 a MAX_BASES_EXEC, separadas por vírgula
COL_EXEC_INICIO = "Data de início dos disparos"
COL_EXEC_FIM = "Data de fim dos disparos"
COL_EXEC_GERACAO = "Data de geração"
COL_EXEC_OBSERVACOES = "Observações para o relatório (opcional)"
# Etapas do funil ANTES do disparo: não existem no BigQuery (a planilha de destinatários e a
# quarentena do Prefect não são gravadas em tabela) — vêm do Form, opcionais. Valor único
# se houver uma campanha; com várias, "campanha=valor; outra_campanha=valor".
COL_EXEC_LINHAS = "Linhas na planilha (opcional)"
COL_EXEC_DISTINTAS = "Pessoas distintas na planilha (opcional)"
COL_EXEC_INVALIDOS = "Telefones inválidos ou sem telefone (opcional)"
COL_EXEC_VALIDOS_QUERY = "Telefones válidos após a query (opcional)"
COL_EXEC_LIBERADOS = "Telefones liberados após a quarentena no Prefect (opcional)"
MAX_BASES_EXEC = 4

# Disparo "diário" do dashboard (fonte do funil) e fato de conversas (fonte do engajamento).
# Nunca usar enviado_iplan_quantidade: o log do flow de disparo não liga ao disparo real.
DASHBOARD_DATASET = "dashboard_crm_whatsapp"
DASHBOARD_DISPARO_TABLE = "disparo_diario"
DASHBOARD_FALHA_TABLE = "disparo_diario_falha"
# Conversa do cidadão chega até 7 dias depois do HSM (ligação por proximidade na fato).
DIAS_RECEPTIVO_APOS_DISPARO = 7

# Textos da LLM dos relatórios executivos.
EXEC_TEMPERATURA_LLM = 0.2
EXEC_MAX_OUTPUT_TOKENS = 4096
EXEC_MAX_TENTATIVAS_TEXTO = 2  # 1ª tentativa + 1 correção (números fora dos fatos / JSON inválido)
EXEC_ELABORACAO = "IplanRio"
EXEC_EMPRESA_RODAPE = "Empresa Municipal de Tecnologia do Rio de Janeiro"

# Os 4 PDFs de cada pedido: chave -> rótulo no nome do arquivo (style guide, seção 10).
PDF_FUNIL_EXECUTIVO = "funil_executivo"
PDF_FUNIL_DIAGNOSTICO = "funil_diagnostico"
PDF_ENGAJAMENTO_EXECUTIVO = "engajamento_executivo"
PDF_ENGAJAMENTO_DETALHADO = "engajamento_detalhado"
ROTULO_PDF = {
    PDF_FUNIL_EXECUTIVO: "versão executiva",
    PDF_FUNIL_DIAGNOSTICO: "diagnóstico do funil",
    PDF_ENGAJAMENTO_EXECUTIVO: "engajamento, versão executiva",
    PDF_ENGAJAMENTO_DETALHADO: "engajamento e temas das respostas",
}

# Erros da Meta que o style guide (seção 6) nomeia: código numérico da fato -> (nome exato
# da Meta, descrição curta). Os demais códigos aparecem como "Código N" com a descrição do
# seed whatsapp_falha_codigo do projeto dbt.
ERROS_META_PADRAO = {
    "131026": (
        "Message Undeliverable",
        "Número inexistente, sem WhatsApp ativo ou fora de área.",
    ),
    "130472": (
        "Phone number part of an experiment",
        "Número retido pela Meta em grupo de teste ou experimento interno.",
    ),
    "131049": (
        "Healthy ecosystem engagement",
        "Bloqueio temporário de proteção da Meta contra spam ou excesso de envio.",
    ),
}
# Os 2 códigos que contam como "política da Meta" (e não número inválido) no gargalo.
ERROS_META_POLITICA = ("130472", "131049")
ERROS_META_OUTROS = {
    "130429": "Limite de volume de envio atingido.",
    "131048": "Limite de envio por marcação de spam.",
    "131000": "Erro genérico da Meta.",
    "131056": "Muitas mensagens para o mesmo destinatário em pouco tempo.",
    "131047": "Fora da janela de 24 horas, exige template.",
    "131053": "Falha ao baixar ou enviar a mídia.",
    "131009": "Parâmetro inválido no conteúdo da mensagem.",
    "132000": "Número de parâmetros do template não confere.",
    "132001": "Template inexistente ou não aprovado no idioma.",
    "132005": "Texto do template longo demais.",
    "132015": "Template pausado por baixa qualidade.",
    "132016": "Template desativado.",
}
MAX_CODIGOS_ERRO_EXIBIDOS = 5  # os demais viram "Outros códigos"

# Engajamento: critérios dos exemplos de fala (citações) — ver utils/executivo.py.
EXEMPLO_MIN_CHARS = 20
EXEMPLO_MAX_CHARS = 200
EXEMPLO_MIN_LETRAS = 8  # fala só com emoji/símbolo não vira citação
SENTIMENTO_ROTULO_MIN_PCT = 7.0  # abaixo disso o segmento da barra é estreito demais pro rótulo
N_EXEMPLOS_EXECUTIVO = 4  # citações na versão executiva do engajamento
N_EXEMPLOS_ELOGIO_DETALHADO = 5
N_EXEMPLOS_CRITICA_DETALHADO = 10
N_CATEGORIAS_EXECUTIVO = 8  # linhas por base na versão executiva
SENTIMENTO_BQ = {"positivo": SENTIMENTO_POSITIVO, "neutro": SENTIMENTO_NEUTRO, "negativo": SENTIMENTO_NEGATIVO}
