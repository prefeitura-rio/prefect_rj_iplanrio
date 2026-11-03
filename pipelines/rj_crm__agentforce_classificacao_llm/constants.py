# -*- coding: utf-8 -*-
"""
Constantes para a pipeline de classificação inicial via LLM das sessões do
Agentforce (WhatsApp).

Portada de `clustering/classificacao_inicial.ipynb` (repo de análise) para produção.
As outras 3 etapas do pipeline de clusterização (taxonomia, taxonomia_motivos,
validacao_taxonomia) continuam só no repo de análise por enquanto.
"""

from enum import Enum


class ClassificacaoConstants(Enum):
    # Projeto GCP onde tudo (fonte e destino) vive
    BQ_PROJECT_ID = "rj-crm-registry"

    # --- Fonte ---
    # fct_chatbot (v1, uma linha por mensagem, todas as integrações) — a query agrupa por
    # id_sessao_receptivo, a sessão do agente calculada no dbt, que já traz o HSM ligado a
    # ela com o texto renderizado (ver cabeçalho de queries/extract_sessoes.sql). Sessão
    # encerrada = 2 h sem mensagem.
    SOURCE_TABLE = "rj-crm-registry.rmi_conversas.chatbot"

    # --- Destino ---
    # Mesmo dataset das tabelas ai_agent_session/ai_agent_interaction (brutos_salesforce),
    # a pedido — mantém a classificação junto da fonte que ela deriva.
    DEST_DATASET_ID = "brutos_salesforce"
    DEST_TABLE_ID = "ai_agent_session_classificacao"
    # Tabela tmp intermediária (append + MERGE + truncate a cada execução) — criada
    # manualmente uma vez (DDL enviado à parte), não pela pipeline (ver tasks/load.py).
    DEST_TMP_TABLE_ID = "ai_agent_session_classificacao_tmp"

    # Catálogo de regras (funções Python) de tema/motivo — gerenciado por fora desta
    # pipeline: promovido manualmente do repo clustering depois de validado (ver
    # tasks/taxonomia.py). Mesmo dataset da tabela destino.
    TAXONOMIA_REGRAS_TABLE_ID = "taxonomia_regras"
    # Nível de agregação das regras (coluna agrupamento_tipo do catálogo): hierarquia
    # agrupamento > tema > motivo. Trocar o nível = promover regras com outro tipo e
    # mapear a coluna da sessão em tasks/taxonomia.py::COLUNA_SESSAO_POR_AGRUPAMENTO.
    TAXONOMIA_AGRUPAMENTO_TIPO = "macrotema"
    TAXONOMIA_ETAPA_TEMA = "tema"
    # Catálogo ainda não tem nenhuma regra com esta etapa — carrega_catalogo_regras já
    # trata "sem regra" como no-op (não bloqueia nada), então já ligar essa etapa nos
    # dois fluxos (normal e recalcula_taxonomia) é seguro hoje e evita precisar mexer
    # em flow.py de novo quando a primeira regra de motivo for promovida.
    TAXONOMIA_ETAPA_MOTIVO = "motivo"

    # --- Recálculo de taxonomia (parâmetro recalcula_taxonomia do flow) ---
    # tema_nome/causa_nome também chegam ao struct `llm` da chatbot (dbt, repo
    # queries-rj-crm-registry), cujos incrementais só reescrevem o passado recente —
    # depois do MERGE parcial, o flow dispara o full refresh do ramo Salesforce pelo
    # deployment do rj_crm__run_dbt (ver tasks/taxonomia.py::dispara_full_refresh_chatbot).
    # O select parte do modelo que faz o left join com a tabela de classificação e não
    # inclui o ramo da fct_chatbot_v2.
    # <flow>/<deployment>: o Prefect troca "_" por "-" no nome do flow (função rj_crm__run_dbt)
    DBT_DEPLOYMENT_NAME = "rj-crm--run-dbt/rj-crm--run_dbt--prod"
    DBT_FULL_REFRESH_SELECT = "int_chatbot_salesforce_sessao_receptivo+"
    DBT_GITHUB_REPO = "https://GITHUB_TOKEN@github.com/prefeitura-rio/queries-rj-crm-registry.git"
    DBT_BIGQUERY_PROJECT = "rj-crm-registry"

    # --- Extração incremental ---
    # Janela fixa de busca — sem parametrização/watermark por decisão de projeto: dois
    # filtros de "já classificada" garantem idempotência (pré-filtro barato por
    # llm.classificacao_datahora na fonte + anti-join contra a tabela destino como garantia
    # final, que cobre o desincronismo de até 15min entre nossa gravação e o chatbot
    # reprocessar — ver queries/extract_sessoes.sql); a janela fixa só limita quanto
    # histórico é reescaneado a cada execução. Sessão que fica sem classificar por mais de
    # LOOKBACK_DAYS (pipeline fora do ar por muito tempo) não é mais pega automaticamente —
    # risco aceito, mitigado por alerta de falha.
    LOOKBACK_DAYS = 7

    # --- LLM (Bifrost / Gemini) ---
    BIFROST_BASE_URL = "https://bifrost.iplan.dados.rio"
    BIFROST_MODEL = "vertex/gemini-3.6-flash"
    MAX_WORKERS = 8  # mesmo valor usado no notebook (ajustar conforme rate limit do gateway)
    MAX_TENTATIVAS_LLM = 3
    ESPERA_INICIAL_SEGUNDOS = 2

    # Tamanho do lote de carga incremental dentro de classifica_sessoes: a cada N sessões
    # classificadas com sucesso, já sobe pro BigQuery (tmp + MERGE) em vez de esperar o
    # lote inteiro terminar. Um lote de 6mil sessões pode levar 20-30min de chamadas
    # pagas à LLM — sem isso, um crash no meio (pod reiniciado, OOM) perde TUDO que já
    # foi classificado, porque nada tinha sido persistido ainda. Com carga incremental,
    # um crash perde só o lote parcial em memória no momento (no máximo TAMANHO_LOTE_CARGA
    # sessões). 500 é um meio-termo: overhead de N/500 MERGEs extras é desprezível perto do
    # tempo de LLM, e o "raio de perda" fica pequeno.
    TAMANHO_LOTE_CARGA = 500

    # Valores de `classificacao` (exposta como llm.relacao_hsm_tipo no chatbot) —
    # a coluna cobre TODA sessão, não só a que passou pelo prompt com_hsm:
    #   DENTRO_DO_ESCOPO / FORA_DO_ESCOPO / MISTO -> classificado pela LLM (tem HSM)
    #   SEM_HSM_ASSOCIADO                          -> não tem HSM pra comparar; LLM
    #                                                  ainda roda (prompt sem_hsm),
    #                                                  só não avalia escopo
    #   RESPOSTA_ATRASADA_BTN                      -> decidido por regra, sem LLM
    # Sessão decidida por regra não passa por modelo nenhum — `modelo` fica null
    # nesse caso (não usar um valor tipo "regra:..." ali, essa informação já está
    # inteira em `classificacao`).
    CLASSIFICACAO_SEM_HSM_ASSOCIADO = "SEM_HSM_ASSOCIADO"
    CLASSIFICACAO_RESPOSTA_ATRASADA_BTN = "RESPOSTA_ATRASADA_BTN"
    JUSTIFICATIVA_RESPOSTA_ATRASADA_BTN = (
        "Resposta do usuário é idêntica ao texto de um botão do HSM, mas chegou fora "
        "da janela de 24h (não foi capturada como clique de botão)."
    )

    # Versão do conjunto de prompts — sobe manualmente quando os .txt em ./prompts mudam
    # de forma que altera o significado da classificação (não a cada typo). Usada só
    # para auditoria (coluna prompt_versao); reclassificação de sessões antigas é MERGE
    # (sobrescreve), não versionamento histórico.
    # v2 (2026-08-25): rótulo "Informação" separado de "Solicitação" — antes, sessão que só
    # avisava/justificava algo (ex.: "vou remarcar online", sem pedir nada à prefeitura)
    # virava Solicitação por engano. ~1713 sessões classificadas em v1 têm Solicitação no
    # sentimento e são candidatas a reclassificação (ver plano combinado com o time).
    # v3 (2026-09-08): item 8 novo no prompt com_hsm — telefone_confirmacao_flag
    # (PERTENCE/INDEFINIDO/NAO_PERTENCE), se a resposta do cidadão indica que o número
    # que recebeu o HSM é dele. Prompt validado antes num reprocessamento retroativo
    # avulso (ver quick/telefone_confirmacao_retroativo, repo queries-rj-crm-registry) —
    # essa pipeline só passa a preencher telefone_confirmacao_flag/_datahora pra sessão
    # NOVA a partir daqui; o histórico já preenchido veio de lá, não desta pipeline.
    # v4 (2026-10-21): secretarias saem do prompt principal e vão pra um prompt próprio
    # (ver PROMPT_VERSAO_SECRETARIA) — itens renumerados, telefone_confirmacao_flag passa
    # a ser o item 7 do com_hsm.
    PROMPT_VERSAO = "v4"

    # Prompts de roteamento — rodam só em sessão com conteudo_relevante=true, depois do
    # prompt principal, cada um com sua versão (coluna própria na tabela destino) pra
    # poder reprocessar só ele quando a taxonomia mudar. A taxonomia em si (nomes +
    # descrições) fica em ./taxonomias/*.json e é renderizada no prompt em runtime — a
    # mesma lista é usada pra validar o rótulo que a LLM devolve. Subir a versão quando
    # o JSON ou o .txt mudar de forma que altera o significado da classificação.
    # v1 (2026-10-21): macrotemas da planilha de taxonomia (aba Macrotemas) + "Outros";
    # secretarias da aba Núcleo, mantendo a grafia do catálogo taxonomia_regras pras 7
    # secretarias originais (Comlurb, RioLuz, Seconserva...) — senão as regras de tema
    # param de bater (match por string exata em tasks/taxonomia.py::_aplica_regras).
    # v2 secretaria (2026-11-03): SUBGGC (Fazenda e Planejamento) entra pra remuneração e
    # vida funcional do servidor ativo, e a PREVI-RIO fica só com previdência e os auxílios
    # do instituto — na v1, salário/bônus de servidor ativo caía na PREVI-RIO por causa do
    # "calendário de pagamento" na descrição dela. As sessões já classificadas na v1 foram
    # migradas por UPDATE (PREVI-RIO → SUBGGC, menos as de aposentadoria/pensão/auxílios).
    # v2 macrotema (2026-11-03): nomes sem vírgula nem " e " (viraram " / ", ex.:
    # "Trabalho / renda / qualificação"). Os relacionados são separados por vírgula, e
    # nome com vírgula era quebrado na validação e descartado. As sessões da v1 foram
    # renomeadas por UPDATE; o secundário que já tinha sido descartado não volta.
    PROMPT_VERSAO_MACROTEMA = "v2"
    PROMPT_VERSAO_SECRETARIA = "v2"
