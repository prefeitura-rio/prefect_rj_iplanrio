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
    # View consolidada de sessões do Agentforce — já é a verdade por trás de
    # ai_agent_session/ai_agent_interaction (fonte do pipeline rj_crm__salesforce_agentforce_api).
    # Não fazemos checagem extra de completude aqui: se `fim_datahora` está preenchido,
    # consideramos a sessão pronta pra classificar.
    SOURCE_TABLE = "rj-crm-registry.rmi_conversas.v2_chatbot_conversas"
    # Catálogo de HSMs por jornada, usado para enriquecer a sessão com o texto do HSM
    # e decidir se a sessão é uma "resposta atrasada a botão" (não vai pra LLM).
    HSM_CATALOG_TABLE = "rj-crm-registry.intermediario_crm_whatsapp.v2_atividades_enriquecidas_hsm"

    # --- Destino ---
    # Mesmo dataset das tabelas ai_agent_session/ai_agent_interaction (brutos_salesforce),
    # a pedido — mantém a classificação junto da fonte que ela deriva.
    DEST_DATASET_ID = "brutos_salesforce"
    DEST_TABLE_ID = "ai_agent_session_classificacao"
    # Catálogo de regras (funções Python) de tema/motivo — gerenciado por fora desta
    # pipeline: promovido manualmente do repo clustering depois de validado (ver
    # tasks/taxonomia.py). Mesmo dataset da tabela destino.
    TAXONOMIA_REGRAS_TABLE_ID = "taxonomia_regras"
    TAXONOMIA_ETAPA_TEMA = "tema"

    # --- Extração incremental ---
    # Janela fixa de busca — sem parametrização/watermark por decisão de projeto: dois
    # filtros de "já classificada" garantem idempotência (pré-filtro barato por
    # classificacao_llm_datahora na fonte + anti-join contra a tabela destino como garantia
    # final, que cobre o desincronismo de até 15min entre nossa gravação e o fct_chatbot_v2
    # reprocessar — ver queries/extract_sessoes.sql); a janela fixa só limita quanto
    # histórico é reescaneado a cada execução. Sessão que fica sem classificar por mais de
    # LOOKBACK_DAYS (pipeline fora do ar por muito tempo) não é mais pega automaticamente —
    # risco aceito, mitigado por alerta de falha.
    LOOKBACK_DAYS = 14
    # Janela de busca por HSM anterior à sessão (mesmo valor do notebook original)
    HSM_MAX_DIAS_ANTES = 7

    # --- LLM (Bifrost / Gemini) ---
    BIFROST_BASE_URL = "https://bifrost.iplan.dados.rio"
    BIFROST_MODEL = "vertex/gemini-3.6-flash"
    MAX_WORKERS = 8  # mesmo valor usado no notebook (ajustar conforme rate limit do gateway)
    MAX_TENTATIVAS_LLM = 3
    ESPERA_INICIAL_SEGUNDOS = 2

    # Valores de `classificacao` (exposta como escopo_hsm_tipo no fct_chatbot_v2) —
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
    PROMPT_VERSAO = "v1"
