-- Sessões do Agentforce (WhatsApp) dos últimos {lookback_days} dias que AINDA NÃO têm
-- classificação — extração incremental de janela fixa, sem watermark: a janela fixa
-- limita o quanto reescaneamos a cada rodada. Traz também a HSM disparada mais próxima
-- (e anterior) ao início da sessão, com no máximo hsm_max_dias_antes dias de antecedência.
-- Fonte: rj-crm-registry.rmi_conversas.v2_chatbot_conversas (fct_chatbot_v2) — já é a
-- verdade consolidada por trás de ai_agent_session/ai_agent_interaction, e já carrega
-- classificacao_llm_datahora via LEFT JOIN em ai_agent_session_classificacao (nossa
-- própria tabela destino, ver fct_chatbot_v2.sql no repo queries-rj-crm-registry).
--
-- DOIS filtros de "já classificada", de propósito, não um só:
--   1. classificacao_llm_datahora IS NULL (abaixo, em sessoes_usuario) — pré-filtro barato,
--      direto na leitura de source_table, sem join adicional aqui. Corta a esmagadora
--      maioria antes do agrupamento/pareamento com HSM, mas pode estar desatualizado até
--      15min (fct_chatbot_v2 roda quarter_hourly) — não é garantia sozinho.
--   2. Anti-join contra a tabela destino, no final — é o que garante correção de verdade:
--      cobre a janela de desincronismo entre a nossa gravação e o fct_chatbot_v2 ainda não
--      ter reprocessado a partição, sem o que reclassificaríamos sessão já feita há pouco.
-- Usa classificacao_llm_datahora e não escopo_hsm_tipo pro filtro 1: esse último pode
-- ficar null mesmo numa sessão já processada (JSON incompleto da LLM, nullable de
-- propósito pra não travar a carga do lote — ver tasks/load.py).
--
-- O anti-join (filtro 2) também tem corte de data_particao no destino (dentro do ON, não
-- do WHERE — no WHERE quebraria a semântica de anti-join, viraria INNER JOIN de fato).
-- Mesmo raciocínio do pré-filtro 1 e do corte em fct_chatbot_v2.sql: classificação só
-- acontece depois da sessão, nunca antes, então toda linha do destino relevante pra essa
-- janela tem data_particao >= data_inicio — sem isso, o anti-join escanearia a tabela
-- destino inteira (sem TTL, cresce sem limite) a cada execução.
--
-- Adaptada de clustering/queries/query_concat_agentforce.sql (repo de análise): mesma
-- lógica, trocando a data de início fixa por uma janela rolante e somando o pré-filtro 1
-- ao anti-join que já existia.
--
-- sessoes_usuario agrupa só por id_sessao (com ANY_VALUE nas demais colunas), não por
-- id_sessao + telefone + cpf + nome_cidadao + datas: essas colunas são documentadas como
-- repetidas/estáveis por sessão na fonte, mas isso é suposição sobre o dado, não garantia
-- do banco — se alguma divergir entre mensagens da mesma sessão, um GROUP BY com todas
-- geraria 2 linhas pra 1 id_sessao, e isso propagaria (LLM chamada em dobro, MERGE final
-- falhando com "must match at most one source row"). Agrupar só por id_sessao garante o
-- grão certo estruturalmente, não por suposição.

DECLARE data_inicio DATETIME DEFAULT DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL {lookback_days} DAY);
DECLARE data_fim DATETIME DEFAULT CURRENT_DATETIME('America/Sao_Paulo');
DECLARE hsm_max_dias_antes INT64 DEFAULT {hsm_max_dias_antes};

WITH

-- sessões do agente: 1 linha por id_sessao, mensagens do cidadão (só usuário,
-- sem as respostas do agente) concatenadas na ordem em que ocorreram
sessoes_usuario AS (
    SELECT
        id_interacao AS id_sessao,
        ANY_VALUE(contato.contato_telefone) AS telefone,
        ANY_VALUE(contato.cpf) AS cpf,
        ANY_VALUE(contato.contato_nome) AS nome_cidadao,
        ANY_VALUE(inicio_datahora) AS sessao_inicio_datahora,
        ANY_VALUE(fim_datahora) AS sessao_fim_datahora,
        COUNT(*) AS qtd_mensagens_usuario,
        STRING_AGG(
            mensagens[SAFE_OFFSET(0)].texto, ' ' ORDER BY mensagem_sequencia
        ) AS mensagens_usuario_concatenadas
    FROM `{source_table}`
    WHERE
        fonte = 'AI_AGENT_CIDADAO'
        AND data_particao BETWEEN DATE(data_inicio) AND DATE(data_fim)
        AND inicio_datahora BETWEEN data_inicio AND data_fim
        AND fim_datahora IS NOT NULL  -- só sessão encerrada
        AND classificacao_llm_datahora IS NULL  -- ainda não classificada (ver cabeçalho)
    GROUP BY id_sessao  -- grão da CTE: 1 linha por sessão, ver nota acima
),

-- candidatas a HSM: disparos (fonte = 'HSM') na janela de até
-- hsm_max_dias_antes dias antes do início do período analisado
hsm_candidatas AS (
    SELECT
        id_interacao AS id_disparo_hsm,
        contato.contato_telefone AS telefone,
        hsm.id_hsm,
        hsm.nome_hsm,
        hsm.categoria_hsm,
        hsm.nome_campanha,
        jornada_nome,
        id_jornada,
        hsm.criacao_envio_datahora AS hsm_envio_datahora
    FROM `{source_table}`
    WHERE
        fonte = 'HSM'
        AND data_particao
        BETWEEN DATE_SUB(DATE(data_inicio), INTERVAL hsm_max_dias_antes DAY) AND DATE(data_fim)
        AND hsm.criacao_envio_datahora
        BETWEEN DATETIME_SUB(data_inicio, INTERVAL hsm_max_dias_antes DAY) AND data_fim
),

-- pareamento: para cada sessão, a HSM do mesmo telefone com
-- criacao_envio_datahora mais próxima (e anterior) do início da sessão,
-- respeitando o teto de hsm_max_dias_antes dias
sessoes_com_hsm AS (
    SELECT
        s.*,
        h.id_disparo_hsm,
        h.jornada_nome,
        h.id_jornada,
        h.hsm_envio_datahora,
        h.id_disparo_hsm IS NOT NULL AS teve_hsm_anterior_indicador
    FROM sessoes_usuario s
    LEFT JOIN
        hsm_candidatas h
        ON
            h.telefone = s.telefone
            AND h.hsm_envio_datahora <= s.sessao_inicio_datahora
            AND h.hsm_envio_datahora
            >= DATETIME_SUB(s.sessao_inicio_datahora, INTERVAL hsm_max_dias_antes DAY)
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY s.id_sessao ORDER BY h.hsm_envio_datahora DESC NULLS LAST
        )
        = 1
)

SELECT sc.*
FROM sessoes_com_hsm sc
LEFT JOIN
    `{destino_full_table_id}` d
    ON
        d.id_sessao = sc.id_sessao
        AND d.data_particao >= DATE(data_inicio)  -- poda a tabela destino, sem TTL (ver cabeçalho)
WHERE d.id_sessao IS NULL  -- garantia final, cobre o desincronismo do pré-filtro 1
ORDER BY sc.sessao_inicio_datahora;
