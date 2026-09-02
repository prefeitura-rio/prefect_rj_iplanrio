-- Sessões do Agentforce (WhatsApp) dos últimos {lookback_days} dias que AINDA NÃO têm
-- classificação — extração incremental de janela fixa, sem watermark: a janela fixa
-- limita o quanto reescaneamos a cada rodada. Traz também a HSM disparada mais próxima
-- (e anterior) ao início da sessão, com no máximo hsm_max_dias_antes dias de antecedência
-- — já com hsm_texto/hsm_variaveis_json resolvidos do disparo específico (ver hsm_candidatas
-- abaixo); enriquece_com_catalogo_hsm (tasks/extract.py) só busca hsm_botoes_json no
-- catálogo por jornada agora, o texto não depende mais dele.
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

-- histórico completo da sessão (cidadão + agente), rotulado por quem falou — contexto
-- extra pro prompt entender a que uma resposta curta do cidadão se refere (ex.: "Entulho"
-- só faz sentido lendo a pergunta do agente logo antes). Não substitui
-- mensagens_usuario_concatenadas (que continua só cidadão — usada pra detectar resposta
-- atrasada a botão e como coluna física de auditoria): as classificações continuam exigidas
-- só com base na fala do cidadão, ver prompt. CTE separada (não dá pra juntar com
-- sessoes_usuario, que agrupa só AI_AGENT_CIDADAO) pra não misturar o grão.
conversa_completa AS (
    SELECT
        id_interacao AS id_sessao,
        STRING_AGG(
            CONCAT(
                IF(fonte = 'AI_AGENT_CIDADAO', 'CIDADÃO: ', 'AGENTE: '),
                mensagens[SAFE_OFFSET(0)].texto
            ),
            '\n'
            ORDER BY mensagem_sequencia
        ) AS conversa_completa
    FROM `{source_table}`
    WHERE
        fonte IN ('AI_AGENT_CIDADAO', 'AI_AGENT_AGENTE')
        AND data_particao BETWEEN DATE(data_inicio) AND DATE(data_fim)
        AND inicio_datahora BETWEEN data_inicio AND data_fim
    GROUP BY id_sessao
),

-- candidatas a HSM: disparos com hsm.nome_hsm identificado, na janela de até
-- hsm_max_dias_antes dias antes do início do período analisado. Filtro é
-- fonte = 'HSM' AND hsm.nome_hsm IS NOT NULL — as duas condições, não só uma:
--   - fonte = 'HSM' sozinho não basta: mensagens_hsm em fct_chatbot_v2.sql seta 'HSM'
--     incondicionalmente pra toda linha da CTE, resolvida ou não (template_match_tipo pode
--     ser 'nao_resolvido' e ainda assim fonte='HSM', com hsm.nome_hsm/hsm_texto NULL — um
--     candidato inútil pra esse pareamento).
--   - hsm.nome_hsm IS NOT NULL sozinho também não basta: uma linha fonte='CUSTOMER'
--     (resposta de botão do cidadão) HERDA os dados de hsm quando pareada, incluindo
--     hsm.nome_hsm preenchido — mas mensagens[0].texto nela é a resposta do cidadão, não o
--     texto do template. Sem o fonte='HSM' também, essas linhas entrariam como candidatas
--     com hsm_texto errado.
-- hsm_texto e hsm_variaveis_json vêm resolvidos aqui mesmo, do disparo específico (não de
-- um catálogo por jornada) — mensagens[0].texto já é o texto do template ou o de Session
-- (ver template_match_tipo em int_chatbot_v2_mensagens_enviadas_enriquecidas.sql, repo
-- queries-rj-crm-registry), e hsm.dados_disparo são as variáveis de personalização
-- daquele disparo específico. A substituição dos placeholders do template acontece em
-- Python (ver tasks/extract.py:_renderiza_hsm) — mais simples que regex em SQL. CUIDADO:
-- este arquivo passa por str.format() em Python (extract.py:extrai_sessoes_nao_classificadas)
-- pra preencher lookback_days/hsm_max_dias_antes/source_table/destino_full_table_id — nunca
-- escrever chaves soltas tipo chave-entre-chaves em comentário aqui, nem como exemplo:
-- str.format() tenta resolver qualquer texto assim, mesmo dentro de comentário, e quebra
-- com KeyError (foi exatamente isso que aconteceu, visto em staging 2026-09-02).
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
        hsm.criacao_envio_datahora AS hsm_envio_datahora,
        mensagens[SAFE_OFFSET(0)].texto AS hsm_texto,
        hsm.dados_disparo AS hsm_variaveis_json
    FROM `{source_table}`
    WHERE
        fonte = 'HSM'
        AND hsm.nome_hsm IS NOT NULL
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
        h.nome_hsm AS hsm_nome,
        h.hsm_texto,
        h.hsm_variaveis_json,
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

SELECT sc.*, cc.conversa_completa
FROM sessoes_com_hsm sc
LEFT JOIN conversa_completa cc ON cc.id_sessao = sc.id_sessao
LEFT JOIN
    `{destino_full_table_id}` d
    ON
        d.id_sessao = sc.id_sessao
        AND d.data_particao >= DATE(data_inicio)  -- poda a tabela destino, sem TTL (ver cabeçalho)
WHERE d.id_sessao IS NULL  -- garantia final, cobre o desincronismo do pré-filtro 1
ORDER BY sc.sessao_inicio_datahora;
