-- Sessões do agente (receptivo Salesforce) dos últimos N dias (lookback_days), encerradas
-- e ainda não classificadas, com o HSM que o dbt ligou a cada uma e as mensagens.
--
-- Fonte: fct_chatbot_salesforce, a perna Salesforce da fct_chatbot (v1) — só ela publica
-- a ligação sessão → HSM (id_sessao_ativo_origem, receptivo_hsm_*), já com o texto do HSM
-- renderizado. Sessão = id_sessao_receptivo (2 h sem mensagem no telefone abre outra), e é
-- ele que vai pra id_sessao da tabela destino: o dbt devolve o struct llm por esse id.
--
-- "Já classificada" é checado duas vezes: llm.classificacao_datahora na fonte (barato, mas
-- até 15min atrasado) e anti-join contra a tabela destino no final (garantia).
--
-- ATENÇÃO: este arquivo passa por str.format() em Python — nunca escrever chaves soltas
-- (nem em comentário), senão quebra com KeyError.

DECLARE data_inicio DATETIME DEFAULT DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL {lookback_days} DAY);
-- o Agentforce encerra a sessão 2 h depois da última mensagem
DECLARE encerrada_ate DATETIME DEFAULT DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 2 HOUR);

WITH

-- 1 linha por sessão: contato, HSM ligado (nulo = sem HSM, vai pro prompt sem_hsm),
-- mensagens do cidadão (sem reação com emoji) e a conversa inteira, com "CIDADÃO: " /
-- "AGENTE: " na frente — é o que vai pro prompt.
sessoes AS (
    SELECT
        id_sessao_receptivo AS id_sessao,
        ANY_VALUE(contato.contato_telefone) AS telefone,
        ANY_VALUE(contato.cpf_validado) AS cpf,
        ANY_VALUE(contato.contato_nome) AS nome_cidadao,
        ANY_VALUE(receptivo_inicio_datahora) AS sessao_inicio_datahora,
        ANY_VALUE(receptivo_fim_datahora) AS sessao_fim_datahora,
        ANY_VALUE(id_sessao_ativo_origem) AS id_disparo_hsm,
        ANY_VALUE(receptivo_hsm_texto_renderizado) AS hsm_texto,
        COUNTIF(
            fonte = 'AI_AGENT_CIDADAO' AND mensagem_tipo != 'reaction'
        ) AS qtd_mensagens_usuario,
        COUNTIF(
            fonte = 'AI_AGENT_CIDADAO' AND mensagem_tipo = 'button'
        ) AS qtd_cliques_botao,
        STRING_AGG(
            IF(
                fonte = 'AI_AGENT_CIDADAO' AND mensagem_tipo != 'reaction',
                mensagem_texto,
                NULL
            ),
            ' '
            ORDER BY mensagem_datahora, mensagem_sequencia
        ) AS mensagens_usuario_concatenadas,
        STRING_AGG(
            CONCAT(
                IF(fonte = 'AI_AGENT_CIDADAO', 'CIDADÃO: ', 'AGENTE: '),
                mensagem_texto
            ),
            '\n'
            ORDER BY mensagem_datahora, mensagem_sequencia
        ) AS conversa_completa
    FROM `{source_table}`
    WHERE
        id_sessao_receptivo IS NOT NULL
        AND data_particao >= DATE(data_inicio)
        AND receptivo_inicio_datahora >= data_inicio
        AND llm.classificacao_datahora IS NULL  -- ainda não classificada
    GROUP BY id_sessao
)

-- Descarta o que já está na tabela destino (anti-join). O corte de data_particao fica no
-- ON (no WHERE viraria inner join) e evita varrer o destino inteiro.
SELECT
    s.* EXCEPT (qtd_cliques_botao),
    -- jornada e envio ficam na linha do HSM (id_disparo_hsm), não na sessão: quem
    -- escolhe o HSM é o dbt (exata ou proximidade de até 7 dias)
    CAST(NULL AS STRING) AS id_jornada,
    CAST(NULL AS STRING) AS jornada_nome,
    CAST(NULL AS DATETIME) AS hsm_envio_datahora,
    -- resposta atrasada a botão: a sessão é só o clique num botão do HSM, que caiu no
    -- agente em vez da jornada — não carrega demanda, não vai pra LLM
    s.qtd_mensagens_usuario = 1 AND s.qtd_cliques_botao = 1 AS resposta_atrasada_btn
FROM sessoes s
LEFT JOIN
    `{destino_full_table_id}` d
    ON
        d.id_sessao = s.id_sessao
        AND d.data_particao >= DATE(data_inicio)
WHERE
    d.id_sessao IS NULL
    AND s.sessao_fim_datahora < encerrada_ate
    AND s.qtd_mensagens_usuario > 0  -- pula sessão só de reação
ORDER BY s.sessao_inicio_datahora;
