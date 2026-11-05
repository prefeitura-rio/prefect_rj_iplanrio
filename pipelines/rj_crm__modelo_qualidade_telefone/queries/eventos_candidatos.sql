-- =====================================================================
-- 02 — Eventos × candidatos (pool pra simulação/avaliação)
-- =====================================================================
-- Porta pra SQL a lógica das seções 2-3 de
-- qualidade_telefone/modelo/03_avaliacao.ipynb: para cada disparo real
-- dos últimos `janela_recente_dias` dias com CPF conhecido, monta o pool
-- de telefones candidatos (todo telefone já associado àquele CPF em
-- qualquer sistema, restrito aos elegíveis — `estrategia_envio != 'NÃO
-- ENVIAR'`) e calcula, pra cada par (id_interacao, telefone candidato),
-- as MESMAS features de sql/01_treino_features.sql — só que com 1
-- `data_corte` por evento (o `envio_datahora` do disparo real sendo
-- avaliado) em vez de 1 corte sorteado por telefone. Mesmo telefone pode
-- aparecer em várias linhas (evento diferente), e vários telefones
-- compartilham o mesmo id_interacao (um por candidato).
--
-- Traz também o resultado real do disparo (telefone_usado, falhou) — o
-- necessário pra montar a matriz de confusão comparativa em
-- avaliacao/base.py sem precisar de uma segunda query.
--
-- Mesmas constantes fixas e mesmas features adicionadas/trocadas em
-- features_telefone.sql (compartilhamento familiar via dim_parentesco,
-- n_sistemas_registrado, dias_desde_primeiro_registro,
-- pct_cpfs_menor_idade, qtd_aparicoes_<sistema> no lugar do boolean
-- registrado_<sistema>, dias_desde_ultima_resposta, telefone_associado_cnpj)
-- — ver comentário de cabeçalho daquele arquivo. telefone_tipo e os 4
-- booleans de validacao_telefone tambem foram removidos aqui pelo mesmo
-- motivo (SHAP = 0.0). One-hot de ddd_categoria/telefone_qualidade feito
-- aqui em SQL (não em Python), mesma decisão de features_telefone.sql —
-- ver comentário lá.
-- =====================================================================

WITH disparos_base AS (
  SELECT
    c.id_interacao,
    c.hsm.id_disparo,
    c.contato.contato_telefone AS telefone,
    c.contato.cpf AS cpf,
    c.hsm.status_disparo,
    c.hsm.envio_datahora
  FROM `rj-crm-registry.rmi_conversas.chatbot` AS c
  WHERE c.hsm.indicador = TRUE
    AND c.hsm.id_hsm IS NOT NULL
),
disparos AS (
  SELECT id_interacao, telefone, cpf, status_disparo, envio_datahora
  FROM disparos_base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_interacao ORDER BY envio_datahora ASC, id_disparo ASC
  ) = 1
),
aparicoes AS (
  SELECT
    t.telefone_numero_completo AS telefone,
    t.telefone_ddd,
    t.telefone_qualidade,
    t.estrategia_envio,
    ap.sistema_nome,
    ap.proprietario_id,
    ap.proprietario_tipo,
    ap.registro_data_atualizacao
  FROM `rj-crm-registry.intermediario_rmi_telefones.int_telefone` AS t,
    UNNEST(t.telefone_aparicoes) AS ap
  WHERE ap.sistema_nome != 'whatsapp'
),

-- ---- 1) disparos recentes elegíveis pra avaliação -----------------------
periodo_recente AS (
  SELECT TIMESTAMP_SUB(MAX(envio_datahora), INTERVAL $janela_recente_dias DAY) AS data_min_avaliacao
  FROM disparos
),
disparos_avaliar AS (
  SELECT d.*
  FROM disparos d
  CROSS JOIN periodo_recente p
  WHERE d.envio_datahora >= p.data_min_avaliacao
    AND d.cpf IS NOT NULL
),

-- ---- 2) pool de candidatos: todo telefone elegível já associado ao CPF -
telefones_elegiveis AS (
  SELECT DISTINCT telefone
  FROM aparicoes
  WHERE estrategia_envio IS NOT NULL AND estrategia_envio != 'NÃO ENVIAR'
),
cpf_telefones_elegivel AS (
  SELECT DISTINCT LPAD(proprietario_id, 11, '0') AS cpf, telefone
  FROM aparicoes
  WHERE proprietario_tipo = 'CPF'
    AND proprietario_id IS NOT NULL
    AND telefone IN (SELECT telefone FROM telefones_elegiveis)
),
eventos AS (
  SELECT DISTINCT
    d.id_interacao,
    ct.telefone,
    d.envio_datahora AS data_corte
  FROM disparos_avaliar d
  JOIN cpf_telefones_elegivel ct ON ct.cpf = d.cpf
),

-- ---- 3) features por evento, mesma lógica leakage-safe de 01 ----------
disparo_evento AS (
  SELECT e.id_interacao, e.telefone, e.data_corte, d.envio_datahora, d.status_disparo
  FROM eventos e
  JOIN disparos d ON d.telefone = e.telefone
),
passado_evento AS (
  SELECT
    id_interacao, telefone, data_corte, envio_datahora,
    status_disparo IN ('delivered', 'read') AS sucesso
  FROM disparo_evento
  WHERE envio_datahora < data_corte
),
feat_disparo_evento AS (
  SELECT
    id_interacao, telefone,
    COUNT(*) AS qtd_disparo_anterior,
    SAFE_DIVIDE(COUNTIF(sucesso), COUNT(*)) AS taxa_sucesso_anterior,
    DIV(TIMESTAMP_DIFF(ANY_VALUE(data_corte), MAX(envio_datahora), SECOND), 86400)
      AS dias_desde_ultimo_disparo,
    DIV(TIMESTAMP_DIFF(ANY_VALUE(data_corte), MAX(IF(sucesso, envio_datahora, NULL)), SECOND), 86400)
      AS dias_desde_ultimo_disparo_sucesso
  FROM passado_evento
  GROUP BY id_interacao, telefone
),

-- ---- 2b) dias_desde_ultima_resposta: por TELEFONE, não por disparo/sessão --
-- (mesma lógica de features_telefone.sql — ver comentário de cabeçalho
-- daquele arquivo)
mensagens_resposta AS (
  SELECT c.contato.contato_telefone AS telefone, c.mensagens[OFFSET(0)].data AS mensagem_datahora
  FROM `rj-crm-registry.rmi_conversas.chatbot` AS c
  WHERE c.plataforma_origem = 'SALESFORCE'
    AND c.fonte IN ('CUSTOMER', 'AI_AGENT_CIDADAO')

  UNION ALL

  SELECT c.contato.contato_telefone AS telefone, msg.data AS mensagem_datahora
  FROM `rj-crm-registry.rmi_conversas.chatbot` AS c,
    UNNEST(c.mensagens) AS msg
  WHERE c.plataforma_origem IS DISTINCT FROM 'SALESFORCE'
    AND msg.fonte = 'CUSTOMER'
),
respostas_evento AS (
  SELECT ev.id_interacao, ev.telefone, ev.data_corte, r.mensagem_datahora
  FROM eventos ev
  JOIN mensagens_resposta r ON r.telefone = ev.telefone
),
feat_resposta_evento AS (
  SELECT
    id_interacao, telefone,
    DIV(TIMESTAMP_DIFF(ANY_VALUE(data_corte), MAX(mensagem_datahora), SECOND), 86400)
      AS dias_desde_ultima_resposta
  FROM respostas_evento
  WHERE mensagem_datahora < data_corte
  GROUP BY id_interacao, telefone
),
aparicoes_evento AS (
  SELECT ap.*, e.id_interacao, e.data_corte AS data_corte_evento
  FROM aparicoes ap
  JOIN eventos e ON ap.telefone = e.telefone
),
aparicoes_conhecidas_evento AS (
  SELECT *
  FROM aparicoes_evento
  WHERE NOT (
    registro_data_atualizacao IS NOT NULL
    AND registro_data_atualizacao > data_corte_evento
  )
),
feat_sistema_evento AS (
  SELECT
    id_interacao, telefone,
    COUNT(DISTINCT sistema_nome) AS n_sistemas_registrado,
    LOGICAL_OR(proprietario_tipo = 'CNPJ') AS telefone_associado_cnpj,
    COUNTIF(sistema_nome = 'agendamento_cadunico') AS qtd_aparicoes_agendamento_cadunico,
    COUNTIF(sistema_nome = 'bcadastro')             AS qtd_aparicoes_bcadastro,
    COUNTIF(sistema_nome = 'cadunico')              AS qtd_aparicoes_cadunico,
    COUNTIF(sistema_nome = 'ergon')                 AS qtd_aparicoes_ergon,
    COUNTIF(sistema_nome = 'sms')                   AS qtd_aparicoes_sms,
    MIN(IF(sistema_nome = 'agendamento_cadunico' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte_evento, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_agendamento_cadunico,
    MIN(IF(sistema_nome = 'bcadastro' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte_evento, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_bcadastro,
    MIN(IF(sistema_nome = 'cadunico' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte_evento, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_cadunico,
    MIN(IF(sistema_nome = 'ergon' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte_evento, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_ergon,
    MIN(IF(sistema_nome = 'sms' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte_evento, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_sms
  FROM aparicoes_conhecidas_evento
  GROUP BY id_interacao, telefone
),
feat_maturidade_evento AS (
  SELECT
    id_interacao, telefone,
    DIV(TIMESTAMP_DIFF(ANY_VALUE(data_corte_evento), MIN(registro_data_atualizacao), SECOND), 86400)
      AS dias_desde_primeiro_registro
  FROM aparicoes_conhecidas_evento
  WHERE registro_data_atualizacao IS NOT NULL
  GROUP BY id_interacao, telefone
),
-- cpf do disparo histórico (não o cpf do evento sendo avaliado) — vem de
-- `disparos`, casado por telefone+envio_datahora com passado_evento (que
-- não carrega cpf pra evitar colisão de nome com o cpf do evento avaliado)
cpf_pares_evento_disparo AS (
  SELECT pe.id_interacao, pe.telefone, d.cpf
  FROM passado_evento pe
  JOIN disparos d
    ON d.telefone = pe.telefone AND d.envio_datahora = pe.envio_datahora
  WHERE d.cpf IS NOT NULL
),
cpf_pares_evento AS (
  SELECT id_interacao, telefone, LPAD(proprietario_id, 11, '0') AS cpf
  FROM aparicoes_conhecidas_evento
  WHERE proprietario_tipo = 'CPF' AND proprietario_id IS NOT NULL

  UNION DISTINCT

  SELECT id_interacao, telefone, cpf
  FROM cpf_pares_evento_disparo
),
feat_cpf_evento AS (
  SELECT id_interacao, telefone, COUNT(DISTINCT cpf) AS qtd_cpfs_associados
  FROM cpf_pares_evento
  GROUP BY id_interacao, telefone
),

-- ---- 3b) volatilidade: quantos telefones o CPF mais recente já usou ---
-- mesma lógica de features_telefone.sql — ver comentário de cabeçalho
-- daquele arquivo. cpf_pares_global sem corte de leakage ainda (aplicado
-- por evento, contra o data_corte de CADA um).
cpf_pares_global AS (
  SELECT LPAD(ap.proprietario_id, 11, '0') AS cpf, ap.telefone, ap.registro_data_atualizacao AS data_associacao
  FROM aparicoes ap
  WHERE ap.proprietario_tipo = 'CPF' AND ap.proprietario_id IS NOT NULL
    AND ap.registro_data_atualizacao IS NOT NULL

  UNION ALL

  SELECT d.cpf, d.telefone, d.envio_datahora AS data_associacao
  FROM disparos d
  WHERE d.cpf IS NOT NULL
),
cpf_mais_recente_evento AS (
  SELECT ev.id_interacao, ev.telefone, ev.data_corte, g.cpf
  FROM eventos ev
  JOIN cpf_pares_global g ON g.telefone = ev.telefone AND g.data_associacao < ev.data_corte
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ev.id_interacao, ev.telefone ORDER BY g.data_associacao DESC) = 1
),
feat_volatilidade_evento AS (
  SELECT
    cmr.id_interacao, cmr.telefone,
    COUNT(DISTINCT g.telefone) AS qtd_telefones_cpf_recente
  FROM cpf_mais_recente_evento cmr
  JOIN cpf_pares_global g
    ON g.cpf = cmr.cpf
    AND g.data_associacao < cmr.data_corte
  GROUP BY cmr.id_interacao, cmr.telefone
),

-- ---- 4) grau de compartilhamento familiar (CadÚnico), por evento -------
parentesco AS (
  SELECT cpf, id_familia, menor_idade
  FROM `rj-crm-registry.intermediario_rmi_parentesco_cadunico.dim_parentesco`
),
cpf_pares_evento_familia AS (
  SELECT cpe.id_interacao, cpe.telefone, cpe.cpf, p.id_familia, p.menor_idade
  FROM cpf_pares_evento cpe
  JOIN parentesco p ON p.cpf = cpe.cpf
),
feat_familia_evento AS (
  SELECT
    id_interacao, telefone,
    COUNT(DISTINCT cpf) AS qtd_cpfs_associados_cadunico,
    COUNT(DISTINCT id_familia) AS qtd_familias_associadas,
    SAFE_DIVIDE(COUNTIF(menor_idade), COUNT(*)) AS pct_cpfs_menor_idade
  FROM cpf_pares_evento_familia
  GROUP BY id_interacao, telefone
),

-- ---- 5) DDD e metadados de telefone (fixos, iguais aos de 01) ---------
ddd_telefone AS (
  SELECT
    telefone,
    CASE WHEN MAX(telefone_ddd) IN ('21', '22', '11', '24', '83') THEN MAX(telefone_ddd)
         ELSE 'outros' END AS ddd_categoria
  FROM aparicoes
  WHERE telefone_ddd IS NOT NULL
  GROUP BY telefone
),
telefone_meta AS (
  SELECT telefone, MAX(telefone_qualidade) AS telefone_qualidade
  FROM aparicoes
  GROUP BY telefone
)

-- ---- 6) monta a saída: 1 linha por (id_interacao, telefone candidato) --
SELECT
  ev.id_interacao,
  ev.telefone,
  ev.data_corte,

  da.telefone AS telefone_usado,
  da.status_disparo,
  da.status_disparo NOT IN ('delivered', 'read') AS falhou,

  COALESCE(fde.qtd_disparo_anterior, 0)               AS qtd_disparo_anterior,
  COALESCE(fde.taxa_sucesso_anterior, -1)             AS taxa_sucesso_anterior,
  COALESCE(fde.dias_desde_ultimo_disparo, -1)         AS dias_desde_ultimo_disparo,
  COALESCE(fde.dias_desde_ultimo_disparo_sucesso, -1) AS dias_desde_ultimo_disparo_sucesso,
  COALESCE(fre.dias_desde_ultima_resposta, -1)        AS dias_desde_ultima_resposta,

  COALESCE(fse.n_sistemas_registrado, 0) AS n_sistemas_registrado,
  CAST(COALESCE(fse.telefone_associado_cnpj, FALSE) AS INT64) AS telefone_associado_cnpj,
  COALESCE(fse.qtd_aparicoes_agendamento_cadunico, 0) AS qtd_aparicoes_agendamento_cadunico,
  COALESCE(fse.qtd_aparicoes_bcadastro, 0)            AS qtd_aparicoes_bcadastro,
  COALESCE(fse.qtd_aparicoes_cadunico, 0)             AS qtd_aparicoes_cadunico,
  COALESCE(fse.qtd_aparicoes_ergon, 0)                AS qtd_aparicoes_ergon,
  COALESCE(fse.qtd_aparicoes_sms, 0)                  AS qtd_aparicoes_sms,
  COALESCE(fse.dias_desde_atualizacao_agendamento_cadunico, -1)       AS dias_desde_atualizacao_agendamento_cadunico,
  COALESCE(fse.dias_desde_atualizacao_bcadastro, -1)                  AS dias_desde_atualizacao_bcadastro,
  COALESCE(fse.dias_desde_atualizacao_cadunico, -1)                   AS dias_desde_atualizacao_cadunico,
  COALESCE(fse.dias_desde_atualizacao_ergon, -1)                      AS dias_desde_atualizacao_ergon,
  COALESCE(fse.dias_desde_atualizacao_sms, -1)                        AS dias_desde_atualizacao_sms,

  COALESCE(fce.qtd_cpfs_associados, 0) AS qtd_cpfs_associados,
  COALESCE(fve.qtd_telefones_cpf_recente, -1) AS qtd_telefones_cpf_recente,

  COALESCE(ffe.qtd_cpfs_associados_cadunico, 0) AS qtd_cpfs_associados_cadunico,
  COALESCE(ffe.qtd_familias_associadas, 0)      AS qtd_familias_associadas,
  CASE WHEN COALESCE(ffe.qtd_familias_associadas, 0) > 0
       THEN ffe.qtd_cpfs_associados_cadunico / ffe.qtd_familias_associadas
       ELSE -1 END AS concentracao_familiar,
  COALESCE(ffe.pct_cpfs_menor_idade, -1) AS pct_cpfs_menor_idade,

  COALESCE(fme.dias_desde_primeiro_registro, -1) AS dias_desde_primeiro_registro,

  CAST(COALESCE(dt.ddd_categoria = '11', FALSE) AS INT64) AS ddd_11,
  CAST(COALESCE(dt.ddd_categoria = '21', FALSE) AS INT64) AS ddd_21,
  CAST(COALESCE(dt.ddd_categoria = '22', FALSE) AS INT64) AS ddd_22,
  CAST(COALESCE(dt.ddd_categoria = '24', FALSE) AS INT64) AS ddd_24,
  CAST(COALESCE(dt.ddd_categoria = '83', FALSE) AS INT64) AS ddd_83,
  CAST(COALESCE(dt.ddd_categoria = 'outros', FALSE) AS INT64) AS ddd_outros,
  CAST(COALESCE(tm.telefone_qualidade = 'INVALIDO', FALSE) AS INT64) AS qualidade_INVALIDO,
  CAST(COALESCE(tm.telefone_qualidade = 'SUSPEITO', FALSE) AS INT64) AS qualidade_SUSPEITO,
  CAST(COALESCE(tm.telefone_qualidade = 'VALIDO', FALSE) AS INT64) AS qualidade_VALIDO

FROM eventos ev
JOIN disparos_avaliar da             ON da.id_interacao = ev.id_interacao
LEFT JOIN feat_disparo_evento fde    ON fde.id_interacao = ev.id_interacao AND fde.telefone = ev.telefone
LEFT JOIN feat_resposta_evento fre   ON fre.id_interacao = ev.id_interacao AND fre.telefone = ev.telefone
LEFT JOIN feat_sistema_evento fse    ON fse.id_interacao = ev.id_interacao AND fse.telefone = ev.telefone
LEFT JOIN feat_cpf_evento fce        ON fce.id_interacao = ev.id_interacao AND fce.telefone = ev.telefone
LEFT JOIN feat_volatilidade_evento fve ON fve.id_interacao = ev.id_interacao AND fve.telefone = ev.telefone
LEFT JOIN feat_familia_evento ffe    ON ffe.id_interacao = ev.id_interacao AND ffe.telefone = ev.telefone
LEFT JOIN feat_maturidade_evento fme ON fme.id_interacao = ev.id_interacao AND fme.telefone = ev.telefone
LEFT JOIN ddd_telefone dt            ON dt.telefone = ev.telefone
LEFT JOIN telefone_meta tm           ON tm.telefone = ev.telefone
ORDER BY ev.id_interacao, ev.telefone
