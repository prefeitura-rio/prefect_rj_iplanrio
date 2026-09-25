-- =====================================================================
-- Features de telefone (1 linha por telefone, calculadas só com dado ANTERIOR
-- ao `data_corte` — leakage-safe). Única fonte das features: o treino e o
-- scoring diário renderizam ESTE arquivo, só muda o que entra nos 3
-- placeholders abaixo (ver tasks/features.py):
--   amostra_ctes : define a CTE `amostra(telefone, data_corte)`
--                    treino = corte sorteado por telefone; agora = CURRENT_DATETIME BRT
--   rotulo_ctes  : treino = futuro/rotulo (HighDelivery); agora = vazio
--   select_final : treino = features + high_delivery; agora = cpf + features
--
-- Portado de qualidade_telefone_modelo/sql/01_treino_features.sql — o cabeçalho
-- de lá tem o histórico de cada decisão de feature (constantes fixas, features
-- removidas/adicionadas, auditoria de leakage). Mudança de feature: mexer SÓ aqui
-- e em constants.py.
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
  -- 1 linha por id_interacao — primeiro disparo de HSM da sessão (mesma
  -- regra de sql/01_disparos_telefones.sql do repo original)
  SELECT id_interacao, telefone, cpf, status_disparo, envio_datahora
  FROM disparos_base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_interacao ORDER BY envio_datahora ASC, id_disparo ASC
  ) = 1
),
aparicoes AS (
  -- whatsapp excluído na origem: gerado pelo próprio disparo, não é
  -- cadastro comparável às demais fontes (mesma decisão de
  -- notebooks/01_preprocessing.ipynb, seção de exclusão de sistemas)
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
universo AS (
  SELECT DISTINCT telefone
  FROM aparicoes
  WHERE telefone IS NOT NULL
),
$amostra_ctes
disparo_amostra AS (
  SELECT d.id_interacao, d.telefone, d.cpf, d.status_disparo, d.envio_datahora, am.data_corte
  FROM disparos d
  JOIN amostra am ON d.telefone = am.telefone
),

$rotulo_ctes
-- ---- 2) features do passado: disparos -----------------------------------
passado AS (
  SELECT
    telefone, cpf, data_corte, envio_datahora,
    status_disparo IN ('delivered', 'read') AS sucesso
  FROM disparo_amostra
  WHERE envio_datahora < data_corte
),
feat_disparo AS (
  SELECT
    telefone,
    COUNT(*) AS qtd_disparo_anterior,
    SAFE_DIVIDE(COUNTIF(sucesso), COUNT(*)) AS taxa_sucesso_anterior,
    DIV(TIMESTAMP_DIFF(ANY_VALUE(data_corte), MAX(envio_datahora), SECOND), 86400)
      AS dias_desde_ultimo_disparo,
    DIV(TIMESTAMP_DIFF(ANY_VALUE(data_corte), MAX(IF(sucesso, envio_datahora, NULL)), SECOND), 86400)
      AS dias_desde_ultimo_disparo_sucesso
  FROM passado
  GROUP BY telefone
),

-- ---- 2b) dias_desde_ultima_resposta: por TELEFONE, não por disparo/sessão --
-- (ver comentário de cabeçalho — WeTalkie x Salesforce estruturam sessão
-- diferente; "há quanto tempo esse telefone respondeu alguma coisa" é
-- mais simples e mais robusto que tentar reamarrar a um disparo específico)
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
respostas_amostra AS (
  SELECT r.telefone, am.data_corte, r.mensagem_datahora
  FROM mensagens_resposta r
  JOIN amostra am ON am.telefone = r.telefone
),
feat_resposta AS (
  SELECT
    telefone,
    DIV(TIMESTAMP_DIFF(ANY_VALUE(data_corte), MAX(mensagem_datahora), SECOND), 86400)
      AS dias_desde_ultima_resposta
  FROM respostas_amostra
  WHERE mensagem_datahora < data_corte
  GROUP BY telefone
),

-- ---- 3) features do passado: sistema e atualidade (leakage-safe) -------
aparicoes_amostra AS (
  SELECT ap.*, am.data_corte
  FROM aparicoes ap
  JOIN amostra am ON ap.telefone = am.telefone
),
aparicoes_conhecidas AS (
  -- descarta só aparição com atualização CONFIRMADAMENTE posterior ao
  -- corte (não-nula E > data_corte); data nula conta como conhecida
  SELECT *
  FROM aparicoes_amostra
  WHERE NOT (
    registro_data_atualizacao IS NOT NULL
    AND registro_data_atualizacao > data_corte
  )
),
feat_sistema AS (
  SELECT
    telefone,
    COUNT(DISTINCT sistema_nome) AS n_sistemas_registrado,
    LOGICAL_OR(proprietario_tipo = 'CNPJ') AS telefone_associado_cnpj,
    -- qtd_aparicoes_<sistema> no lugar de um boolean registrado_<sistema>:
    -- telefone pode aparecer mais de uma vez no MESMO sistema (ex.: vários
    -- membros de uma família citando o mesmo telefone no cadunico) — a
    -- contagem carrega esse sinal, o boolean (qtd > 0) fica implícito
    COUNTIF(sistema_nome = 'agendamento_cadunico') AS qtd_aparicoes_agendamento_cadunico,
    COUNTIF(sistema_nome = 'bcadastro')             AS qtd_aparicoes_bcadastro,
    COUNTIF(sistema_nome = 'cadunico')              AS qtd_aparicoes_cadunico,
    COUNTIF(sistema_nome = 'ergon')                 AS qtd_aparicoes_ergon,
    COUNTIF(sistema_nome = 'sms')                   AS qtd_aparicoes_sms,
    MIN(IF(sistema_nome = 'agendamento_cadunico' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_agendamento_cadunico,
    MIN(IF(sistema_nome = 'bcadastro' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_bcadastro,
    MIN(IF(sistema_nome = 'cadunico' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_cadunico,
    MIN(IF(sistema_nome = 'ergon' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_ergon,
    MIN(IF(sistema_nome = 'sms' AND registro_data_atualizacao IS NOT NULL,
           DIV(TIMESTAMP_DIFF(data_corte, registro_data_atualizacao, SECOND), 86400), NULL))
      AS dias_desde_atualizacao_sms
  FROM aparicoes_conhecidas
  GROUP BY telefone
),
feat_maturidade AS (
  -- oposto de dias_desde_atualizacao_<sistema> (que usa o registro mais
  -- RECENTE): aqui é o mais ANTIGO conhecido, entre qualquer sistema —
  -- proxy de "há quanto tempo esse número existe na base"
  SELECT
    telefone,
    DIV(TIMESTAMP_DIFF(ANY_VALUE(data_corte), MIN(registro_data_atualizacao), SECOND), 86400)
      AS dias_desde_primeiro_registro
  FROM aparicoes_conhecidas
  WHERE registro_data_atualizacao IS NOT NULL
  GROUP BY telefone
),

-- ---- 4) qtd_cpfs_associados (cadastro + disparo, ambos < data_corte) ---
cpf_pares AS (
  SELECT telefone, LPAD(proprietario_id, 11, '0') AS cpf
  FROM aparicoes_conhecidas
  WHERE proprietario_tipo = 'CPF' AND proprietario_id IS NOT NULL

  UNION DISTINCT

  SELECT telefone, cpf
  FROM passado
  WHERE cpf IS NOT NULL
),
feat_cpf AS (
  SELECT telefone, COUNT(DISTINCT cpf) AS qtd_cpfs_associados
  FROM cpf_pares
  GROUP BY telefone
),

-- ---- 4b) volatilidade: quantos telefones o CPF mais recente já usou ---
-- "esse CPF troca de número com frequência?" — pega, pra este telefone, o
-- CPF associado mais recentemente (antes do data_corte, "o dono atual" na
-- falta de termo melhor) e conta quantos telefones DISTINTOS esse CPF já
-- usou até o mesmo data_corte. Usa cpf_pares_global (sem o corte de
-- leakage ainda) porque um CPF pode estar ligado a vários telefones, cada
-- um com seu próprio data_corte sorteado — não dá pra reusar
-- aparicoes_conhecidas/passado (já pré-filtrados pro corte de UM telefone
-- específico).
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
cpf_mais_recente AS (
  SELECT am.telefone, am.data_corte, g.cpf
  FROM amostra am
  JOIN cpf_pares_global g ON g.telefone = am.telefone AND g.data_associacao < am.data_corte
  QUALIFY ROW_NUMBER() OVER (PARTITION BY am.telefone ORDER BY g.data_associacao DESC) = 1
),
feat_volatilidade AS (
  SELECT
    cmr.telefone,
    COUNT(DISTINCT g.telefone) AS qtd_telefones_cpf_recente
  FROM cpf_mais_recente cmr
  JOIN cpf_pares_global g
    ON g.cpf = cmr.cpf
    AND g.data_associacao < cmr.data_corte
  GROUP BY cmr.telefone
),

-- ---- 5) grau de compartilhamento familiar (CadÚnico) --------------------
parentesco AS (
  SELECT cpf, id_familia, menor_idade
  FROM `rj-crm-registry.intermediario_rmi_parentesco_cadunico.dim_parentesco`
),
cpf_pares_familia AS (
  SELECT cp.telefone, cp.cpf, p.id_familia, p.menor_idade
  FROM cpf_pares cp
  JOIN parentesco p ON p.cpf = cp.cpf
),
feat_familia AS (
  SELECT
    telefone,
    COUNT(DISTINCT cpf) AS qtd_cpfs_associados_cadunico,
    COUNT(DISTINCT id_familia) AS qtd_familias_associadas,
    SAFE_DIVIDE(COUNTIF(menor_idade), COUNT(*)) AS pct_cpfs_menor_idade
  FROM cpf_pares_familia
  GROUP BY telefone
),

-- ---- 6) DDD e metadados de telefone (fixos, não dependem de data_corte) --
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
),

-- ---- 7) monta as features (1 linha por telefone) ------------------------
features AS (
SELECT
  am.telefone,
  am.data_corte,

  COALESCE(fd.qtd_disparo_anterior, 0)                AS qtd_disparo_anterior,
  COALESCE(fd.taxa_sucesso_anterior, -1)              AS taxa_sucesso_anterior,
  COALESCE(fd.dias_desde_ultimo_disparo, -1)          AS dias_desde_ultimo_disparo,
  COALESCE(fd.dias_desde_ultimo_disparo_sucesso, -1)  AS dias_desde_ultimo_disparo_sucesso,
  COALESCE(fr.dias_desde_ultima_resposta, -1)         AS dias_desde_ultima_resposta,

  COALESCE(fs.n_sistemas_registrado, 0) AS n_sistemas_registrado,
  CAST(COALESCE(fs.telefone_associado_cnpj, FALSE) AS INT64) AS telefone_associado_cnpj,
  COALESCE(fs.qtd_aparicoes_agendamento_cadunico, 0) AS qtd_aparicoes_agendamento_cadunico,
  COALESCE(fs.qtd_aparicoes_bcadastro, 0)            AS qtd_aparicoes_bcadastro,
  COALESCE(fs.qtd_aparicoes_cadunico, 0)             AS qtd_aparicoes_cadunico,
  COALESCE(fs.qtd_aparicoes_ergon, 0)                AS qtd_aparicoes_ergon,
  COALESCE(fs.qtd_aparicoes_sms, 0)                  AS qtd_aparicoes_sms,
  COALESCE(fs.dias_desde_atualizacao_agendamento_cadunico, -1)       AS dias_desde_atualizacao_agendamento_cadunico,
  COALESCE(fs.dias_desde_atualizacao_bcadastro, -1)                  AS dias_desde_atualizacao_bcadastro,
  COALESCE(fs.dias_desde_atualizacao_cadunico, -1)                   AS dias_desde_atualizacao_cadunico,
  COALESCE(fs.dias_desde_atualizacao_ergon, -1)                      AS dias_desde_atualizacao_ergon,
  COALESCE(fs.dias_desde_atualizacao_sms, -1)                        AS dias_desde_atualizacao_sms,

  COALESCE(fc.qtd_cpfs_associados, 0) AS qtd_cpfs_associados,
  COALESCE(fv.qtd_telefones_cpf_recente, -1) AS qtd_telefones_cpf_recente,

  COALESCE(ff.qtd_cpfs_associados_cadunico, 0) AS qtd_cpfs_associados_cadunico,
  COALESCE(ff.qtd_familias_associadas, 0)      AS qtd_familias_associadas,
  CASE WHEN COALESCE(ff.qtd_familias_associadas, 0) > 0
       THEN ff.qtd_cpfs_associados_cadunico / ff.qtd_familias_associadas
       ELSE -1 END AS concentracao_familiar,
  COALESCE(ff.pct_cpfs_menor_idade, -1) AS pct_cpfs_menor_idade,

  COALESCE(fm.dias_desde_primeiro_registro, -1) AS dias_desde_primeiro_registro,

  -- one-hot aqui (não em Python): as categorias já são fixas nesta query (top 5 DDDs
  -- + 'outros'), então treino e scoring recebem sempre as mesmas colunas. NULL
  -- (telefone sem DDD/qualidade) vira 0 em todas as dummies da coluna.
  CAST(COALESCE(dt.ddd_categoria = '11', FALSE) AS INT64) AS ddd_11,
  CAST(COALESCE(dt.ddd_categoria = '21', FALSE) AS INT64) AS ddd_21,
  CAST(COALESCE(dt.ddd_categoria = '22', FALSE) AS INT64) AS ddd_22,
  CAST(COALESCE(dt.ddd_categoria = '24', FALSE) AS INT64) AS ddd_24,
  CAST(COALESCE(dt.ddd_categoria = '83', FALSE) AS INT64) AS ddd_83,
  CAST(COALESCE(dt.ddd_categoria = 'outros', FALSE) AS INT64) AS ddd_outros,
  CAST(COALESCE(tm.telefone_qualidade = 'INVALIDO', FALSE) AS INT64) AS qualidade_INVALIDO,
  CAST(COALESCE(tm.telefone_qualidade = 'SUSPEITO', FALSE) AS INT64) AS qualidade_SUSPEITO,
  CAST(COALESCE(tm.telefone_qualidade = 'VALIDO', FALSE) AS INT64) AS qualidade_VALIDO
FROM amostra am
LEFT JOIN feat_disparo fd    ON fd.telefone = am.telefone
LEFT JOIN feat_resposta fr   ON fr.telefone = am.telefone
LEFT JOIN feat_cpf fc        ON fc.telefone = am.telefone
LEFT JOIN feat_volatilidade fv ON fv.telefone = am.telefone
LEFT JOIN feat_familia ff    ON ff.telefone = am.telefone
LEFT JOIN feat_sistema fs    ON fs.telefone = am.telefone
LEFT JOIN feat_maturidade fm ON fm.telefone = am.telefone
LEFT JOIN ddd_telefone dt    ON dt.telefone = am.telefone
LEFT JOIN telefone_meta tm   ON tm.telefone = am.telefone
)

$select_final
