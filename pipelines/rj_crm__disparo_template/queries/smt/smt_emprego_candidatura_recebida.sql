DECLARE inicio_janela DATETIME DEFAULT DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 DAY);

WITH
rmi AS (
  -- RMI: nome e telefone de fallback. O telefone só é trazido para as estratégias de envio permitidas.
  SELECT
    cpf,
    COALESCE(IF(nome_social = "", NULL, nome_social), nome) AS nome_rmi,
    obito.indicador AS obito,
    menor_idade,
    IF(
      telefone.principal.estrategia_envio IN ("ENVIAR", "TESTAR", "EVITAR"),
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
        CONCAT(IFNULL(telefone.principal.ddi, '55'), telefone.principal.ddd, telefone.principal.valor)
      ),
      NULL
    ) AS telefone
  FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`
),

telefone_origem_extraido AS (
  -- Telefone self-declared: extrai do JSON (data) o telefone principal
  SELECT
    REGEXP_REPLACE(JSON_VALUE(data, '$.cpf'), r'\D', '') AS cpf,
    CONCAT(
      IFNULL(JSON_VALUE(data, '$.telefone.principal.ddi'), '55'),
      JSON_VALUE(data, '$.telefone.principal.ddd'),
      JSON_VALUE(data, '$.telefone.principal.valor')
    ) AS phone,
    SAFE_CAST(
      REGEXP_EXTRACT(
        JSON_VALUE(data, '$.telefone.principal.updated_at'),
        r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
      ) AS TIMESTAMP
    ) AS atualizado_em
  FROM `rj-superapp.brutos_rmi.rmi_self_declared`
  WHERE TRIM(IFNULL(JSON_VALUE(data, '$.telefone.principal.valor'), '')) != ''
),

telefone_origem_formatado AS (
  -- Telefone self-declared, validado/formatado pela UDF.
  SELECT
    cpf,
    phone,
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
      phone
    ) AS telefone,
    atualizado_em
  FROM telefone_origem_extraido
),

telefone_origem AS (
  -- Telefone válido mais recente por CPF
  SELECT cpf, telefone
  FROM telefone_origem_formatado
  WHERE telefone IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY atualizado_em DESC) = 1
),

candidaturas_todas AS (
  SELECT
    id AS id_candidatura,
    id_vaga,
    id_etapa_atual,
    REGEXP_REPLACE(cpf, r'\D', '') AS cpf,
    nome,
    status,
    created_at,
    updated_at
  FROM `rj-superapp.brutos_go.emp_candidaturas`
  WHERE deleted_at IS NULL
),

candidaturas_ultimo_status AS (
  -- Último registro por CPF + vaga (mais recente por updated_at), antes de filtrar status e janela
  SELECT *
  FROM candidaturas_todas
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf, id_vaga ORDER BY updated_at DESC) = 1
),

candidaturas AS (
  -- Só entra quem está com o último status em candidatura_enviada
  SELECT
    id_candidatura,
    id_vaga,
    id_etapa_atual,
    cpf,
    nome,
    created_at,
    updated_at
  FROM candidaturas_ultimo_status
  WHERE status = 'candidatura_enviada'
    AND created_at >= inicio_janela
),

vagas AS (
  SELECT
    v.id AS id_vaga,
    v.titulo AS nome_vaga,
    `rj-crm-registry.udf.FORMAT_NAME`(
      COALESCE(NULLIF(TRIM(e.nome_fantasia), ''), NULLIF(TRIM(e.razao_social), '')),
      FALSE
    ) AS nome_empresa,
    NULLIF(TRIM(u.nome_ua), '') AS orgao_intermediario
  FROM `rj-superapp.brutos_go.emp_vagas` v
  LEFT JOIN `rj-superapp.brutos_go.emp_empresas` e
    ON e.cnpj = v.id_contratante
  LEFT JOIN `rj-iplanrio.brutos_unidades_administrativas.orgaos_superapp` u
    ON u.cd_ua = v.id_orgao_parceiro
),
remove_enviados as (
  select
    distinct json_value(hsm.dados_disparo, '$.id_candidatura') as id_candidatura
  from `rj-crm-registry.rmi_conversas.chatbot`
  where data_particao >= date_sub(current_date(), interval 7 day)
    and json_value(hsm.dados_disparo, '$.id_candidatura') is not null
    and hsm.nome_hsm like 'smt_emprego_candidatura_recebida%'
),
base AS (
  SELECT
    c.id_candidatura,
    c.cpf,
    COALESCE(t.telefone, r.telefone) AS telefone,
    IF(t.telefone IS NOT NULL, 'self_declared', 'rmi') AS telefone_fonte,
    COALESCE(NULLIF(TRIM(c.nome), ''), r.nome_rmi) AS nome_completo,
    v.nome_vaga,
    v.nome_empresa,
    v.orgao_intermediario,
    c.created_at
  FROM candidaturas c
  JOIN vagas v
    ON v.id_vaga = c.id_vaga
  LEFT JOIN rmi r
    ON r.cpf = c.cpf
  LEFT JOIN telefone_origem t
    ON t.cpf = c.cpf
    LEFT JOIN remove_enviados env ON env.id_candidatura = c.id_candidatura
  WHERE r.obito IS NOT TRUE
    AND r.menor_idade IS NOT TRUE
    AND COALESCE(t.telefone, r.telefone) IS NOT NULL
    AND env.id_candidatura IS NULL -- remove quem já recebeu sobre aquela candidatura
)

SELECT
  id_candidatura,
  cpf,
  telefone,
  telefone_fonte,
  INITCAP(SPLIT(TRIM(nome_completo), ' ')[SAFE_OFFSET(0)]) AS nome,
  INITCAP(CONCAT(
    SPLIT(TRIM(nome_completo), ' ')[SAFE_OFFSET(0)],
    IF(ARRAY_LENGTH(SPLIT(TRIM(nome_completo), ' ')) > 1,
       CONCAT(' ', ARRAY_REVERSE(SPLIT(TRIM(nome_completo), ' '))[SAFE_OFFSET(0)]), '')
  )) AS nome_sobrenome,
  nome_vaga,
  nome_empresa,
  orgao_intermediario,
  "servicos/trabalho" as rota_pagina,
  created_at
FROM base