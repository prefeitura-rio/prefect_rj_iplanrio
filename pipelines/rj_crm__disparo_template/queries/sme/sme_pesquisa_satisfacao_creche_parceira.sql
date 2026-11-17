WITH rmi AS (
  -- Telefone do RMI, só para quem passa nos filtros obrigatórios (seção 4.5)
  SELECT
    LPAD(CAST(pf.cpf AS STRING), 11, '0') AS cpf,
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(CONCAT(
      IFNULL(pf.telefone.principal.ddi, '55'),
      IFNULL(pf.telefone.principal.ddd, '21'),
      pf.telefone.principal.valor
    )) AS telefone_rmi
  FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf
  WHERE pf.menor_idade IS FALSE
    AND pf.obito.indicador IS FALSE
    AND pf.telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LPAD(CAST(pf.cpf AS STRING), 11, '0')
    ORDER BY IF(pf.telefone.principal.estrategia_envio = 'ENVIAR', 0, 1)
  ) = 1
),

respondeu AS (
  -- Quem já respondeu a pesquisa (HSM original ou lembrete) nos últimos 30 dias.
  -- Cruza por CPF e por telefone, pois o mesmo responsável pode aparecer com outro número.
  SELECT DISTINCT
    c.contato.cpf AS cpf,
    c.contato.contato_telefone AS telefone
  FROM `rj-crm-registry.rmi_conversas.chatbot` c
  WHERE c.data_particao >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 30 DAY)
    AND c.hsm.indicador IS TRUE
    AND c.hsm.nome_hsm IN (
      'sme_pesquisa_satisfacao_creche_parceira',
      'sme_pesquisa_satisfacao_creche_parceira_lembrete'
    )
    AND c.foi_respondida IS TRUE
),

obitos AS (
  -- Responsáveis falecidos não recebem, mesmo com telefone vindo da SME
  SELECT DISTINCT LPAD(CAST(cpf AS STRING), 11, '0') AS cpf
  FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`
  WHERE obito.indicador IS TRUE
),

alunos_base AS (
  SELECT
    a.*,
    LPAD(REGEXP_REPLACE(TRIM(CAST(a.cpf AS STRING)), r'\D', ''), 11, '0') AS cpf_aluno_limpo,
    LPAD(REGEXP_REPLACE(TRIM(CAST(a.cpf_responsavel AS STRING)), r'\D', ''), 11, '0') AS cpf_resp_limpo,
    nome_unidade as denominacao
  FROM `rj-sme-dev.Pequenos_cariocas.CP_BasePesquisaSatisfacao_20261109_id` a
  WHERE a.cpf IS NOT NULL AND TRIM(CAST(a.cpf AS STRING)) != ''
),

alunos AS (
  -- Validação do CPF do responsável (dígitos verificadores)
  SELECT
    ab.*,
    (
      LENGTH(ab.cpf_resp_limpo) = 11
      AND (SELECT COUNT(DISTINCT x) FROM UNNEST(SPLIT(ab.cpf_resp_limpo, '')) x) > 1
      AND (SELECT MOD(MOD(SUM(SAFE_CAST(x AS INT64) * (10 - o)) * 10, 11), 10)
           FROM UNNEST(SPLIT(SUBSTR(ab.cpf_resp_limpo, 1, 9), '')) x WITH OFFSET o)
          = SAFE_CAST(SUBSTR(ab.cpf_resp_limpo, 10, 1) AS INT64)
      AND (SELECT MOD(MOD(SUM(SAFE_CAST(x AS INT64) * (11 - o)) * 10, 11), 10)
           FROM UNNEST(SPLIT(SUBSTR(ab.cpf_resp_limpo, 1, 10), '')) x WITH OFFSET o)
          = SAFE_CAST(SUBSTR(ab.cpf_resp_limpo, 11, 1) AS INT64)
    ) AS resp_valido
  FROM alunos_base ab
),

cadunico_base AS (
  SELECT
    LPAD(REGEXP_REPLACE(TRIM(CAST(cpf AS STRING)), r'\D', ''), 11, '0') AS cpf_aluno_limpo,
    LPAD(REGEXP_REPLACE(TRIM(CAST(cpf_responsavel AS STRING)), r'\D', ''), 11, '0') AS cpf_resp_cadunico,
    nome_responsavel AS nome_resp_cadunico
  FROM `rj-crm-registry.intermediario_rmi_parentesco_cadunico.dim_parentesco`
  WHERE cpf IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LPAD(REGEXP_REPLACE(TRIM(CAST(cpf AS STRING)), r'\D', ''), 11, '0')
    ORDER BY id_familia  -- desempate se o aluno aparecer em mais de uma família
  ) = 1
),

cadunico AS (
  SELECT
    cb.*,
    (
      LENGTH(cb.cpf_resp_cadunico) = 11
      AND (SELECT COUNT(DISTINCT x) FROM UNNEST(SPLIT(cb.cpf_resp_cadunico, '')) x) > 1
      AND (SELECT MOD(MOD(SUM(SAFE_CAST(x AS INT64) * (10 - o)) * 10, 11), 10)
           FROM UNNEST(SPLIT(SUBSTR(cb.cpf_resp_cadunico, 1, 9), '')) x WITH OFFSET o)
          = SAFE_CAST(SUBSTR(cb.cpf_resp_cadunico, 10, 1) AS INT64)
      AND (SELECT MOD(MOD(SUM(SAFE_CAST(x AS INT64) * (11 - o)) * 10, 11), 10)
           FROM UNNEST(SPLIT(SUBSTR(cb.cpf_resp_cadunico, 1, 10), '')) x WITH OFFSET o)
          = SAFE_CAST(SUBSTR(cb.cpf_resp_cadunico, 11, 1) AS INT64)
    ) AS cadunico_valido
  FROM cadunico_base cb
),

responsavel AS (
  -- CPF e nome do responsável: original se válido; senão CadÚnico
  SELECT
    a.cpf_aluno_limpo,
    a.nome_aluno,
    a.telefone_1,
    a.contato_telefone_1,
    a.telefone_2,
    CASE
      WHEN a.resp_valido THEN a.cpf_resp_limpo
      WHEN c.cadunico_valido THEN c.cpf_resp_cadunico
    END AS cpf_responsavel_final,
    CASE
      WHEN a.resp_valido THEN NULLIF(TRIM(a.responsavel), '')
      WHEN c.cadunico_valido THEN NULLIF(TRIM(c.nome_resp_cadunico), '')
    END AS nome_responsavel_final,
    a.id_aluno,
    a.denominacao
  FROM alunos a
  LEFT JOIN cadunico c
    ON a.cpf_aluno_limpo = c.cpf_aluno_limpo
   AND NOT a.resp_valido
),

dados AS (
  SELECT
    r.cpf_responsavel_final AS cpf,
    -- Prioridade: contato_telefone_1, telefone_1, telefone_2, RMI
    COALESCE(
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(r.contato_telefone_1),
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(r.telefone_1),
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(r.telefone_2),
      rmi.telefone_rmi
    ) AS telefone,
    `rj-crm-registry.udf.FORMAT_NAME`(r.nome_responsavel_final, TRUE) AS nome_responsavel,
    `rj-crm-registry.udf.FORMAT_NAME`(r.nome_aluno, TRUE) AS nome_aluno,
    r.cpf_aluno_limpo AS cpf_aluno,   -- coluna auxiliar de rastreio (seção 4.4)
    id_aluno,
    `rj-crm-registry.udf.FORMAT_NAME`(denominacao, FALSE) as denominacao
  FROM responsavel r
  LEFT JOIN rmi      ON rmi.cpf = r.cpf_responsavel_final
  LEFT JOIN obitos o ON o.cpf  = r.cpf_responsavel_final
  WHERE r.cpf_responsavel_final IS NOT NULL
    AND o.cpf IS NULL
)

SELECT
  telefone,          -- 55DDD9xxxxxxxx
  cpf,               -- do responsável, 11 dígitos
  nome_responsavel,
  nome_aluno,
  id_aluno,          -- coluna auxiliar de rastreio
  denominacao
FROM dados
WHERE telefone IS NOT NULL
  -- AND nome_responsavel IS NOT NULL
  -- AND nome_aluno IS NOT NULL
  AND denominacao IS NOT NULL
  -- Já respondeu a pesquisa nos últimos 30 dias (por CPF ou telefone): não dispara de novo
  AND cpf NOT IN (SELECT cpf FROM respondeu WHERE cpf IS NOT NULL)
  AND telefone NOT IN (SELECT telefone FROM respondeu WHERE telefone IS NOT NULL)
  AND nome_aluno != "173.561.157-35"
-- Um disparo por responsável, mesmo com mais de um filho
QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf_aluno) = 1;