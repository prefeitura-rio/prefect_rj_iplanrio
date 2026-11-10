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
    AND pf.telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR', 'EVITAR')
),

parentesco_cadunico AS (
  -- Busca CPF e Nome do responsável no CadÚnico via CPF do aluno
  -- QUALIFY garante exatamente 1 responsável por aluno para evitar duplicação no JOIN
  SELECT
    LPAD(CAST(cpf AS STRING), 11, '0') AS cpf_aluno,
    LPAD(CAST(cpf_responsavel AS STRING), 11, '0') AS cpf_responsavel,
    `rj-crm-registry.udf.FORMAT_NAME`(nome_responsavel, TRUE) AS nome_responsavel
  FROM `rj-crm-registry.intermediario_rmi_parentesco_cadunico.dim_parentesco`
  WHERE cpf IS NOT NULL 
    AND cpf_responsavel IS NOT NULL
    AND cpf != cpf_responsavel
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LPAD(CAST(cpf AS STRING), 11, '0') 
    ORDER BY cpf_responsavel
  ) = 1
),

alunos AS (
  SELECT
    a.Matricula AS matricula,
    -- Prioridade do CPF: 1º SME, 2º CadÚnico
    COALESCE(
      NULLIF(LPAD(REGEXP_REPLACE(a.CPF_Responsavel, r'\D', ''), 11, '0'), '00000000000'),
      p.cpf_responsavel
    ) AS cpf,
    `rj-crm-registry.udf.FORMAT_NAME`(
      COALESCE(NULLIF(TRIM(a.NomeSocial), ''), a.Nome), TRUE
    ) AS nome_aluno,
    -- Prioridade do Nome do Responsável: 1º SME, 2º CadÚnico
    COALESCE(
      `rj-crm-registry.udf.FORMAT_NAME`(NULLIF(TRIM(a.Nome_Responsavel), ''), TRUE),
      p.nome_responsavel
    ) AS nome_responsavel,
    a.Contato_Responsavel,
    a.celular_1,
    a.celular_2
  FROM `rj-sme.gestao_escolar.vw_bi_aluno` a
  LEFT JOIN parentesco_cadunico p
    ON LPAD(REGEXP_REPLACE(a.CPF, r'\D', ''), 11, '0') = p.cpf_aluno
  WHERE a.Situacao = 'Ativo'
    AND a.grupamento = '2º ano'
    AND a.CPF IS NOT NULL AND TRIM(a.CPF) != ''
),

dados AS (
  SELECT
    al.cpf,
    -- Prioridade definida na doc para a educação: contato_responsavel, celular_1, celular_2, RMI
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(al.Contato_Responsavel) AS tel_contato,
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(al.celular_1)            AS tel_celular_1,
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(al.celular_2)            AS tel_celular_2,
    rmi.telefone_rmi                                                           AS tel_rmi,
    COALESCE(
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(al.Contato_Responsavel),
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(al.celular_1),
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(al.celular_2),
      rmi.telefone_rmi
    ) AS telefone,
    al.nome_responsavel,
    al.nome_aluno,
    al.matricula
  FROM alunos al
  LEFT JOIN rmi ON rmi.cpf = al.cpf
  WHERE al.cpf IS NOT NULL
    AND al.cpf != '00000000000'   -- CPFs "0" da SME
)

SELECT
  telefone,          -- 55DDD9xxxxxxxx
  cpf,               -- do responsável, 11 dígitos
  nome_responsavel,
  nome_aluno,
  matricula AS id_matricula,   -- coluna auxiliar de rastreio (seção 4.4)
  ARRAY(
    SELECT x
    FROM UNNEST([tel_contato, tel_celular_1, tel_celular_2, tel_rmi]) AS x WITH OFFSET pos
    WHERE x IS NOT NULL AND x != telefone
    QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY pos) = 1
    ORDER BY pos
  ) AS others
FROM dados
WHERE telefone IS NOT NULL
  AND nome_responsavel IS NOT NULL
  AND nome_aluno IS NOT NULL
-- Um disparo por responsável, mesmo com mais de um filho no 2º ano
QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY matricula) = 1;


-- WITH rmi AS (
--   -- Telefone do RMI, só para quem passa nos filtros obrigatórios (seção 4.5)
--   SELECT
--     LPAD(CAST(pf.cpf AS STRING), 11, '0') AS cpf,
--     `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(CONCAT(
--       IFNULL(pf.telefone.principal.ddi, '55'),
--       IFNULL(pf.telefone.principal.ddd, '21'),
--       pf.telefone.principal.valor
--     )) AS telefone_rmi
--   FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf
--   WHERE pf.menor_idade IS FALSE
--     AND pf.obito.indicador IS FALSE
--     AND pf.telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR', 'EVITAR')
-- ),

-- alunos AS (
--   SELECT
--     a.Matricula AS matricula,
--     LPAD(REGEXP_REPLACE(a.CPF_Responsavel, r'\D', ''), 11, '0') AS cpf,
--     `rj-crm-registry.udf.FORMAT_NAME`(
--       COALESCE(NULLIF(TRIM(a.NomeSocial), ''), a.Nome), TRUE
--     ) AS nome_aluno,
--     `rj-crm-registry.udf.FORMAT_NAME`(a.Nome_Responsavel, TRUE) AS nome_responsavel,
--     a.Contato_Responsavel,
--     a.celular_1,
--     a.celular_2
--   FROM `rj-sme.gestao_escolar.vw_bi_aluno` a
--   WHERE a.Situacao = 'Ativo'
--     AND a.grupamento = '2º ano'
--     AND a.CPF IS NOT NULL AND TRIM(a.CPF) != ''
--     AND a.CPF_Responsavel IS NOT NULL AND TRIM(a.CPF_Responsavel) != ''
-- ),

-- dados AS (
--   SELECT
--     al.cpf,
--     -- Prioridade definida na doc para a educação: contato_responsavel, celular_1, celular_2, RMI
--     COALESCE(
--       `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(al.Contato_Responsavel),
--       `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(al.celular_1),
--       `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(al.celular_2),
--       rmi.telefone_rmi
--     ) AS telefone,
--     al.nome_responsavel,
--     al.nome_aluno,
--     al.matricula
--   FROM alunos al
--   LEFT JOIN rmi      ON rmi.cpf = al.cpf
--   WHERE al.cpf != '00000000000'   -- CPFs "0" da SME
-- )

-- SELECT
--   telefone,          -- 55DDD9xxxxxxxx
--   cpf,               -- do responsável, 11 dígitos
--   nome_responsavel,
--   nome_aluno,
--   matricula as id_matricula          -- coluna auxiliar de rastreio (seção 4.4)
-- FROM dados
-- WHERE telefone IS NOT NULL
--   AND nome_responsavel IS NOT NULL
--   AND nome_aluno IS NOT NULL
-- -- Um disparo por responsável, mesmo com mais de um filho no 2º ano
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY matricula) = 1