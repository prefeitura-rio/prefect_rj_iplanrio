WITH rmi AS (
  -- Busca telefone no RMI para quem passa no filtro de estratégia
  SELECT
    LPAD(CAST(pf.cpf AS STRING), 11, '0') AS cpf,
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(CONCAT(
      IFNULL(pf.telefone.principal.ddi, '55'),
      IFNULL(pf.telefone.principal.ddd, '21'),
      pf.telefone.principal.valor
    )) AS telefone_rmi
  FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf
  WHERE pf.menor_idade IS FALSE
    AND pf.telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR', 'EVITAR')
),

obitos AS (
  -- Base global de óbitos (independe de ter telefone no RMI ou não)
  SELECT DISTINCT LPAD(CAST(cpf AS STRING), 11, '0') AS cpf
  FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`
  WHERE obito.indicador IS TRUE
),

simulado AS (
  SELECT DISTINCT TRIM(CAST(CD_ALUNO_INSTITUCIONAL AS STRING)) AS matricula
  FROM `rj-sme-dev.alunos_alfabetizados_simulado_2026.alfabetizados_simulado2026`
  WHERE CD_ALUNO_INSTITUCIONAL IS NOT NULL
),

parentesco_cadunico AS (
  -- Recupera CPF do responsável via CadÚnico usando o CPF do aluno
  SELECT DISTINCT
    LPAD(CAST(cpf AS STRING), 11, '0') AS cpf_aluno,
    LPAD(CAST(cpf_responsavel AS STRING), 11, '0') AS cpf_responsavel,
    nome_responsavel
  FROM `rj-crm-registry.intermediario_rmi_parentesco_cadunico.dim_parentesco`
  WHERE cpf IS NOT NULL AND cpf_responsavel IS NOT NULL
),

alunos AS (
  SELECT
    a.Matricula AS matricula,
    -- Prioridade do CPF: 1º SME, 2º CadÚnico via CPF do Aluno
    COALESCE(
      NULLIF(LPAD(REGEXP_REPLACE(a.CPF_Responsavel, r'\D', ''), 11, '0'), '00000000000'),
      p.cpf_responsavel
    ) AS cpf,
    `rj-crm-registry.udf.FORMAT_NAME`(
      COALESCE(NULLIF(TRIM(a.NomeSocial), ''), a.Nome), TRUE
    ) AS nome_aluno,
    `rj-crm-registry.udf.FORMAT_NAME`(
      COALESCE(a.Nome_Responsavel, p.nome_responsavel)
      , TRUE) AS nome_responsavel,
    a.Contato_Responsavel,
    a.celular_1,
    a.celular_2
  FROM `rj-sme.gestao_escolar.vw_bi_aluno` a
  INNER JOIN simulado s
    ON TRIM(CAST(a.Matricula AS STRING)) = s.matricula
  LEFT JOIN parentesco_cadunico p
    ON LPAD(REGEXP_REPLACE(a.CPF, r'\D', ''), 11, '0') = p.cpf_aluno
  WHERE a.Situacao = 'Ativo'
    AND a.grupamento = '2º ano'
    AND a.CPF IS NOT NULL AND TRIM(a.CPF) != ''
),

dados AS (
  SELECT
    al.cpf,
    -- Prioridade de Telefone: SME -> RMI
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
  LEFT JOIN rmi      ON rmi.cpf = al.cpf
  LEFT JOIN obitos o ON o.cpf  = al.cpf
  WHERE o.cpf IS NULL             -- Exclui qualquer CPF confirmado falecido
    AND al.cpf IS NOT NULL
    AND al.cpf != '00000000000'
)

SELECT
  telefone,
  cpf,
  nome_responsavel,
  nome_aluno,
  matricula,
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
QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY matricula) = 1;