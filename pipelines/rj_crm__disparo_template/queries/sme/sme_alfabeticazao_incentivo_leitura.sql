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

alunos AS (
  SELECT
    a.Matricula AS matricula,
    LPAD(REGEXP_REPLACE(a.CPF_Responsavel, r'\D', ''), 11, '0') AS cpf,
    `rj-crm-registry.udf.FORMAT_NAME`(
      COALESCE(NULLIF(TRIM(a.NomeSocial), ''), a.Nome), TRUE
    ) AS nome_aluno,
    `rj-crm-registry.udf.FORMAT_NAME`(a.Nome_Responsavel, TRUE) AS nome_responsavel,
    a.Contato_Responsavel,
    a.celular_1,
    a.celular_2
  FROM `rj-sme.gestao_escolar.vw_bi_aluno` a
  WHERE a.Situacao = 'Ativo'
    AND a.grupamento = '2º ano'
    AND a.CPF IS NOT NULL AND TRIM(a.CPF) != ''
    AND a.CPF_Responsavel IS NOT NULL AND TRIM(a.CPF_Responsavel) != ''
),

dados AS (
  SELECT
    al.cpf,
    -- Prioridade definida na doc para a educação: contato_responsavel, celular_1, celular_2, RMI
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
  WHERE al.cpf != '00000000000'   -- CPFs "0" da SME
)

SELECT
  telefone,          -- 55DDD9xxxxxxxx
  cpf,               -- do responsável, 11 dígitos
  nome_responsavel,
  nome_aluno,
  matricula as id_matricula          -- coluna auxiliar de rastreio (seção 4.4)
FROM dados
WHERE telefone IS NOT NULL
  AND nome_responsavel IS NOT NULL
  AND nome_aluno IS NOT NULL
-- Um disparo por responsável, mesmo com mais de um filho no 2º ano
QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY matricula) = 1