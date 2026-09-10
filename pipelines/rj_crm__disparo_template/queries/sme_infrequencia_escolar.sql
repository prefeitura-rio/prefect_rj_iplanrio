with 
rmi as (
  select
    pf.cpf,
   `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
      CONCAT(
        IFNULL(pf.telefone.principal.ddi, '55'),
        IFNULL(pf.telefone.principal.ddd, '21'),
        pf.telefone.principal.valor
      )
    ) AS telefone_rmi
  from `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf
  WHERE 1=1
    -- filtros obrigatórios de disparo
    AND pf.menor_idade IS FALSE
    AND pf.obito.indicador IS FALSE
    AND pf.telefone.principal.qualidade = 'VALIDO'
    AND pf.telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR')
    AND COALESCE(pf.telefone.principal.indicador_optout, FALSE) = FALSE
    AND COALESCE(pf.telefone.principal.indicador_quarentena, FALSE) = FALSE),

dados as (
  SELECT
  b.MATRICULA AS matricula,
  `rj-crm-registry.udf.FORMAT_NAME`(b.NOME, TRUE) AS nome_aluno,
  `rj-crm-registry.udf.FORMAT_NAME`(b.NOME, TRUE) AS nome_aluno_2,
  coalesce(
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(a.contato_responsavel),
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(a.celular_1),
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(a.celular_2),
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(telefone_rmi)
    ) as telefone,
  a.CPF_Responsavel as SubscriberKey,
  `rj-crm-registry.udf.FORMAT_NAME`(a.Nome_responsavel, TRUE) as nome_responsavel,

  -- Celular da escola
  `rj-crm-registry.udf.FORMAT_PHONE_DISPLAY`(cast(c.telefone as string)) AS telefone_escola
FROM
  rj-sme.gestao_escolar.vw_bi_aluno a
LEFT JOIN rmi on rmi.cpf = a.CPF_Responsavel
LEFT JOIN
  rj-sme-dev.Inscricoes_site_26_creche.basecoc2infreq b
ON
  TRIM(CAST(b.MATRICULA AS STRING)) = TRIM(a.Matricula)
LEFT JOIN
  rj-sme.gestao_escolar.turma t 
ON
  a.tur_id = t.id_turma
LEFT JOIN
  rj-sme.educacao_basica.escola e 
ON
  t.id_escola = e.id_escola
LEFT JOIN
  rj-sme-dev.Inscricoes_site_26_creche.celulares_escolas c
ON
  CAST(e.id_designacao AS STRING) = LPAD(CAST(c.designacao AS STRING), 7, '0')  -- 7 dígitos com zero à esquerda
WHERE 1=1
  AND UPPER(TRIM(b.GRUPAMENTO)) IN (
    '6º ANO',
    '7º ANO',
    '8º ANO',
    '9º ANO',
    'CARIOCA I',
    'CARIOCA II'
  )
  AND a.situacao = 'Ativo'
  AND a.CPF_Responsavel IS NOT NULL
  AND a.CPF_Responsavel != '0'
  AND a.CPF_Responsavel != ''
  AND LENGTH(CAST(a.cpf AS STRING)) = 11
  AND c.telefone is not null
  ANd b.matricula is not null
  and b.NOME is not null
GROUP BY
  1, 2, 3, 4, 5, 6, 7
ORDER BY
  a.CPF_Responsavel)

select * from dados where telefone is not null