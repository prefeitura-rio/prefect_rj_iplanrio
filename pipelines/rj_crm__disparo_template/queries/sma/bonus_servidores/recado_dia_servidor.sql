WITH cpfs AS (
  SELECT DISTINCT lpad(cpf, 11, '0') as cpf
  FROM `rj-iplanrio.brutos_pessoa_vinculada.pessoa_vinculada`
  WHERE cpf IS NOT NULL
    AND status_vinculacao = 'ATIVO'   -- data uses ATIVO, not ATIVA
), -- total na base 126413, com telefones válidos 117551
telefones AS (
  SELECT pf.cpf, 'principal' AS origem_registro, 0 AS ordem, pf.telefone.principal AS t
  FROM `rj-rmi-dev.staging__rmi_dados_mestres.pessoa_fisica` pf
  WHERE pf.telefone.principal.valor IS NOT NULL
  AND pf.menor_idade IS FALSE
    AND pf.obito.indicador IS FALSE
    AND pf.telefone.principal.estrategia_envio != "NÃO ENVIAR"
  UNION ALL
  SELECT pf.cpf, 'alternativo', 1 + off, alt
  FROM `rj-rmi-dev.staging__rmi_dados_mestres.pessoa_fisica` pf,
    UNNEST(pf.telefone.alternativo) alt WITH OFFSET off
    where pf.menor_idade IS FALSE
    AND pf.obito.indicador IS FALSE
    AND alt.estrategia_envio != "NÃO ENVIAR"
),
celulares AS (
  SELECT
    cpf,
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(CONCAT(COALESCE(t.ddi, '55'), t.ddd, t.valor)) AS telefone,
    origem_registro,
    t.qualidade,
    t.indicador_optout,
    t.data_atualizacao,
    ordem
  FROM telefones
  WHERE t.tipo = 'CELULAR'
    and `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(CONCAT(COALESCE(t.ddi, '55'), t.ddd, t.valor))  is not null
),
final as (
  SELECT
    c.cpf,
    cel.telefone,
    cel.origem_registro,
    -- cel.qualidade,
    -- cel.indicador_optout,
    -- cel.data_atualizacao
  FROM cpfs c
  LEFT JOIN celulares cel USING (cpf)
  where cpf is not null and telefone is not null
  QUALIFY ROW_NUMBER() OVER (PARTITION BY c.cpf ORDER BY cel.ordem) = 1
  ORDER BY c.cpf, cel.ordem
)

select * from final