-- Query 4

WITH
  cadastro_inscritos AS (
    SELECT
      *
    FROM `rj-smtr-sandbox.projeto_disparo_enem_2026.cadastro_incritos`
  ),
  rmi AS (
    SELECT
      INITCAP(
        IF(
            ARRAY_LENGTH(SPLIT(COALESCE(rmi.nome_social, rmi.nome), ' ')) > 1,
            CONCAT(
                SPLIT(COALESCE(rmi.nome_social, rmi.nome), ' ')[SAFE_OFFSET(0)], ' ',
                SPLIT(COALESCE(rmi.nome_social, rmi.nome), ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(COALESCE(rmi.nome_social, rmi.nome), ' ')) - 1)]
            ),
            COALESCE(rmi.nome_social, rmi.nome)
        )
    ) AS nome_sobrenome,
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
        CONCAT(
          IFNULL(rmi.telefone.principal.ddi, '55'),
          IFNULL(rmi.telefone.principal.ddd, '21'),
          rmi.telefone.principal.valor)) AS telefone,
      LPAD(CAST(rmi.cpf AS STRING), 11, '0') AS cpf
    FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` rmi
    JOIN cadastro_inscritos USING(cpf_particao)
    WHERE
      rmi.obito.indicador = FALSE
      -- AND rmi.menor_idade = FALSE
      AND (rmi.nome IS NOT NULL OR rmi.nome_social IS NOT NULL)
      AND rmi.telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR', 'EVITAR')
  )
SELECT
  cpf,
  pf.nome_sobrenome,
  pf.telefone
FROM cadastro_inscritos c
JOIN rmi pf
  USING (cpf)
  where telefone is not null

