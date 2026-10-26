-- recado_servidores_pref_rio_14

with dados as (
  SELECT 
    INITCAP(
        IF(
            ARRAY_LENGTH(SPLIT(NOME, ' ')) > 1,
            CONCAT(
                SPLIT(NOME, ' ')[SAFE_OFFSET(0)], ' ',
                SPLIT(NOME, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(NOME, ' ')) - 1)]
            ),
            NOME
        )
    ) AS nome_servidor,
  LPAD(CAST(CPF AS STRING), 11, '0') as cpf, 
  coalesce(
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(split(celular,'/')[safe_offset(0)]),
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(split(celular,'/')[safe_offset(1)]),
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(cast(celular_dois as string)),  
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(split(TELEFONE,'/')[safe_offset(0)]),
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(split(TELEFONE,'/')[safe_offset(1)])
    ) as telefone
    FROM `rj-crm-registry-dev.teste.bonus_servidor_primeiro_envio`

)
select * from dados where cpf is not null and telefone is not null