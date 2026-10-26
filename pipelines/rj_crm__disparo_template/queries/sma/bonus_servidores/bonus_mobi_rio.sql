-- recado_mobirio_pref_rio_bonus_v1

with dados as (
SELECT 
    INITCAP(
        IF(
            ARRAY_LENGTH(SPLIT(NOMECOMPLETOFUNC, ' ')) > 1,
            CONCAT(
                SPLIT(NOMECOMPLETOFUNC, ' ')[SAFE_OFFSET(0)], ' ',
                SPLIT(NOMECOMPLETOFUNC, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(NOMECOMPLETOFUNC, ' ')) - 1)]
            ),
            NOMECOMPLETOFUNC
        )
    ) AS nome_servidor
, LPAD(CAST(CPFNUMERO AS STRING), 11, '0') as cpf, 
    coalesce(
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(`TELEFONE`),
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(`TELEFONE_2`),
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(`Telefone_resgatado`),
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(`TELEFONE_GLOBUS`)
        ) as telefone FROM `rj-crm-registry-dev.teste.bonus_mobi_rio` 
)
select * from dados where cpf is not null and telefone is not null
union all
select 'Edy', '00000000353', '5511974243228'